# calculate device wearing for specific participant.
import marimo as mo
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import requests
import os
from datetime import datetime, timedelta
from sensorfabric.mdh import MDH
from sensorfabric.needle import Needle
from urllib.parse import urlencode
import hashlib
import json
from pathlib import Path
from sensorfabric.athena import athena


mdh = MDH()

mdh_athena = Needle(method="mdh")

aws_athena = aws = athena(
    profile_name=os.getenv("AWS_PROFILE_NAME"),
    database=os.getenv('AWS_BIOBAYB_DB_NAME'),
    s3_location=os.getenv('AWS_BIOBAYB_S3_LOCATION'),
    workgroup=os.getenv('AWS_BIOBAYB_WORKGROUP'),
    offlineCache=False,
)


def calculate_daily_wear_from_oura(participantidentifier, first_week, last_week):
    query = f"""
    WITH edd AS (
    SELECT
        participantidentifier,
        DATE_PARSE(JSON_EXTRACT_SCALAR(CAST(customfields AS JSON), '$.edd_final'), '%Y-%m-%d') AS edd_final
    FROM allparticipants
    WHERE participantidentifier = '{participantidentifier}'
    ),
    w1 AS (
    SELECT
        participantidentifier,
        CAST(edd_final AS date) - INTERVAL '280' DAY AS w1_date
    FROM edd
    WHERE edd_final IS NOT NULL
    ),
    oura_days AS (
    SELECT
        participantidentifier,
        CAST("timestamp" AS date) AS day_date,
        -- Clamp to [0,1] and ensure floating math
        GREATEST(0.0, LEAST(1.0, 1.0 - CAST(COALESCE(nonweartime, 0) AS DOUBLE) / 86400.0)) AS wear_fraction
    FROM ouradailyactivity
    WHERE participantidentifier = '{participantidentifier}'
    ),
    wear_flags AS (
    SELECT
        participantidentifier,
        day_date,
        CASE WHEN wear_fraction >= 0.75 THEN 1 ELSE 0 END AS wear_day_flag
    FROM oura_days
    ),
    days_with_week AS (
    SELECT
        wf.participantidentifier,
        wf.day_date,
        1 + CAST(date_diff('day', w.w1_date, wf.day_date) / 7 AS integer) AS ga_week,
        wf.wear_day_flag
    FROM wear_flags wf
    JOIN w1 w
        ON w.participantidentifier = wf.participantidentifier
    ),
    weekly_sums AS (
    SELECT
        participantidentifier,
        ga_week AS week,
        SUM(wear_day_flag) AS wear_days_ge_75
    FROM days_with_week
    WHERE ga_week BETWEEN {first_week} AND {last_week}
    GROUP BY 1, 2
    ),
    weeks AS (
    SELECT
        w1.participantidentifier,
        CAST(week AS integer) AS week
    FROM w1
    CROSS JOIN UNNEST(sequence({first_week}, {last_week})) AS t(week)
    )
    SELECT
    w.participantidentifier,
    w.week,
    COALESCE(ws.wear_days_ge_75, 0) AS wear_days_ge_75
    FROM weeks w
    JOIN w1
    ON w1.participantidentifier = w.participantidentifier
    LEFT JOIN weekly_sums ws
    ON ws.participantidentifier = w.participantidentifier
    AND ws.week = w.week
    ORDER BY w.week
    """
    result = mdh_athena.execQuery(query)
    return [int(i) for i in result['wear_days_ge_75'].tolist()]

def calculate_daily_wear_from_uh(participantidentifier, first_w1_day, first_week, last_week):
    query = f"""
    WITH w1 AS (
    SELECT DATE '{first_w1_day.date()}' AS w1_date
    ),
    src AS (
    SELECT
        pid,
        CAST(from_iso8601_timestamp(object_day_start_timestamp_iso8601_tz) AS date) AS day_date,
        object_values_timestamp
    FROM temp
    WHERE pid = '{participantidentifier}'
        AND object_day_start_timestamp_iso8601_tz IS NOT NULL
    ),
    day_counts AS (
    SELECT
        pid,
        day_date,
        COUNT(DISTINCT object_values_timestamp) AS samples_in_day
    FROM src
    GROUP BY 1, 2
    ),
    day_flags AS (
    SELECT
        d.pid,
        d.day_date,
        CASE WHEN d.samples_in_day >= 0.75 * 288 THEN 1 ELSE 0 END AS wear_day_flag
    FROM day_counts d
    ),
    days_with_week AS (
    SELECT
        '{participantidentifier}' AS pid,
        df.day_date,
        1 + CAST(date_diff('day', w.w1_date, df.day_date) / 7 AS integer) AS ga_week,
        df.wear_day_flag
    FROM day_flags df
    CROSS JOIN w1 w
    ),
    weekly_sums AS (
    SELECT
        pid,
        ga_week AS week,
        SUM(wear_day_flag) AS wear_days_ge_75
    FROM days_with_week
    WHERE ga_week BETWEEN {first_week} AND {last_week}
    GROUP BY 1, 2
    ),
    weeks AS (
    SELECT
        '{participantidentifier}' AS pid,
        CAST(week AS integer) AS week
    FROM UNNEST(sequence({first_week}, {last_week})) AS t(week)
    )
    SELECT
    w.pid,
    w.week,
    COALESCE(ws.wear_days_ge_75, 0) AS wear_days_ge_75
    FROM weeks w
    CROSS JOIN w1
    LEFT JOIN weekly_sums ws
    ON ws.pid  = w.pid
    AND ws.week = w.week
    ORDER BY w.week
    """
    result = aws_athena.execQuery(query)
    return [int(i) for i in result['wear_days_ge_75'].tolist()]

# Device wear percentage detection
def calculate_daily_wear(email, date):
    if date > datetime.today():
        return 0.0 # skip requests for future data.
    if 'UHKEY' not in os.environ:
        print('Could not find UH authorization key.')
        return None
    auth_key = os.environ['UHKEY']
    endpoint = 'https://partner.ultrahuman.com/api/v1/metrics'
    headers = {'Authorization': auth_key}
    params = {
        'email': email,
        'date': date.strftime('%Y-%m-%d'),
    }
    try:
        data = None
        cached_response_file = f".cache/{get_hash_of_params(params, endpoint)}"
        if os.path.exists(cached_response_file):
            with open(cached_response_file, 'r', encoding="utf-8") as file:
                data = json.load(file)
        else:
            response = requests.get(endpoint, params=params, headers=headers)
            if response.status_code == 200:
                data = response.json()
                Path('.cache').mkdir(parents=True, exist_ok=True)
                if date < datetime.today(): # cache only past data.
                    with open(cached_response_file, "w", encoding="utf-8") as file:
                        json.dump(data, file, ensure_ascii=False)
            else:
                print(f"API error: {response.status_code} for {email} on {date}")
                return 0.0
        map = {d['type']: d for d in data['data']['metric_data']}
        if 'temp' not in map:
            return 0.0
        subset = map['temp']['object']
        values = [v['value'] for v in subset['values']]
        expected_values_length = 288  # 100%
        return (len(values) / expected_values_length) * 100
    except Exception as e:
        print(f"Error fetching wear data for {email} on {date}: {e}")
        return 0.0

def get_hash_of_params(params, endpoint):
    q = urlencode(sorted(params.items()))
    raw = f"{endpoint}?{q}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def get_weekly_wear_count(email, week_start, week_end):
    count = 0
    current_date = week_start
    while current_date <= week_end:
        percentage = calculate_daily_wear(email, current_date)
        if percentage is not None and percentage >= 75:
            count += 1
        current_date += timedelta(days=1)
    return count

def calculate_daily_symptoms(participantidentifier, first_week, last_week):
    query = f"""
    WITH edd AS (
    SELECT
        participantidentifier,
        date_parse(json_extract_scalar(cast(customfields AS JSON), '$.edd_final'), '%Y-%m-%d') AS edd_final
    FROM allparticipants
    WHERE participantidentifier = '{participantidentifier}'
    ),
    w1 AS (
    SELECT
        participantidentifier,
        CAST(edd_final AS date) - INTERVAL '280' DAY AS w1_date
    FROM edd
    WHERE edd_final IS NOT NULL
    ),
    calendar_days AS (
    SELECT
        w.participantidentifier,
        d AS day_date,
        1 + CAST(date_diff('day', w.w1_date, d) / 7 AS integer) AS gest_week
    FROM w1 w
    CROSS JOIN UNNEST(
        SEQUENCE(
        date_add('day', 7 * ({first_week} - 1), w.w1_date),   -- start of first_week
        date_add('day', 7 *  {last_week} - 1, w.w1_date),     -- end of last_week
        INTERVAL '1' DAY
        )
    ) AS t(d)
    ),
    pdd_days AS (
    SELECT
        participantidentifier,
        CAST(inserteddate AS date) AS day_date
    FROM projectdevicedata
    WHERE participantidentifier = '{participantidentifier}'
        AND CAST(inserteddate AS date) BETWEEN
            (SELECT MIN(day_date) FROM calendar_days)
            AND (SELECT MAX(day_date) FROM calendar_days)
    GROUP BY 1, 2
    )
    SELECT
    c.participantidentifier,
    c.gest_week AS week,
    SUM(CASE WHEN p.day_date IS NOT NULL THEN 1 ELSE 0 END) AS days_with_checkin
    FROM calendar_days c
    LEFT JOIN pdd_days p
    ON p.participantidentifier = c.participantidentifier
    AND p.day_date = c.day_date
    WHERE c.gest_week BETWEEN {first_week} AND {last_week}
    GROUP BY 1, 2
    ORDER BY week;
    """
    result = mdh_athena.execQuery(query)
    return [int(i) for i in result['days_with_checkin'].tolist()]

def calculate_daily_questions(participantidentifier, first_week, last_week):
    query = f"""    
    WITH ema_results AS (
        SELECT surveyresultkey, surveyname
        FROM surveyresults
        WHERE surveyname IN ('EMA PM', 'EMA AM')
    ),
    answers AS (
    SELECT
        sqr.participantidentifier,
        er.surveyname,
        CAST(sqr.startdate - INTERVAL '7' HOUR AS date) AS day_date,
        sqr.resultidentifier,
        sqr.surveyresultkey
    FROM surveyquestionresults sqr
    JOIN ema_results er
        ON er.surveyresultkey = sqr.surveyresultkey
    WHERE sqr.participantidentifier = '{participantidentifier}'
    ),
    edd AS (
    SELECT
        participantidentifier,
        DATE_PARSE(JSON_EXTRACT_SCALAR(CAST(customfields AS JSON), '$.edd_final'), '%Y-%m-%d') AS edd_final
    FROM allparticipants
    WHERE participantidentifier = '{participantidentifier}'
    ),
    w1 AS (
    SELECT
        participantidentifier,
        CAST(edd_final AS date) - INTERVAL '280' DAY AS w1_date
    FROM edd
    WHERE edd_final IS NOT NULL
    ),
    day_counts AS (
    SELECT
        a.participantidentifier,
        a.day_date,
        COUNT(DISTINCT a.resultidentifier) AS questions_answered
    FROM answers a
    GROUP BY 1, 2
    ),
    days_with_weeks AS (
    SELECT
        d.participantidentifier,
        d.day_date,
        1 + CAST(date_diff('day', w.w1_date, d.day_date) / 7 AS integer) AS ga_week,
        d.questions_answered
    FROM day_counts d
    JOIN w1 w
        ON d.participantidentifier = w.participantidentifier
    ),
    weekly_counts AS (
    SELECT
        participantidentifier,
        ga_week AS week,
        SUM(CASE WHEN questions_answered >= 6 THEN 1 ELSE 0 END) AS days_with_5q
    FROM days_with_weeks
    WHERE ga_week BETWEEN {first_week} AND {last_week}
    GROUP BY 1, 2
    ),
    weeks AS (
    SELECT
        w1.participantidentifier,
        CAST(week AS integer) AS week
    FROM w1
    CROSS JOIN UNNEST(sequence({first_week}, {last_week})) AS t(week)
    )
    SELECT
    w.participantidentifier,
    w.week,
    COALESCE(wc.days_with_5q, 0) AS days_with_5q
    FROM weeks w
    LEFT JOIN weekly_counts wc
    ON wc.participantidentifier = w.participantidentifier
    AND wc.week = w.week
    ORDER BY w.week;
    """
    result = mdh_athena.execQuery(query)
    return [int(i) for i in result['days_with_5q'].tolist()]


def calculate_weekly_bimontly_surveys(participantidentifier, first_week, last_week):
    query = f"""
    WITH sr AS (
    SELECT surveyresultkey, surveyname
    FROM surveyresults
    WHERE surveyname IN (
        -- weekly surveys
        'mMOS (Weekly)',
        'PROMIS Sleep (Weekly)',
        'BRCS (Weekly)',
        'Pregnancy Experience Scale',
        -- exception (biomonthly) surveys
        'Maternal Antenatal Attachment Scale',
        'Edinburgh Postnatal Depression Scale (EPDS)',
        'Perinatal Anxiety Screening Scale (PASS)'
    )
    ),
    submissions AS (
    SELECT
        sqr.participantidentifier,
        sr.surveyname,
        CAST(MIN(sqr.startdate - INTERVAL '7' HOUR) AS date) AS day_date,
        sqr.surveyresultkey
    FROM surveyquestionresults sqr
    JOIN sr
        ON sr.surveyresultkey = sqr.surveyresultkey
    WHERE sqr.participantidentifier = '{participantidentifier}'
    GROUP BY sqr.participantidentifier, sr.surveyname, sqr.surveyresultkey
    ),
    edd AS (
    SELECT
        participantidentifier,
        DATE_PARSE(JSON_EXTRACT_SCALAR(CAST(customfields AS JSON), '$.edd_final'), '%Y-%m-%d') AS edd_final
    FROM allparticipants
    WHERE participantidentifier = '{participantidentifier}'
    ),
    w1 AS (
    SELECT
        participantidentifier,
        CAST(edd_final AS date) - INTERVAL '280' DAY AS w1_date
    FROM edd
    WHERE edd_final IS NOT NULL
    ),
    -- Map each submission to gestational week
    submissions_with_weeks AS (
    SELECT
        s.participantidentifier,
        s.surveyname,
        1 + CAST(date_diff('day', w.w1_date, s.day_date) / 7 AS integer) AS ga_week
    FROM submissions s
    JOIN w1 w
        ON w.participantidentifier = s.participantidentifier
    ),
    weekly_flags AS (
    SELECT
        participantidentifier,
        ga_week AS week,
        -- weekly surveys: 0/1 if done at least once in that week
        MAX(CASE WHEN surveyname = 'mMOS (Weekly)'              THEN 1 ELSE 0 END) AS mmos_done,
        MAX(CASE WHEN surveyname = 'PROMIS Sleep (Weekly)'      THEN 1 ELSE 0 END) AS promis_sleep_done,
        MAX(CASE WHEN surveyname = 'BRCS (Weekly)'              THEN 1 ELSE 0 END) AS brcs_done,
        MAX(CASE WHEN surveyname = 'Pregnancy Experience Scale' THEN 1 ELSE 0 END) AS pes_done,
        -- exception surveys: only count when done in the week 20, 28, 32
        MAX(CASE WHEN surveyname = 'Maternal Antenatal Attachment Scale'        AND ga_week = 20 THEN 1 ELSE 0 END) AS maas_w20_done,
        MAX(CASE WHEN surveyname = 'Edinburgh Postnatal Depression Scale (EPDS)' AND ga_week = 28 THEN 1 ELSE 0 END) AS epds_w28_done,
        MAX(CASE WHEN surveyname = 'Perinatal Anxiety Screening Scale (PASS)'    AND ga_week = 32 THEN 1 ELSE 0 END) AS pass_w32_done
    FROM submissions_with_weeks
    WHERE ga_week BETWEEN {first_week} AND {last_week}
    GROUP BY 1, 2
    ),
    -- Generate a full list of weeks so missing ones show 0s
    weeks AS (
    SELECT
        w1.participantidentifier,
        CAST(week AS integer) AS week
    FROM w1
    CROSS JOIN UNNEST(sequence({first_week}, {last_week})) AS t(week)
    )
    SELECT
    w.participantidentifier,
    w.week,
    -- weekly-only count (0..7)
    (COALESCE(wf.mmos_done, 0)
    + COALESCE(wf.promis_sleep_done, 0)
    + COALESCE(wf.brcs_done, 0)
    + COALESCE(wf.pes_done, 0)
    + COALESCE(wf.maas_w20_done, 0)
    + COALESCE(wf.epds_w28_done, 0)
    + COALESCE(wf.pass_w32_done, 0)) AS weekly_completed_count
    FROM weeks w
    LEFT JOIN weekly_flags wf
    ON wf.participantidentifier = w.participantidentifier
    AND wf.week = w.week
    ORDER BY w.week;
    """
    result = mdh_athena.execQuery(query)
    return [int(i) for i in result['weekly_completed_count'].tolist()]


def calculate_weight_measurements(participantidentifier, first_week, last_week):
    query = f"""
    WITH bp_src AS (
    SELECT
        participantidentifier,
        CAST(COALESCE(datetimelocal, datetime, inserteddate) AS date) AS day_date
    FROM omronbloodpressure
    WHERE participantidentifier = '{participantidentifier}'
    ),
    bp_days AS (
    SELECT participantidentifier, day_date
    FROM bp_src
    GROUP BY 1, 2
    ),

    googlefit_wt_src AS (
        SELECT
            participantidentifier,
            CAST(COALESCE(windowstart - INTERVAL '7' HOUR) AS date) AS day_date
        FROM googlefitsamples
        WHERE participantidentifier = '{participantidentifier}'
        AND type = 'Weight'
    ),
    googlefit_wt_days AS (
        SELECT participantidentifier, day_date
        FROM googlefit_wt_src
        GROUP BY 1, 2
    ),

    wt_src AS (
    SELECT
        participantidentifier,
        CAST(COALESCE(startdate - INTERVAL '7' HOUR) AS date) AS day_date
    FROM healthkitv2samples
    WHERE participantidentifier = '{participantidentifier}'
        AND type = 'Weight'
    ),
    wt_days AS (
    SELECT participantidentifier, day_date
    FROM wt_src
    GROUP BY 1, 2
    ),

    edd AS (
    SELECT
        participantidentifier,
        DATE_PARSE(JSON_EXTRACT_SCALAR(CAST(customfields AS JSON), '$.edd_final'), '%Y-%m-%d') AS edd_final
    FROM allparticipants
    WHERE participantidentifier = '{participantidentifier}'
    ),
    w1 AS (
    SELECT
        participantidentifier,
        CAST(edd_final AS date) - INTERVAL '280' DAY AS w1_date
    FROM edd
    WHERE edd_final IS NOT NULL
    ),

    bp_with_weeks AS (
    SELECT
        b.participantidentifier,
        1 + CAST(date_diff('day', w.w1_date, b.day_date) / 7 AS integer) AS ga_week
    FROM bp_days b
    JOIN w1 w
        ON w.participantidentifier = b.participantidentifier
    ),
    wt_with_weeks AS (
    SELECT
        wtd.participantidentifier,
        1 + CAST(date_diff('day', w.w1_date, wtd.day_date) / 7 AS integer) AS ga_week
    FROM wt_days wtd
    JOIN w1 w
        ON w.participantidentifier = wtd.participantidentifier
    ),
    googlefit_wt_with_weeks AS (
    SELECT
        wtd.participantidentifier,
        1 + CAST(date_diff('day', w.w1_date, wtd.day_date) / 7 AS integer) AS ga_week
    FROM googlefit_wt_days wtd
    JOIN w1 w
        ON w.participantidentifier = wtd.participantidentifier
    ),

    bp_weekly AS (
    SELECT participantidentifier, ga_week AS week, COUNT(*) AS bp_days_in_week
    FROM bp_with_weeks
    WHERE ga_week BETWEEN {first_week} AND {last_week}
    GROUP BY 1, 2
    ),
    wt_weekly AS (
    SELECT participantidentifier, ga_week AS week, COUNT(*) AS weight_days_in_week
    FROM wt_with_weeks
    WHERE ga_week BETWEEN {first_week} AND {last_week}
    GROUP BY 1, 2
    ),
    googlefit_wt_weekly AS (
    SELECT participantidentifier, ga_week AS week, COUNT(*) AS weight_days_in_week
    FROM googlefit_wt_with_weeks
    WHERE ga_week BETWEEN {first_week} AND {last_week}
    GROUP BY 1, 2
    ),

    weeks AS (
    SELECT
        w1.participantidentifier,
        CAST(week AS integer) AS week
    FROM w1
    CROSS JOIN UNNEST(sequence({first_week}, {last_week})) AS t(week)
    )

    SELECT
    w.participantidentifier,
    w.week,
    COALESCE(ww.weight_days_in_week, gw.weight_days_in_week, 0) AS meets_2x
    FROM weeks w
    LEFT JOIN bp_weekly bw
    ON bw.participantidentifier = w.participantidentifier
    AND bw.week = w.week
    LEFT JOIN wt_weekly ww
    ON ww.participantidentifier = w.participantidentifier
    AND ww.week = w.week
    LEFT JOIN googlefit_wt_weekly gw
    ON gw.participantidentifier = w.participantidentifier
    AND gw.week = w.week
    ORDER BY w.week;
    """
    result = mdh_athena.execQuery(query)
    return [int(i) for i in result['meets_2x'].tolist()]

def calculate_bp_measurements(participantidentifier, first_week, last_week):
    query = f"""
    WITH bp_src AS (
    SELECT
        participantidentifier,
        CAST(COALESCE(datetimelocal, datetime, inserteddate) AS date) AS day_date
    FROM omronbloodpressure
    WHERE participantidentifier = '{participantidentifier}'
    ),
    bp_days AS (
    SELECT participantidentifier, day_date
    FROM bp_src
    GROUP BY 1, 2
    ),

    googlefit_bp_src AS (
        SELECT
            participantidentifier,
            CAST(COALESCE(windowstart - INTERVAL '7' HOUR) AS date) AS day_date
        FROM googlefitsamples
        WHERE participantidentifier = '{participantidentifier}'
        AND (type = 'blood_pressure_diastolic' OR type = 'blood_pressure_systolic')
    ),
    googlefit_bp_days AS (
    SELECT participantidentifier, day_date
    FROM googlefit_bp_src
    GROUP BY 1, 2
    ),

    wt_src AS (
    SELECT
        participantidentifier,
        CAST(COALESCE(startdate - INTERVAL '7' HOUR) AS date) AS day_date
    FROM healthkitv2samples
    WHERE participantidentifier = '{participantidentifier}'
        AND (type = 'BloodPressureSystolic' OR type = 'BloodPressureDiastolic')
    ),
    wt_days AS (
    SELECT participantidentifier, day_date
    FROM wt_src
    GROUP BY 1, 2
    ),

    edd AS (
    SELECT
        participantidentifier,
        DATE_PARSE(JSON_EXTRACT_SCALAR(CAST(customfields AS JSON), '$.edd_final'), '%Y-%m-%d') AS edd_final
    FROM allparticipants
    WHERE participantidentifier = '{participantidentifier}'
    ),
    w1 AS (
    SELECT
        participantidentifier,
        CAST(edd_final AS date) - INTERVAL '280' DAY AS w1_date
    FROM edd
    WHERE edd_final IS NOT NULL
    ),

    bp_with_weeks AS (
    SELECT
        b.participantidentifier,
        1 + CAST(date_diff('day', w.w1_date, b.day_date) / 7 AS integer) AS ga_week
    FROM bp_days b
    JOIN w1 w
        ON w.participantidentifier = b.participantidentifier
    ),
    wt_with_weeks AS (
    SELECT
        wtd.participantidentifier,
        1 + CAST(date_diff('day', w.w1_date, wtd.day_date) / 7 AS integer) AS ga_week
    FROM wt_days wtd
    JOIN w1 w
        ON w.participantidentifier = wtd.participantidentifier
    ),
    googlefit_bp_with_weeks AS (
    SELECT
        gbp.participantidentifier,
        1 + CAST(date_diff('day', w.w1_date, gbp.day_date) / 7 AS integer) AS ga_week
    FROM googlefit_bp_days gbp
    JOIN w1 w
        ON w.participantidentifier = gbp.participantidentifier
    ),

    bp_weekly AS (
    SELECT participantidentifier, ga_week AS week, COUNT(*) AS bp_days_in_week
    FROM bp_with_weeks
    WHERE ga_week BETWEEN {first_week} AND {last_week}
    GROUP BY 1, 2
    ),
    wt_weekly AS (
    SELECT participantidentifier, ga_week AS week, COUNT(*) AS weight_days_in_week
    FROM wt_with_weeks
    WHERE ga_week BETWEEN {first_week} AND {last_week}
    GROUP BY 1, 2
    ),
    googlefit_bp_weekly AS (
    SELECT participantidentifier, ga_week AS week, COUNT(*) AS bp_days_in_week
    FROM googlefit_bp_with_weeks
    WHERE ga_week BETWEEN {first_week} AND {last_week}
    GROUP BY 1, 2
    ),

    weeks AS (
    SELECT
        w1.participantidentifier,
        CAST(week AS integer) AS week
    FROM w1
    CROSS JOIN UNNEST(sequence({first_week}, {last_week})) AS t(week)
    )

    SELECT
    w.participantidentifier,
    w.week,
    GREATEST(COALESCE(ww.weight_days_in_week, 0), COALESCE(bw.bp_days_in_week, 0), COALESCE(gb.bp_days_in_week, 0)) AS meets_2x
    FROM weeks w
    LEFT JOIN bp_weekly bw
    ON bw.participantidentifier = w.participantidentifier
    AND bw.week = w.week
    LEFT JOIN wt_weekly ww
    ON ww.participantidentifier = w.participantidentifier
    AND ww.week = w.week
    LEFT JOIN googlefit_bp_weekly gb
    ON gb.participantidentifier = w.participantidentifier
    AND gb.week = w.week
    ORDER BY w.week;
    """
    result = mdh_athena.execQuery(query)
    return [int(i) for i in result['meets_2x'].tolist()]


def show_heatmap_for_stage(participant_email, participantidentifier, first_week, last_week, title, w1, ring_vendor='uh', is_postpartum=False):
    # Check if this is postpartum and if we have delivery info
    delivery_info = None
    if is_postpartum:
        edd_final, delivery_date, postpartum_days = get_participant_delivery_info(participantidentifier)
        if delivery_date and postpartum_days:
            delivery_info = (delivery_date, postpartum_days)
    
    if delivery_info and is_postpartum:
        # Use delivery-based calculations for postpartum
        delivery_date, postpartum_days = delivery_info
        weeks = [f"PP W{w}" for w in range(first_week, last_week + 1)]  # PP = PostPartum
        
        frame = {
            "Symptom check-in (daily)": calculate_daily_symptoms_postpartum(participantidentifier, first_week, last_week, delivery_date, postpartum_days),
            "Daily questions (1-5 Q)": calculate_daily_questions_postpartum(participantidentifier, first_week, last_week, delivery_date, postpartum_days),
            "Weekly/bimonthly questionnaire": calculate_weekly_bimontly_surveys_postpartum(participantidentifier, first_week, last_week, delivery_date, postpartum_days),
            "Weight(per week)": calculate_weight_measurements_postpartum(participantidentifier, first_week, last_week, delivery_date, postpartum_days),
            "BP (per week)": calculate_bp_measurements_postpartum(participantidentifier, first_week, last_week, delivery_date, postpartum_days)
        }

        if ring_vendor == 'oura':
            frame["Oura - Smart ring wear (~19h/day)"] = calculate_daily_wear_from_oura_postpartum(participantidentifier, first_week, last_week, delivery_date, postpartum_days)
        else:
            if os.getenv('UH_API_CALL'):
                # Calculate device wear for each postpartum week using API
                week_ranges = calculate_postpartum_weeks_from_delivery(participantidentifier, first_week, last_week, delivery_date, postpartum_days)
                wear_counts = []
                for week_num, week_start, week_end in week_ranges:
                    count = get_weekly_wear_count(participant_email, week_start, week_end)
                    wear_counts.append(count)
                # Pad with zeros for any missing weeks
                while len(wear_counts) < (last_week - first_week + 1):
                    wear_counts.append(0)
                frame["Smart ring wear (~19h/day)"] = wear_counts
            else:
                frame["UH - Smart ring wear (~19h/day)"] = calculate_daily_wear_from_uh_postpartum(participantidentifier, first_week, last_week, delivery_date, postpartum_days)
        
        # Update title to indicate delivery-based calculation
        title = title.replace("Postpartum", f"Postpartum (from delivery {delivery_date.strftime('%Y-%m-%d')})")
    else:
        # Use original gestational week-based calculations
        weeks = [f"W{w}" for w in range(first_week, last_week + 1)]

        frame = {
            "Symptom check-in (daily)": calculate_daily_symptoms(participantidentifier, first_week, last_week),
            "Daily questions (1-5 Q)": calculate_daily_questions(participantidentifier, first_week, last_week),
            "Weekly/bimonthly questionnaire": calculate_weekly_bimontly_surveys(participantidentifier, first_week, last_week),
            "Weight(per week)": calculate_weight_measurements(participantidentifier, first_week, last_week),
            "BP (per week)": calculate_bp_measurements(participantidentifier, first_week, last_week)
        }

        if ring_vendor == 'oura':
            frame["Oura - Smart ring wear (~19h/day)"] = calculate_daily_wear_from_oura(participantidentifier, first_week, last_week) # from MDH
        else:
            if os.getenv('UH_API_CALL'):
                # Calculate device wear for each week
                for i, week_num in enumerate(range(first_week, last_week + 1)):
                    # Calculate the start date of the week (w1 is the start of week 1)
                    week_start = w1 + timedelta(days=(week_num - 1) * 7)
                    week_end = week_start + timedelta(days=6)
                    frame["Smart ring wear (~19h/day)"][i] = get_weekly_wear_count(participant_email, week_start, week_end)
            else:
                frame["UH - Smart ring wear (~19h/day)"] = calculate_daily_wear_from_uh(participantidentifier, w1, first_week, last_week) # UH AWS

    df = pd.DataFrame(frame, index=weeks).T

    # Plot heatmap
    fig = plt.figure(figsize=(11, 4))
    ax = sns.heatmap(
        df,
        vmin=0,
        vmax=7,
        cmap="YlGn",
        linewidths=0.5,
        linecolor="white",
        cbar_kws={"label": "Days (0-7) or frequency per week"},
        annot=True,
        fmt="g",
    )
    ax.set_title(title)
    plt.tight_layout()

    percentage_fig = show_percentage_heatmap_for_stage(frame, first_week, last_week, title + " in Percentage (%)")

    return fig, percentage_fig

def show_percentage_heatmap_for_stage(frame, first_week, last_week, title):
    weeks = [f"W{w}" for w in range(first_week, last_week + 1)]
    for key, value in frame.items():
        if key == "Symptom check-in (daily)":
            frame[key] = [(i/7 * 100) for i in value]
        if key == "Daily questions (1-5 Q)":
            frame[key] = [i/7 * 100 for i in value]
        if key == "Weekly/bimonthly questionnaire":
            percentages = []
            for i in value:
                if i >= 1:
                    percentages.append(100)
                else:
                    percentages.append(0)
            frame[key] = percentages
        if key == "UH - Smart ring wear (~19h/day)":
            frame[key] = [i/7 * 100 for i in value]
        if key == "Oura - Smart ring wear (~19h/day)":
            frame[key] = [i/7 * 100 for i in value]
        if key == "Weight(per week)":
            percentages = []
            for i in value:
                if i >= 2:
                    percentages.append(100)
                    continue
                percentages.append(i / 2 * 100)
            frame[key] = percentages
        if key == "BP (per week)":
            percentages = []
            for i in value:
                if i >= 2:
                    percentages.append(100)
                    continue
                percentages.append(i / 2 * 100)
            frame[key] = percentages

    df = pd.DataFrame(frame, index=weeks).T
    self_report_average = df.iloc[:3].sum(axis=0).astype(int) / 3
    biometrics_average = df.iloc[3:].sum(axis=0).astype(int) / 3
    df.loc["Self Report Average"] = self_report_average
    df.loc["Biometrics Average"] = biometrics_average


    weekly_comp = (
        (self_report_average >= 70).astype(int) * 3
        + (biometrics_average >= 70).astype(int) * 4
    )
    df.loc["Weekly Compensation ($)"] = weekly_comp

    # Prepare per-cell annotations: % for all rows except compensation (use $)
    annot_labels = df.copy()
    for row in df.index:
        if row == "Weekly Compensation ($)":
            annot_labels.loc[row] = annot_labels.loc[row].map(lambda v: f"${int(round(v))}")
        else:
            annot_labels.loc[row] = annot_labels.loc[row].map(lambda v: f"{float(v):.1f}%")

    # Plot heatmap
    fig = plt.figure(figsize=(11, 5))
    ax = sns.heatmap(
        df,
        vmin=0,
        vmax=100,
        cmap="YlGn",
        linewidths=0.5,
        linecolor="white",
        annot=annot_labels.values,
        fmt='',
    )
    ax.set_title(title)

    # Bold line before "Self Report Average"
    try:
        sep_y = df.index.get_loc("Self Report Average")
        ax.hlines(sep_y, *ax.get_xlim(), colors="black", linewidth=2.8)
    except KeyError:
        pass


    stage_total = int(weekly_comp.sum())
    fig.subplots_adjust(top=10)
    fig.subplots_adjust(bottom=9)  # make room for the caption
    fig.text(
        0.5, 0.0005,
        f"Total compensation for this stage: ${stage_total}",
        ha="center", va="bottom", fontsize=10, fontweight="bold"
    )

    plt.tight_layout()

    return fig

def participant_first_w1_day(participantidentifier):
    first_date_final_edd_query = f"""
    WITH
    edd AS (
        SELECT
        participantidentifier,
        date_parse(
            json_extract_scalar(cast(customfields AS JSON), '$.edd_final'),
            '%Y-%m-%d'
        ) edd_final
        FROM
        allparticipants
        WHERE participantidentifier = '{participantidentifier}'
    )
    SELECT
    participantidentifier,
    edd_final - interval '280' day w1,
    edd_final
    FROM
    edd
    WHERE
    edd_final IS NOT NULL
    """

    result = mdh_athena.execQuery(first_date_final_edd_query)
    first_w1_day = result['w1'][0]
    format_string = '%Y-%m-%d %H:%M:%S.%f'
    return datetime.strptime(first_w1_day, format_string)

def get_participant_delivery_info(participantidentifier):
    """Get delivery date and postpartum days from participant custom fields"""
    query = f"""
    SELECT
        participantidentifier,
        date_parse(
            json_extract_scalar(cast(customfields AS JSON), '$.edd_final'),
            '%Y-%m-%d'
        ) AS edd_final,
        date_parse(
            json_extract_scalar(cast(customfields AS JSON), '$.delivery_date'),
            '%Y-%m-%d'
        ) AS delivery_date,
        CAST(json_extract_scalar(cast(customfields AS JSON), '$.postpartum_days') AS integer) AS postpartum_days
    FROM allparticipants
    WHERE participantidentifier = '{participantidentifier}'
    """
    
    result = mdh_athena.execQuery(query)
    if len(result) == 0:
        return None, None, None
    
    row = result.iloc[0]
    edd_final = row['edd_final']
    delivery_date = row['delivery_date'] 
    postpartum_days = float(row['postpartum_days']) if row['postpartum_days'] is not None else None
    
    # Convert to datetime objects if not None
    if edd_final:
        edd_final = datetime.strptime(str(edd_final), '%Y-%m-%d %H:%M:%S.%f')
    if delivery_date:
        delivery_date = datetime.strptime(str(delivery_date), '%Y-%m-%d %H:%M:%S.%f')
        
    return edd_final, delivery_date, postpartum_days

def calculate_postpartum_weeks_from_delivery(participantidentifier, first_week, last_week, delivery_date, postpartum_days):
    """Calculate postpartum weeks based on actual delivery date and postpartum_days"""
    if not delivery_date or not postpartum_days:
        return None
    
    # Calculate the end of the postpartum period
    postpartum_end_date = delivery_date + timedelta(days=postpartum_days)
    
    # For postpartum weeks, week 1 starts on delivery_date
    # first_week and last_week are postpartum week numbers (1, 2, 3, etc.)
    week_ranges = []
    for week_num in range(first_week, last_week + 1):
        week_start = delivery_date + timedelta(days=(week_num - 1) * 7)
        week_end = week_start + timedelta(days=6)
        
        # Only include weeks that fall within the postpartum period
        if week_start <= postpartum_end_date:
            week_ranges.append((week_num, week_start, min(week_end, postpartum_end_date)))
    
    return week_ranges

def calculate_daily_symptoms_postpartum(participantidentifier, first_week, last_week, delivery_date, postpartum_days):
    """Calculate daily symptoms for postpartum period based on delivery date"""
    week_ranges = calculate_postpartum_weeks_from_delivery(participantidentifier, first_week, last_week, delivery_date, postpartum_days)
    if not week_ranges:
        return [0] * (last_week - first_week + 1)
    
    # Get all symptom check-in dates for the postpartum period
    postpartum_end_date = delivery_date + timedelta(days=postpartum_days)
    
    query = f"""
    SELECT
        participantidentifier,
        CAST(inserteddate AS date) AS day_date
    FROM projectdevicedata
    WHERE participantidentifier = '{participantidentifier}'
        AND CAST(inserteddate AS date) BETWEEN DATE '{delivery_date.date()}'
        AND DATE '{postpartum_end_date.date()}'
    GROUP BY 1, 2
    """
    
    result = mdh_athena.execQuery(query)
    checkin_dates = set()
    if len(result) > 0:
        checkin_dates = set(pd.to_datetime(result['day_date']).dt.date)
    
    # Count checkins for each week
    weekly_counts = []
    for week_num, week_start, week_end in week_ranges:
        count = 0
        current_date = week_start.date()
        while current_date <= week_end.date():
            if current_date in checkin_dates:
                count += 1
            current_date += timedelta(days=1)
        weekly_counts.append(count)
    
    # Pad with zeros for any missing weeks
    while len(weekly_counts) < (last_week - first_week + 1):
        weekly_counts.append(0)
    
    return weekly_counts

def calculate_daily_questions_postpartum(participantidentifier, first_week, last_week, delivery_date, postpartum_days):
    """Calculate daily questions for postpartum period based on delivery date"""
    week_ranges = calculate_postpartum_weeks_from_delivery(participantidentifier, first_week, last_week, delivery_date, postpartum_days)
    if not week_ranges:
        return [0] * (last_week - first_week + 1)
    
    postpartum_end_date = delivery_date + timedelta(days=postpartum_days)
    
    query = f"""    
    WITH ema_results AS (
        SELECT surveyresultkey, surveyname
        FROM surveyresults
        WHERE surveyname IN ('EMA PM', 'EMA AM')
    ),
    answers AS (
    SELECT
        sqr.participantidentifier,
        er.surveyname,
        CAST(sqr.startdate - INTERVAL '7' HOUR AS date) AS day_date,
        sqr.resultidentifier,
        sqr.surveyresultkey
    FROM surveyquestionresults sqr
    JOIN ema_results er
        ON er.surveyresultkey = sqr.surveyresultkey
    WHERE sqr.participantidentifier = '{participantidentifier}'
        AND CAST(sqr.startdate - INTERVAL '7' HOUR AS date) BETWEEN DATE '{delivery_date.date()}'
        AND DATE '{postpartum_end_date.date()}'
    ),
    day_counts AS (
    SELECT
        a.participantidentifier,
        a.day_date,
        COUNT(DISTINCT a.resultidentifier) AS questions_answered
    FROM answers a
    GROUP BY 1, 2
    )
    SELECT
        participantidentifier,
        day_date,
        questions_answered
    FROM day_counts
    WHERE questions_answered >= 6
    ORDER BY day_date
    """
    
    result = mdh_athena.execQuery(query)
    question_dates = set()
    if len(result) > 0:
        question_dates = set(pd.to_datetime(result['day_date']).dt.date)
    
    # Count question days for each week
    weekly_counts = []
    for week_num, week_start, week_end in week_ranges:
        count = 0
        current_date = week_start.date()
        while current_date <= week_end.date():
            if current_date in question_dates:
                count += 1
            current_date += timedelta(days=1)
        weekly_counts.append(count)
    
    # Pad with zeros for any missing weeks
    while len(weekly_counts) < (last_week - first_week + 1):
        weekly_counts.append(0)
    
    return weekly_counts

def calculate_weekly_bimontly_surveys_postpartum(participantidentifier, first_week, last_week, delivery_date, postpartum_days):
    """Calculate weekly/bimonthly surveys for postpartum period based on delivery date"""
    week_ranges = calculate_postpartum_weeks_from_delivery(participantidentifier, first_week, last_week, delivery_date, postpartum_days)
    if not week_ranges:
        return [0] * (last_week - first_week + 1)
    
    postpartum_end_date = delivery_date + timedelta(days=postpartum_days)
    
    query = f"""
    WITH sr AS (
    SELECT surveyresultkey, surveyname
    FROM surveyresults
    WHERE surveyname IN (
        -- weekly surveys
        'mMOS (Weekly)',
        'PROMIS Sleep (Weekly)',
        'BRCS (Weekly)',
        'Pregnancy Experience Scale',
        -- exception (biomonthly) surveys
        'Maternal Antenatal Attachment Scale',
        'Edinburgh Postnatal Depression Scale (EPDS)',
        'Perinatal Anxiety Screening Scale (PASS)'
    )
    ),
    submissions AS (
    SELECT
        sqr.participantidentifier,
        sr.surveyname,
        CAST(MIN(sqr.startdate - INTERVAL '7' HOUR) AS date) AS day_date,
        sqr.surveyresultkey
    FROM surveyquestionresults sqr
    JOIN sr
        ON sr.surveyresultkey = sqr.surveyresultkey
    WHERE sqr.participantidentifier = '{participantidentifier}'
        AND CAST(sqr.startdate - INTERVAL '7' HOUR AS date) BETWEEN DATE '{delivery_date.date()}'
        AND DATE '{postpartum_end_date.date()}'
    GROUP BY sqr.participantidentifier, sr.surveyname, sqr.surveyresultkey
    )
    SELECT
        participantidentifier,
        surveyname,
        day_date
    FROM submissions
    ORDER BY day_date
    """
    
    result = mdh_athena.execQuery(query)
    
    # Group surveys by date
    surveys_by_date = {}
    if len(result) > 0:
        for _, row in result.iterrows():
            date = pd.to_datetime(row['day_date']).date()
            if date not in surveys_by_date:
                surveys_by_date[date] = set()
            surveys_by_date[date].add(row['surveyname'])
    
    # Count surveys for each week
    weekly_counts = []
    for week_num, week_start, week_end in week_ranges:
        count = 0
        current_date = week_start.date()
        while current_date <= week_end.date():
            if current_date in surveys_by_date:
                count += len(surveys_by_date[current_date])
            current_date += timedelta(days=1)
        weekly_counts.append(min(count, 1))  # Cap at 1 per week
    
    # Pad with zeros for any missing weeks
    while len(weekly_counts) < (last_week - first_week + 1):
        weekly_counts.append(0)
    
    return weekly_counts

def calculate_weight_measurements_postpartum(participantidentifier, first_week, last_week, delivery_date, postpartum_days):
    """Calculate weight measurements for postpartum period based on delivery date"""
    week_ranges = calculate_postpartum_weeks_from_delivery(participantidentifier, first_week, last_week, delivery_date, postpartum_days)
    if not week_ranges:
        return [0] * (last_week - first_week + 1)
    
    postpartum_end_date = delivery_date + timedelta(days=postpartum_days)
    
    query = f"""
    WITH googlefit_wt_src AS (
        SELECT
            participantidentifier,
            CAST(COALESCE(windowstart - INTERVAL '7' HOUR) AS date) AS day_date
        FROM googlefitsamples
        WHERE participantidentifier = '{participantidentifier}'
        AND type = 'Weight'
        AND CAST(COALESCE(windowstart - INTERVAL '7' HOUR) AS date) BETWEEN DATE '{delivery_date.date()}'
        AND DATE '{postpartum_end_date.date()}'
    ),
    googlefit_wt_days AS (
        SELECT participantidentifier, day_date
        FROM googlefit_wt_src
        GROUP BY 1, 2
    ),
    wt_src AS (
    SELECT
        participantidentifier,
        CAST(COALESCE(startdate - INTERVAL '7' HOUR) AS date) AS day_date
    FROM healthkitv2samples
    WHERE participantidentifier = '{participantidentifier}'
        AND type = 'Weight'
        AND CAST(COALESCE(startdate - INTERVAL '7' HOUR) AS date) BETWEEN DATE '{delivery_date.date()}'
        AND DATE '{postpartum_end_date.date()}'
    ),
    wt_days AS (
    SELECT participantidentifier, day_date
    FROM wt_src
    GROUP BY 1, 2
    ),
    all_weight_days AS (
        SELECT participantidentifier, day_date FROM googlefit_wt_days
        UNION
        SELECT participantidentifier, day_date FROM wt_days
    )
    SELECT
        participantidentifier,
        day_date
    FROM all_weight_days
    ORDER BY day_date
    """
    
    result = mdh_athena.execQuery(query)
    weight_dates = set()
    if len(result) > 0:
        weight_dates = set(pd.to_datetime(result['day_date']).dt.date)
    
    # Count weight measurements for each week
    weekly_counts = []
    for week_num, week_start, week_end in week_ranges:
        count = 0
        current_date = week_start.date()
        while current_date <= week_end.date():
            if current_date in weight_dates:
                count += 1
            current_date += timedelta(days=1)
        weekly_counts.append(count)
    
    # Pad with zeros for any missing weeks
    while len(weekly_counts) < (last_week - first_week + 1):
        weekly_counts.append(0)
    
    return weekly_counts

def calculate_bp_measurements_postpartum(participantidentifier, first_week, last_week, delivery_date, postpartum_days):
    """Calculate BP measurements for postpartum period based on delivery date"""
    week_ranges = calculate_postpartum_weeks_from_delivery(participantidentifier, first_week, last_week, delivery_date, postpartum_days)
    if not week_ranges:
        return [0] * (last_week - first_week + 1)
    
    postpartum_end_date = delivery_date + timedelta(days=postpartum_days)
    
    query = f"""
    WITH bp_src AS (
    SELECT
        participantidentifier,
        CAST(COALESCE(datetimelocal, datetime, inserteddate) AS date) AS day_date
    FROM omronbloodpressure
    WHERE participantidentifier = '{participantidentifier}'
        AND CAST(COALESCE(datetimelocal, datetime, inserteddate) AS date) BETWEEN DATE '{delivery_date.date()}'
        AND DATE '{postpartum_end_date.date()}'
    ),
    bp_days AS (
    SELECT participantidentifier, day_date
    FROM bp_src
    GROUP BY 1, 2
    ),
    googlefit_bp_src AS (
        SELECT
            participantidentifier,
            CAST(COALESCE(windowstart - INTERVAL '7' HOUR) AS date) AS day_date
        FROM googlefitsamples
        WHERE participantidentifier = '{participantidentifier}'
        AND (type = 'blood_pressure_diastolic' OR type = 'blood_pressure_systolic')
        AND CAST(COALESCE(windowstart - INTERVAL '7' HOUR) AS date) BETWEEN DATE '{delivery_date.date()}'
        AND DATE '{postpartum_end_date.date()}'
    ),
    googlefit_bp_days AS (
    SELECT participantidentifier, day_date
    FROM googlefit_bp_src
    GROUP BY 1, 2
    ),
    wt_src AS (
    SELECT
        participantidentifier,
        CAST(COALESCE(startdate - INTERVAL '7' HOUR) AS date) AS day_date
    FROM healthkitv2samples
    WHERE participantidentifier = '{participantidentifier}'
        AND (type = 'BloodPressureSystolic' OR type = 'BloodPressureDiastolic')
        AND CAST(COALESCE(startdate - INTERVAL '7' HOUR) AS date) BETWEEN DATE '{delivery_date.date()}'
        AND DATE '{postpartum_end_date.date()}'
    ),
    wt_days AS (
    SELECT participantidentifier, day_date
    FROM wt_src
    GROUP BY 1, 2
    ),
    all_bp_days AS (
        SELECT participantidentifier, day_date FROM bp_days
        UNION
        SELECT participantidentifier, day_date FROM googlefit_bp_days
        UNION
        SELECT participantidentifier, day_date FROM wt_days
    )
    SELECT
        participantidentifier,
        day_date
    FROM all_bp_days
    ORDER BY day_date
    """
    
    result = mdh_athena.execQuery(query)
    bp_dates = set()
    if len(result) > 0:
        bp_dates = set(pd.to_datetime(result['day_date']).dt.date)
    
    # Count BP measurements for each week
    weekly_counts = []
    for week_num, week_start, week_end in week_ranges:
        count = 0
        current_date = week_start.date()
        while current_date <= week_end.date():
            if current_date in bp_dates:
                count += 1
            current_date += timedelta(days=1)
        weekly_counts.append(count)
    
    # Pad with zeros for any missing weeks
    while len(weekly_counts) < (last_week - first_week + 1):
        weekly_counts.append(0)
    
    return weekly_counts

def calculate_daily_wear_from_oura_postpartum(participantidentifier, first_week, last_week, delivery_date, postpartum_days):
    """Calculate Oura ring wear for postpartum period based on delivery date"""
    week_ranges = calculate_postpartum_weeks_from_delivery(participantidentifier, first_week, last_week, delivery_date, postpartum_days)
    if not week_ranges:
        return [0] * (last_week - first_week + 1)
    
    postpartum_end_date = delivery_date + timedelta(days=postpartum_days)
    
    query = f"""
    SELECT
        participantidentifier,
        CAST("timestamp" AS date) AS day_date,
        -- Clamp to [0,1] and ensure floating math
        GREATEST(0.0, LEAST(1.0, 1.0 - CAST(COALESCE(nonweartime, 0) AS DOUBLE) / 86400.0)) AS wear_fraction
    FROM ouradailyactivity
    WHERE participantidentifier = '{participantidentifier}'
        AND CAST("timestamp" AS date) BETWEEN DATE '{delivery_date.date()}'
        AND DATE '{postpartum_end_date.date()}'
    """
    
    result = mdh_athena.execQuery(query)
    wear_days = set()
    if len(result) > 0:
        for _, row in result.iterrows():
            if row['wear_fraction'] >= 0.75:
                wear_days.add(pd.to_datetime(row['day_date']).date())
    
    # Count wear days for each week
    weekly_counts = []
    for week_num, week_start, week_end in week_ranges:
        count = 0
        current_date = week_start.date()
        while current_date <= week_end.date():
            if current_date in wear_days:
                count += 1
            current_date += timedelta(days=1)
        weekly_counts.append(count)
    
    # Pad with zeros for any missing weeks
    while len(weekly_counts) < (last_week - first_week + 1):
        weekly_counts.append(0)
    
    return weekly_counts

def calculate_daily_wear_from_uh_postpartum(participantidentifier, first_week, last_week, delivery_date, postpartum_days):
    """Calculate UH ring wear for postpartum period based on delivery date"""
    week_ranges = calculate_postpartum_weeks_from_delivery(participantidentifier, first_week, last_week, delivery_date, postpartum_days)
    if not week_ranges:
        return [0] * (last_week - first_week + 1)
    
    postpartum_end_date = delivery_date + timedelta(days=postpartum_days)
    
    query = f"""
    SELECT
        pid,
        CAST(from_iso8601_timestamp(object_day_start_timestamp_iso8601_tz) AS date) AS day_date,
        COUNT(DISTINCT object_values_timestamp) AS samples_in_day
    FROM temp
    WHERE pid = '{participantidentifier}'
        AND object_day_start_timestamp_iso8601_tz IS NOT NULL
        AND CAST(from_iso8601_timestamp(object_day_start_timestamp_iso8601_tz) AS date) BETWEEN DATE '{delivery_date.date()}'
        AND DATE '{postpartum_end_date.date()}'
    GROUP BY 1, 2
    """
    
    result = aws_athena.execQuery(query)
    wear_days = set()
    if len(result) > 0:
        for _, row in result.iterrows():
            if row['samples_in_day'] >= 0.75 * 288:  # 75% of 288 samples per day
                wear_days.add(pd.to_datetime(row['day_date']).date())
    
    # Count wear days for each week
    weekly_counts = []
    for week_num, week_start, week_end in week_ranges:
        count = 0
        current_date = week_start.date()
        while current_date <= week_end.date():
            if current_date in wear_days:
                count += 1
            current_date += timedelta(days=1)
        weekly_counts.append(count)
    
    # Pad with zeros for any missing weeks
    while len(weekly_counts) < (last_week - first_week + 1):
        weekly_counts.append(0)
    
    return weekly_counts

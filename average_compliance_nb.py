import marimo

__generated_with = "0.15.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import io
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    import seaborn as sns
    import os

    def fig_to_image(fig):
        """Convert a matplotlib figure to a static mo.image for reliable rendering."""
        if fig is None:
            return mo.md("")
        buf = io.BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight', dpi=150)
        buf.seek(0)
        return mo.image(buf.read(), width=1100)

    return fig_to_image, mo, np, os, pd, plt, sns


@app.cell
def _():
    from sensorfabric.mdh import MDH
    from query_cache import CachedNeedle
    return MDH, CachedNeedle


@app.cell
def _(MDH, CachedNeedle):
    mdh = MDH()
    mdh_athena = CachedNeedle(method="mdh")
    return mdh, mdh_athena


@app.cell
def _(mo):
    mo.md(
        r"""
    # Average Compliance Report — All Participants

    This report shows the **average weekly compliance percentage** across all enrolled participants, grouped by gestational week.

    Each activity is converted to a percentage of its weekly maximum:
    - Daily symptom check-ins: days/7 × 100%
    - Daily 6 questions: days/7 × 100%
    - Weekly/bimonthly questionnaire: 100% if ≥1 completed, else 0%
    - Smart ring wear: days/7 × 100%
    - Weight measurements: 100% if ≥2, else count/2 × 100%
    - BP measurements: 100% if ≥2, else count/2 × 100%

    Averages are computed only across participants whose gestational timeline includes that week.
    """
    )
    return


@app.cell
def _(mdh, mdh_athena, pd):
    # Get enrolled participants from segment
    segmentID = os.getenv('MDH_SEGMENT_ID') # segment ID of enrolled participants
    all_participants_data = mdh.getAllParticipants({'segmentID': segmentID})
    participant_ids = [p['participantIdentifier'] for p in all_participants_data['participants']]
    participant_count = len(participant_ids)
    # Format for SQL IN clause
    participant_ids_sql = ",".join([f"'{pid}'" for pid in participant_ids])
    return participant_count, participant_ids_sql


@app.cell
def _(mo, participant_count):
    mo.md(f"**Total participants with EDD: {participant_count}**")
    return


@app.cell
def _(mdh_athena, pd):
    def get_avg_daily_symptoms(first_week, last_week, participant_ids_sql):
        """Get average daily symptom check-in compliance across all participants by GA week."""
        query = f"""
        WITH edd AS (
            SELECT
                participantidentifier,
                date_parse(json_extract_scalar(cast(customfields AS JSON), '$.edd_final'), '%Y-%m-%d') AS edd_final
            FROM allparticipants
            WHERE participantidentifier IN ({participant_ids_sql})
                AND json_extract_scalar(cast(customfields AS JSON), '$.edd_final') IS NOT NULL
                AND json_extract_scalar(cast(customfields AS JSON), '$.edd_final') != ''
        ),
        w1 AS (
            SELECT
                participantidentifier,
                CAST(edd_final AS date) - INTERVAL '280' DAY AS w1_date,
                1 + CAST(date_diff('day', CAST(edd_final AS date) - INTERVAL '280' DAY, CURRENT_DATE) / 7 AS integer) AS current_ga_week
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
                    date_add('day', 7 * ({first_week} - 1), w.w1_date),
                    date_add('day', 7 * {last_week} - 1, w.w1_date),
                    INTERVAL '1' DAY
                )
            ) AS t(d)
            WHERE d <= CURRENT_DATE
        ),
        pdd_days AS (
            SELECT
                participantidentifier,
                CAST(inserteddate AS date) AS day_date
            FROM projectdevicedata
            WHERE participantidentifier IN ({participant_ids_sql})
            GROUP BY 1, 2
        ),
        weekly_counts AS (
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
        )
        SELECT
            week,
            AVG(CAST(days_with_checkin AS DOUBLE) / 7.0 * 100) AS avg_pct,
            COUNT(DISTINCT participantidentifier) AS n_participants
        FROM weekly_counts
        GROUP BY week
        ORDER BY week
        """
        return mdh_athena.execQuery(query)

    return (get_avg_daily_symptoms,)


@app.cell
def _(mdh_athena, pd):
    def get_avg_daily_questions(first_week, last_week, participant_ids_sql):
        """Get average daily questions compliance across all participants by GA week."""
        query = f"""
        WITH ema_results AS (
            SELECT surveyresultkey, surveyname
            FROM surveyresults
            WHERE surveyname IN ('EMA PM', 'EMA AM')
        ),
        answers AS (
            SELECT
                sqr.participantidentifier,
                CAST(sqr.startdate - INTERVAL '7' HOUR AS date) AS day_date,
                sqr.resultidentifier
            FROM surveyquestionresults sqr
            JOIN ema_results er
                ON er.surveyresultkey = sqr.surveyresultkey
            WHERE sqr.participantidentifier IN ({participant_ids_sql})
        ),
        edd AS (
            SELECT
                participantidentifier,
                date_parse(json_extract_scalar(cast(customfields AS JSON), '$.edd_final'), '%Y-%m-%d') AS edd_final
            FROM allparticipants
            WHERE participantidentifier IN ({participant_ids_sql})
                AND json_extract_scalar(cast(customfields AS JSON), '$.edd_final') IS NOT NULL
                AND json_extract_scalar(cast(customfields AS JSON), '$.edd_final') != ''
        ),
        w1 AS (
            SELECT
                participantidentifier,
                CAST(edd_final AS date) - INTERVAL '280' DAY AS w1_date,
                1 + CAST(date_diff('day', CAST(edd_final AS date) - INTERVAL '280' DAY, CURRENT_DATE) / 7 AS integer) AS current_ga_week
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
                SUM(CASE WHEN questions_answered >= 6 THEN 1 ELSE 0 END) AS days_with_6q
            FROM days_with_weeks
            WHERE ga_week BETWEEN {first_week} AND {last_week}
            GROUP BY 1, 2
        ),
        all_participants_weeks AS (
            SELECT
                w1.participantidentifier,
                CAST(week AS integer) AS week
            FROM w1
            CROSS JOIN UNNEST(sequence({first_week}, {last_week})) AS t(week)
            WHERE CAST(week AS integer) <= w1.current_ga_week
        )
        SELECT
            w.week,
            AVG(CAST(COALESCE(wc.days_with_6q, 0) AS DOUBLE) / 7.0 * 100) AS avg_pct,
            COUNT(DISTINCT w.participantidentifier) AS n_participants
        FROM all_participants_weeks w
        LEFT JOIN weekly_counts wc
            ON wc.participantidentifier = w.participantidentifier
            AND wc.week = w.week
        GROUP BY w.week
        ORDER BY w.week
        """
        return mdh_athena.execQuery(query)

    return (get_avg_daily_questions,)


@app.cell
def _(mdh_athena, pd):
    def get_avg_weekly_surveys(first_week, last_week, participant_ids_sql):
        """Get average weekly/bimonthly survey compliance across all participants by GA week."""
        query = f"""
        WITH sr AS (
            SELECT surveyresultkey, surveyname
            FROM surveyresults
            WHERE surveyname IN (
                'mMOS (Weekly)', 'PROMIS Sleep (Weekly)', 'BRCS (Weekly)',
                'Pregnancy Experience Scale', 'Maternal Antenatal Attachment Scale',
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
            JOIN sr ON sr.surveyresultkey = sqr.surveyresultkey
            WHERE sqr.participantidentifier IN ({participant_ids_sql})
            GROUP BY sqr.participantidentifier, sr.surveyname, sqr.surveyresultkey
        ),
        edd AS (
            SELECT
                participantidentifier,
                date_parse(json_extract_scalar(cast(customfields AS JSON), '$.edd_final'), '%Y-%m-%d') AS edd_final
            FROM allparticipants
            WHERE participantidentifier IN ({participant_ids_sql})
                AND json_extract_scalar(cast(customfields AS JSON), '$.edd_final') IS NOT NULL
                AND json_extract_scalar(cast(customfields AS JSON), '$.edd_final') != ''
        ),
        w1 AS (
            SELECT
                participantidentifier,
                CAST(edd_final AS date) - INTERVAL '280' DAY AS w1_date,
                1 + CAST(date_diff('day', CAST(edd_final AS date) - INTERVAL '280' DAY, CURRENT_DATE) / 7 AS integer) AS current_ga_week
            FROM edd
            WHERE edd_final IS NOT NULL
        ),
        submissions_with_weeks AS (
            SELECT
                s.participantidentifier,
                s.surveyname,
                1 + CAST(date_diff('day', w.w1_date, s.day_date) / 7 AS integer) AS ga_week
            FROM submissions s
            JOIN w1 w ON w.participantidentifier = s.participantidentifier
        ),
        weekly_flags AS (
            SELECT
                participantidentifier,
                ga_week AS week,
                GREATEST(
                    MAX(CASE WHEN surveyname = 'mMOS (Weekly)' THEN 1 ELSE 0 END),
                    MAX(CASE WHEN surveyname = 'PROMIS Sleep (Weekly)' THEN 1 ELSE 0 END),
                    MAX(CASE WHEN surveyname = 'BRCS (Weekly)' THEN 1 ELSE 0 END),
                    MAX(CASE WHEN surveyname = 'Pregnancy Experience Scale' THEN 1 ELSE 0 END)
                ) AS any_survey_done
            FROM submissions_with_weeks
            WHERE ga_week BETWEEN {first_week} AND {last_week}
            GROUP BY 1, 2
        ),
        all_participants_weeks AS (
            SELECT
                w1.participantidentifier,
                CAST(week AS integer) AS week
            FROM w1
            CROSS JOIN UNNEST(sequence({first_week}, {last_week})) AS t(week)
            WHERE CAST(week AS integer) <= w1.current_ga_week
        )
        SELECT
            w.week,
            AVG(CAST(COALESCE(wf.any_survey_done, 0) AS DOUBLE) * 100) AS avg_pct,
            COUNT(DISTINCT w.participantidentifier) AS n_participants
        FROM all_participants_weeks w
        LEFT JOIN weekly_flags wf
            ON wf.participantidentifier = w.participantidentifier
            AND wf.week = w.week
        GROUP BY w.week
        ORDER BY w.week
        """
        return mdh_athena.execQuery(query)

    return (get_avg_weekly_surveys,)


@app.cell
def _(mdh_athena, pd):
    def get_avg_ring_wear(first_week, last_week, participant_ids_sql):
        """Get average ring wear compliance (percentage and hours) across all participants by GA week (Oura)."""
        query = f"""
        WITH edd AS (
            SELECT
                participantidentifier,
                date_parse(json_extract_scalar(cast(customfields AS JSON), '$.edd_final'), '%Y-%m-%d') AS edd_final
            FROM allparticipants
            WHERE participantidentifier IN ({participant_ids_sql})
                AND json_extract_scalar(cast(customfields AS JSON), '$.edd_final') IS NOT NULL
                AND json_extract_scalar(cast(customfields AS JSON), '$.edd_final') != ''
        ),
        w1 AS (
            SELECT
                participantidentifier,
                CAST(edd_final AS date) - INTERVAL '280' DAY AS w1_date,
                1 + CAST(date_diff('day', CAST(edd_final AS date) - INTERVAL '280' DAY, CURRENT_DATE) / 7 AS integer) AS current_ga_week
            FROM edd
            WHERE edd_final IS NOT NULL
        ),
        oura_days AS (
            SELECT
                participantidentifier,
                CAST("timestamp" AS date) AS day_date,
                GREATEST(0.0, LEAST(1.0, 1.0 - CAST(COALESCE(nonweartime, 0) AS DOUBLE) / 86400.0)) AS wear_fraction
            FROM ouradailyactivity
            WHERE participantidentifier IN ({participant_ids_sql})
        ),
        days_with_week AS (
            SELECT
                od.participantidentifier,
                od.day_date,
                1 + CAST(date_diff('day', w.w1_date, od.day_date) / 7 AS integer) AS ga_week,
                od.wear_fraction,
                CASE WHEN od.wear_fraction >= 0.75 THEN 1 ELSE 0 END AS wear_day_flag
            FROM oura_days od
            JOIN w1 w ON w.participantidentifier = od.participantidentifier
        ),
        weekly_stats AS (
            SELECT
                participantidentifier,
                ga_week AS week,
                SUM(wear_day_flag) AS wear_days_ge_75,
                AVG(wear_fraction * 24.0) AS avg_daily_hours
            FROM days_with_week
            WHERE ga_week BETWEEN {first_week} AND {last_week}
            GROUP BY 1, 2
        ),
        all_participants_weeks AS (
            SELECT
                w1.participantidentifier,
                CAST(week AS integer) AS week
            FROM w1
            CROSS JOIN UNNEST(sequence({first_week}, {last_week})) AS t(week)
            WHERE CAST(week AS integer) <= w1.current_ga_week
        )
        SELECT
            w.week,
            AVG(CAST(COALESCE(ws.wear_days_ge_75, 0) AS DOUBLE) / 7.0 * 100) AS avg_pct,
            AVG(COALESCE(ws.avg_daily_hours, 0.0)) AS avg_hours,
            COUNT(DISTINCT w.participantidentifier) AS n_participants
        FROM all_participants_weeks w
        LEFT JOIN weekly_stats ws
            ON ws.participantidentifier = w.participantidentifier
            AND ws.week = w.week
        GROUP BY w.week
        ORDER BY w.week
        """
        return mdh_athena.execQuery(query)

    return (get_avg_ring_wear,)


@app.cell
def _(mdh_athena, pd):
    def get_avg_weight(first_week, last_week, participant_ids_sql):
        """Get average weight measurement compliance across all participants by GA week."""
        query = f"""
        WITH edd AS (
            SELECT
                participantidentifier,
                date_parse(json_extract_scalar(cast(customfields AS JSON), '$.edd_final'), '%Y-%m-%d') AS edd_final
            FROM allparticipants
            WHERE participantidentifier IN ({participant_ids_sql})
                AND json_extract_scalar(cast(customfields AS JSON), '$.edd_final') IS NOT NULL
                AND json_extract_scalar(cast(customfields AS JSON), '$.edd_final') != ''
        ),
        w1 AS (
            SELECT
                participantidentifier,
                CAST(edd_final AS date) - INTERVAL '280' DAY AS w1_date,
                1 + CAST(date_diff('day', CAST(edd_final AS date) - INTERVAL '280' DAY, CURRENT_DATE) / 7 AS integer) AS current_ga_week
            FROM edd
            WHERE edd_final IS NOT NULL
        ),
        wt_src AS (
            SELECT participantidentifier, CAST(COALESCE(startdate - INTERVAL '7' HOUR) AS date) AS day_date
            FROM healthkitv2samples WHERE type = 'Weight' AND participantidentifier IN ({participant_ids_sql})
            UNION ALL
            SELECT participantidentifier, CAST(COALESCE(windowstart - INTERVAL '7' HOUR) AS date) AS day_date
            FROM googlefitsamples WHERE type = 'Weight' AND participantidentifier IN ({participant_ids_sql})
        ),
        wt_days AS (
            SELECT participantidentifier, day_date FROM wt_src GROUP BY 1, 2
        ),
        wt_with_weeks AS (
            SELECT
                wtd.participantidentifier,
                1 + CAST(date_diff('day', w.w1_date, wtd.day_date) / 7 AS integer) AS ga_week
            FROM wt_days wtd
            JOIN w1 w ON w.participantidentifier = wtd.participantidentifier
        ),
        weekly_counts AS (
            SELECT participantidentifier, ga_week AS week, COUNT(*) AS weight_days
            FROM wt_with_weeks
            WHERE ga_week BETWEEN {first_week} AND {last_week}
            GROUP BY 1, 2
        ),
        all_participants_weeks AS (
            SELECT
                w1.participantidentifier,
                CAST(week AS integer) AS week
            FROM w1
            CROSS JOIN UNNEST(sequence({first_week}, {last_week})) AS t(week)
            WHERE CAST(week AS integer) <= w1.current_ga_week
        )
        SELECT
            w.week,
            AVG(LEAST(CAST(COALESCE(wc.weight_days, 0) AS DOUBLE) / 2.0, 1.0) * 100) AS avg_pct,
            COUNT(DISTINCT w.participantidentifier) AS n_participants
        FROM all_participants_weeks w
        LEFT JOIN weekly_counts wc
            ON wc.participantidentifier = w.participantidentifier
            AND wc.week = w.week
        GROUP BY w.week
        ORDER BY w.week
        """
        return mdh_athena.execQuery(query)

    return (get_avg_weight,)


@app.cell
def _(mdh_athena, pd):
    def get_avg_bp(first_week, last_week, participant_ids_sql):
        """Get average BP measurement compliance across all participants by GA week."""
        query = f"""
        WITH edd AS (
            SELECT
                participantidentifier,
                date_parse(json_extract_scalar(cast(customfields AS JSON), '$.edd_final'), '%Y-%m-%d') AS edd_final
            FROM allparticipants
            WHERE participantidentifier IN ({participant_ids_sql})
                AND json_extract_scalar(cast(customfields AS JSON), '$.edd_final') IS NOT NULL
                AND json_extract_scalar(cast(customfields AS JSON), '$.edd_final') != ''
        ),
        w1 AS (
            SELECT
                participantidentifier,
                CAST(edd_final AS date) - INTERVAL '280' DAY AS w1_date,
                1 + CAST(date_diff('day', CAST(edd_final AS date) - INTERVAL '280' DAY, CURRENT_DATE) / 7 AS integer) AS current_ga_week
            FROM edd
            WHERE edd_final IS NOT NULL
        ),
        bp_src AS (
            SELECT participantidentifier, CAST(COALESCE(datetimelocal, datetime, inserteddate) AS date) AS day_date
            FROM omronbloodpressure WHERE participantidentifier IN ({participant_ids_sql})
            UNION ALL
            SELECT participantidentifier, CAST(COALESCE(windowstart - INTERVAL '7' HOUR) AS date) AS day_date
            FROM googlefitsamples WHERE type IN ('blood_pressure_diastolic', 'blood_pressure_systolic') AND participantidentifier IN ({participant_ids_sql})
            UNION ALL
            SELECT participantidentifier, CAST(COALESCE(startdate - INTERVAL '7' HOUR) AS date) AS day_date
            FROM healthkitv2samples WHERE type IN ('BloodPressureSystolic', 'BloodPressureDiastolic') AND participantidentifier IN ({participant_ids_sql})
        ),
        bp_days AS (
            SELECT participantidentifier, day_date FROM bp_src GROUP BY 1, 2
        ),
        bp_with_weeks AS (
            SELECT
                b.participantidentifier,
                1 + CAST(date_diff('day', w.w1_date, b.day_date) / 7 AS integer) AS ga_week
            FROM bp_days b
            JOIN w1 w ON w.participantidentifier = b.participantidentifier
        ),
        weekly_counts AS (
            SELECT participantidentifier, ga_week AS week, COUNT(*) AS bp_days_count
            FROM bp_with_weeks
            WHERE ga_week BETWEEN {first_week} AND {last_week}
            GROUP BY 1, 2
        ),
        all_participants_weeks AS (
            SELECT
                w1.participantidentifier,
                CAST(week AS integer) AS week
            FROM w1
            CROSS JOIN UNNEST(sequence({first_week}, {last_week})) AS t(week)
            WHERE CAST(week AS integer) <= w1.current_ga_week
        )
        SELECT
            w.week,
            AVG(LEAST(CAST(COALESCE(wc.bp_days_count, 0) AS DOUBLE) / 2.0, 1.0) * 100) AS avg_pct,
            COUNT(DISTINCT w.participantidentifier) AS n_participants
        FROM all_participants_weeks w
        LEFT JOIN weekly_counts wc
            ON wc.participantidentifier = w.participantidentifier
            AND wc.week = w.week
        GROUP BY w.week
        ORDER BY w.week
        """
        return mdh_athena.execQuery(query)

    return (get_avg_bp,)


@app.cell
def _(fig_to_image, get_avg_bp, get_avg_daily_questions, get_avg_daily_symptoms, get_avg_ring_wear, get_avg_weekly_surveys, get_avg_weight, mo, np, pd, plt, sns):
    def build_average_heatmap(first_week, last_week, title, participant_ids_sql):
        """Build an averaged compliance heatmap for all participants across GA weeks."""
        symptoms = get_avg_daily_symptoms(first_week, last_week, participant_ids_sql)
        questions = get_avg_daily_questions(first_week, last_week, participant_ids_sql)
        surveys = get_avg_weekly_surveys(first_week, last_week, participant_ids_sql)
        ring_wear = get_avg_ring_wear(first_week, last_week, participant_ids_sql)
        weight = get_avg_weight(first_week, last_week, participant_ids_sql)
        bp = get_avg_bp(first_week, last_week, participant_ids_sql)

        weeks = [f"W{w}" for w in range(first_week, last_week + 1)]
        num_weeks = last_week - first_week + 1

        def extract_pct(result_df, num_weeks):
            """Extract avg_pct values, filling missing weeks with 0."""
            pcts = [0.0] * num_weeks
            if len(result_df) > 0:
                for _, row in result_df.iterrows():
                    week_idx = int(float(row['week'])) - first_week
                    if 0 <= week_idx < num_weeks:
                        pcts[week_idx] = float(row['avg_pct'])
            return pcts

        def extract_hours(result_df, num_weeks):
            """Extract avg_hours values, filling missing weeks with 0."""
            hours = [0.0] * num_weeks
            if len(result_df) > 0:
                for _, row in result_df.iterrows():
                    week_idx = int(float(row['week'])) - first_week
                    if 0 <= week_idx < num_weeks:
                        hours[week_idx] = float(row['avg_hours'])
            return hours

        frame = {
            "Symptom check-in (daily)": extract_pct(symptoms, num_weeks),
            "Daily questions (1-5 Q)": extract_pct(questions, num_weeks),
            "Weekly/bimonthly questionnaire": extract_pct(surveys, num_weeks),
            "Smart ring wear (~19h/day)": extract_pct(ring_wear, num_weeks),
            "Weight (per week)": extract_pct(weight, num_weeks),
            "BP (per week)": extract_pct(bp, num_weeks),
        }

        df = pd.DataFrame(frame, index=weeks).T

        # Calculate averages
        self_report_average = df.iloc[:3].mean(axis=0)
        biometrics_average = df.iloc[3:].mean(axis=0)
        df.loc["Self Report Average"] = self_report_average
        df.loc["Biometrics Average"] = biometrics_average

        # Ring wear hours data for separate heatmap
        def extract_hours(result_df, num_weeks):
            hours = [0.0] * num_weeks
            if len(result_df) > 0:
                for _, row in result_df.iterrows():
                    week_idx = int(float(row['week'])) - first_week
                    if 0 <= week_idx < num_weeks:
                        hours[week_idx] = float(row['avg_hours'])
            return hours

        ring_hours = extract_hours(ring_wear, num_weeks)
        df_hours = pd.DataFrame({"Smart ring wear (avg hrs/day)": ring_hours}, index=weeks).T
        annot_hours = df_hours.copy().astype(str)
        annot_hours.loc["Smart ring wear (avg hrs/day)"] = df_hours.loc["Smart ring wear (avg hrs/day)"].map(lambda v: f"{v:.1f}h")

        # Participant counts
        n_row = [0] * num_weeks
        if len(symptoms) > 0:
            for _, row in symptoms.iterrows():
                week_idx = int(float(row['week'])) - first_week
                if 0 <= week_idx < num_weeks:
                    n_row[week_idx] = int(row['n_participants'])

        # Add participant count as a row in the main heatmap
        df.loc["N Participants"] = n_row

        # Update annotations to include participant count
        annot_pct = df.copy().astype(str)
        for row_name in df.index:
            if row_name == "N Participants":
                annot_pct.loc[row_name] = df.loc[row_name].map(lambda v: f"{int(v)}")
            else:
                annot_pct.loc[row_name] = df.loc[row_name].map(lambda v: f"{v:.1f}%")

        # Plot with two subplots: percentage heatmap + hours heatmap
        n_pct_rows = len(df)
        fig, (ax_pct, ax_hours) = plt.subplots(
            2, 1,
            figsize=(max(11, num_weeks * 0.9), 6),
            gridspec_kw={'height_ratios': [n_pct_rows, 1], 'hspace': 0.15},
            sharex=True
        )

        # Main percentage heatmap
        sns.heatmap(
            df,
            vmin=0, vmax=100,
            cmap="YlGn",
            linewidths=0.5, linecolor="white",
            annot=annot_pct.values, fmt='',
            ax=ax_pct, cbar=False
        )
        ax_pct.set_title(title)
        ax_pct.set_xlabel("")

        # Bold separator before averages
        try:
            sep_y = list(df.index).index("Self Report Average")
            ax_pct.hlines(sep_y, *ax_pct.get_xlim(), colors="black", linewidth=2.8)
        except ValueError:
            pass

        # Bold separator before N Participants
        try:
            sep_y2 = list(df.index).index("N Participants")
            ax_pct.hlines(sep_y2, *ax_pct.get_xlim(), colors="black", linewidth=2.8)
        except ValueError:
            pass

        # Hours heatmap (ring wear) with Blues colormap
        sns.heatmap(
            df_hours,
            vmin=0, vmax=24,
            cmap="Blues",
            linewidths=0.5, linecolor="white",
            annot=annot_hours.values, fmt='',
            ax=ax_hours, cbar=False
        )
        ax_hours.set_ylabel("")
        ax_hours.set_yticklabels(ax_hours.get_yticklabels(), rotation=0)

        plt.tight_layout()
        return fig

    return (build_average_heatmap,)


@app.cell
def _(fig_to_image, mdh_athena, mo, np, participant_ids_sql, plt):
    def build_stage_distribution(first_week, last_week, title, participant_ids_sql):
        """Build a ring wear hours distribution histogram for a specific stage (GA week range), including 0h for days with no data."""
        query = f"""
        WITH edd AS (
            SELECT
                participantidentifier,
                date_parse(json_extract_scalar(cast(customfields AS JSON), '$.edd_final'), '%Y-%m-%d') AS edd_final
            FROM allparticipants
            WHERE participantidentifier IN ({participant_ids_sql})
                AND json_extract_scalar(cast(customfields AS JSON), '$.edd_final') IS NOT NULL
                AND json_extract_scalar(cast(customfields AS JSON), '$.edd_final') != ''
        ),
        w1 AS (
            SELECT
                participantidentifier,
                CAST(edd_final AS date) - INTERVAL '280' DAY AS w1_date
            FROM edd
            WHERE edd_final IS NOT NULL
        ),
        expected_days AS (
            SELECT
                w.participantidentifier,
                d AS day_date
            FROM w1 w
            CROSS JOIN UNNEST(
                SEQUENCE(
                    date_add('day', 7 * ({first_week} - 1), w.w1_date),
                    date_add('day', 7 * {last_week} - 1, w.w1_date),
                    INTERVAL '1' DAY
                )
            ) AS t(d)
            WHERE d <= CURRENT_DATE
        ),
        oura_days AS (
            SELECT
                participantidentifier,
                CAST("timestamp" AS date) AS day_date,
                GREATEST(0.0, LEAST(1.0, 1.0 - CAST(COALESCE(nonweartime, 0) AS DOUBLE) / 86400.0)) * 24.0 AS wear_hours
            FROM ouradailyactivity
            WHERE participantidentifier IN ({participant_ids_sql})
        )
        SELECT
            COALESCE(od.wear_hours, 0.0) AS wear_hours
        FROM expected_days ed
        LEFT JOIN oura_days od
            ON od.participantidentifier = ed.participantidentifier
            AND od.day_date = ed.day_date
        """
        result = mdh_athena.execQuery(query)

        if len(result) == 0:
            return None

        hours = result['wear_hours'].astype(float).values
        days_with_data = (hours > 0).sum()
        days_without_data = (hours == 0).sum()

        fig, ax = plt.subplots(figsize=(10, 3))
        ax.hist(hours, bins=48, range=(0, 24), color='steelblue', edgecolor='white', alpha=0.85)
        ax.axvline(x=18, color='red', linestyle='--', linewidth=1.5, label='Target: 18h')
        ax.axvline(x=np.median(hours), color='orange', linestyle='-', linewidth=1.5, label=f'Median: {np.median(hours):.1f}h')
        ax.axvline(x=np.mean(hours), color='green', linestyle='-.', linewidth=1.5, label=f'Mean: {np.mean(hours):.1f}h')
        ax.set_xlabel("Daily Wear (hours)")
        ax.set_ylabel("Days")
        ax.set_title(f"{title} ({len(hours):,} total days | {days_without_data:,} with 0h)")
        ax.set_xlim(0, 24)
        ax.legend(fontsize=8)
        plt.tight_layout()
        return fig

    return (build_stage_distribution,)


@app.cell
def _(build_average_heatmap, fig_to_image, mo):
    mo.md("## Stage 1: Prenatal Weeks 9-19")
    return


@app.cell
def _(build_average_heatmap, fig_to_image, participant_ids_sql):
    stage1_avg_fig = build_average_heatmap(9, 19, "Average Compliance (%) — Prenatal Weeks 9-19", participant_ids_sql)
    fig_to_image(stage1_avg_fig)
    return


@app.cell
def _(build_stage_distribution, fig_to_image, participant_ids_sql):
    _fig = build_stage_distribution(9, 19, "Ring Wear Distribution — W9-19", participant_ids_sql)
    fig_to_image(_fig)
    return


@app.cell
def _(build_average_heatmap, fig_to_image, mo):
    mo.md("## Stage 2: Prenatal Weeks 20-30")
    return


@app.cell
def _(build_average_heatmap, fig_to_image, participant_ids_sql):
    stage2_avg_fig = build_average_heatmap(20, 30, "Average Compliance (%) — Prenatal Weeks 20-30", participant_ids_sql)
    fig_to_image(stage2_avg_fig)
    return


@app.cell
def _(build_stage_distribution, fig_to_image, participant_ids_sql):
    _fig = build_stage_distribution(20, 30, "Ring Wear Distribution — W20-30", participant_ids_sql)
    fig_to_image(_fig)
    return


@app.cell
def _(build_average_heatmap, fig_to_image, mo):
    mo.md("## Stage 3: Prenatal Weeks 31-40")
    return


@app.cell
def _(build_average_heatmap, fig_to_image, participant_ids_sql):
    stage3_avg_fig = build_average_heatmap(31, 40, "Average Compliance (%) — Prenatal Weeks 31-40", participant_ids_sql)
    fig_to_image(stage3_avg_fig)
    return


@app.cell
def _(build_stage_distribution, fig_to_image, participant_ids_sql):
    _fig = build_stage_distribution(31, 40, "Ring Wear Distribution — W31-40", participant_ids_sql)
    fig_to_image(_fig)
    return


@app.cell
def _(fig_to_image, mdh_athena, mo, np, participant_ids_sql, plt):
    mo.md("## Ring Wear — Daily Hours Distribution (All Participants)")
    return


@app.cell
def _(fig_to_image, mdh_athena, mo, np, participant_ids_sql, plt):
    # Query all daily wear hours, including 0 for expected days from W9 to today
    ring_hist_query = f"""
    WITH edd AS (
        SELECT
            participantidentifier,
            date_parse(json_extract_scalar(cast(customfields AS JSON), '$.edd_final'), '%Y-%m-%d') AS edd_final
        FROM allparticipants
        WHERE participantidentifier IN ({participant_ids_sql})
            AND json_extract_scalar(cast(customfields AS JSON), '$.edd_final') IS NOT NULL
            AND json_extract_scalar(cast(customfields AS JSON), '$.edd_final') != ''
    ),
    w1 AS (
        SELECT
            participantidentifier,
            CAST(edd_final AS date) - INTERVAL '280' DAY AS w1_date
        FROM edd
        WHERE edd_final IS NOT NULL
    ),
    expected_days AS (
        SELECT
            w.participantidentifier,
            d AS day_date
        FROM w1 w
        CROSS JOIN UNNEST(
            SEQUENCE(
                date_add('day', 7 * (9 - 1), w.w1_date),
                date_add('day', 7 * 40 - 1, w.w1_date),
                INTERVAL '1' DAY
            )
        ) AS t(d)
        WHERE d <= CURRENT_DATE
    ),
    oura_days AS (
        SELECT
            participantidentifier,
            CAST("timestamp" AS date) AS day_date,
            GREATEST(0.0, LEAST(1.0, 1.0 - CAST(COALESCE(nonweartime, 0) AS DOUBLE) / 86400.0)) * 24.0 AS wear_hours
        FROM ouradailyactivity
        WHERE participantidentifier IN ({participant_ids_sql})
    )
    SELECT
        COALESCE(od.wear_hours, 0.0) AS wear_hours
    FROM expected_days ed
    LEFT JOIN oura_days od
        ON od.participantidentifier = ed.participantidentifier
        AND od.day_date = ed.day_date
    """
    ring_hist_data = mdh_athena.execQuery(ring_hist_query)

    _hist_output = mo.md("*No ring wear data available.*")
    if len(ring_hist_data) > 0:
        hours = ring_hist_data['wear_hours'].astype(float).values
        days_with_data = (hours > 0).sum()
        days_without_data = (hours == 0).sum()

        fig, ax = plt.subplots(figsize=(10, 4))
        ax.hist(hours, bins=48, range=(0, 24), color='steelblue', edgecolor='white', alpha=0.85)
        ax.axvline(x=18, color='red', linestyle='--', linewidth=1.5, label='Target: 18h')
        ax.axvline(x=np.median(hours), color='orange', linestyle='-', linewidth=1.5, label=f'Median: {np.median(hours):.1f}h')
        ax.axvline(x=np.mean(hours), color='green', linestyle='-.', linewidth=1.5, label=f'Mean: {np.mean(hours):.1f}h')
        ax.set_xlabel("Daily Wear (hours)")
        ax.set_ylabel("Number of Days")
        ax.set_title(f"Ring Wear Hours Distribution — All Stages W9-40 ({len(hours):,} total days | {days_without_data:,} with 0h)")
        ax.set_xlim(0, 24)
        ax.legend()
        plt.tight_layout()
        _hist_output = fig_to_image(fig)
    _hist_output
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()

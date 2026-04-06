import marimo

__generated_with = "0.15.2"
app = marimo.App(width="medium")


@app.cell
def _():

    import marimo as mo
    import io
    import base64

    def fig_to_image(fig):
        """Convert a matplotlib figure to a static mo.image for reliable rendering."""
        if fig is None:
            return mo.md("")
        buf = io.BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight', dpi=150)
        buf.seek(0)
        return mo.image(buf.read(), width=1100)

    return fig_to_image, mo


@app.cell
def _():
    from sensorfabric.mdh import MDH
    import pandas as pd
    import os
    return MDH, os, pd


@app.cell
def _(MDH):
    mdh = MDH()
    return (mdh,)


@app.cell
def _(mdh, os):
    segmentID = os.getenv('MDH_SEGMENT_ID') # segment ID of enrolled participants
    all_participants_data = mdh.getAllParticipants({'segmentID': segmentID})
    return (all_participants_data,)


@app.cell
def _():
    # all_participants_table = mo.ui.table(data=all_participants_data['participants'], pagination=True)
    return


@app.cell
def _():
    # mo.vstack([all_participants_table, all_participants_table.value])
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    # Heatmap Explanation

    ## Stages

    We show participation stages dynamically based on delivery status:

    - Prenatal Weeks 9 - 19
    - Prenatal Weeks 20 - 30
    - Prenatal Weeks 31 - 40 (or up to delivery week if premature)
    - If delivery_date exists and delivery was after week 40: extended Prenatal Weeks 41+ through delivery week, then Postpartum Weeks 1-6 from delivery date
    - If delivery_date exists and delivery was at or before week 40: Postpartum Weeks 1-6 from delivery date
    - If no delivery_date: Prenatal continues past week 40 up to the current week (no postpartum shown)

    **Prenatal W1 Calculation:**
    - W1 always starts at EDD - 280 days (40 weeks before estimated due date)

    Each stage includes two heatmaps:

    - Counts heatmap - the number of completed days per week.
    - Percentage heatmap - weekly completion percentages, plus Self-Report Average, Biometrics Average, and the weekly compensation calculation.

    ## Activities shown in the heatmaps

    - Daily symptom check-ins - up to 7 per week.
    - Daily 6 questions - up to 7 per week.
    - Weekly/bimonthly questionnaire - up to 1 per week.
    - Ultrahuman smart ring wear - a day counts as 1 if daily wear ≥ 75%; up to 7 per week.
    - Oura smart ring wear - a day counts as 1 if daily wear ≥ 75%; up to 7 per week.
    - Twice-weekly weight measurements - up to 2 per week.
    - Twice-weekly blood pressure measurements - up to 2 per week.

    *The counts heatmap shows these raw weekly counts.*
    *The percentage heatmap converts each row to a % of the weekly maximum (e.g., 5/7 days == ~71.4%).*

    ## Averages in the percentage heatmap

    Self-Report Average includes:

    - Daily symptom check-ins
    - Daily 6 questions
    - Weekly/bimonthly questionnaire

    Biometrics Average includes:

    - Smart ring wear (participants wear either Ultrahuman or Oura)
    - Twice-weekly weight measurements
    - Twice-weekly blood pressure measurements

    Weekly compensation logic

    - $3 for Self-Report activities if the Self-Report Average ≥ 70%
    - $4 for Biometric activities if the Biometrics Average ≥ 70%
    - Weekly total = sum of the two amounts (i.e., $0, $3, $4, or $7).
    """
    )
    return


@app.cell
def _(all_participants_data, mo, pd):
    dropdown = mo.ui.dropdown(
        options=pd.DataFrame(all_participants_data['participants'])['participantIdentifier'], label="Choose a Participant ID", value="BB-4053-1232", searchable=True
    )
    dropdown
    return (dropdown,)


@app.cell
def _(dropdown, mdh):
    participantidentifier = dropdown.value
    participant = mdh.getParticipant(participantidentifier)
    # participant_table = mo.ui.table(data=participant, pagination=False)
    # participant_table
    return participant, participantidentifier


@app.cell
def _(participant):
    participant_email = (participant['customFields']['uh_email'] or participant['accountEmail'])
    ring_vendor = participant['customFields']['ring_vendor']
    return participant_email, ring_vendor


@app.cell
def _(
    first_w1_day,
    get_current_gestational_week,
    get_delivery_week,
    get_participant_delivery_info,
    mo,
    participantidentifier,
):
    edd_final, delivery_date, postpartum_days = get_participant_delivery_info(participantidentifier)

    if delivery_date:
        delivery_week = get_delivery_week(first_w1_day, delivery_date)
        # Stage 3 ends at delivery week or 40, whichever is smaller
        stage3_last_week = min(40, delivery_week)
        # If delivery is after week 40, we need an extra prenatal stage for W41 through delivery week
        stage3_extended_last_week = delivery_week if delivery_week > 40 else None
        has_postpartum = True

        mo.md(f"""
        **Delivery Information:**
        - EDD: {edd_final.strftime('%Y-%m-%d') if edd_final else 'Not available'}
        - Actual Delivery Date: {delivery_date.strftime('%Y-%m-%d')} (gestational week {delivery_week})
        - Postpartum: 6 weeks from delivery date
        """)
    else:
        current_week = get_current_gestational_week(first_w1_day)
        stage3_last_week = 40
        # No delivery — prenatal continues past W40 up to current week
        stage3_extended_last_week = current_week if current_week > 40 else None
        has_postpartum = False
        delivery_week = None

        mo.md(f"""
        **Delivery Information:**
        - EDD: {edd_final.strftime('%Y-%m-%d') if edd_final else 'Not available'}
        - Actual Delivery Date: Not available
        - Currently at gestational week {current_week}
        - Prenatal tracking continues past W40 until delivery date is recorded
        """)
    return (
        delivery_date,
        has_postpartum,
        postpartum_days,
        stage3_extended_last_week,
        stage3_last_week,
    )


@app.cell
def _(delivery_date, mo, ring_vendor):
    delivery_info = f" | Delivery Date: {delivery_date.strftime('%Y-%m-%d')}" if delivery_date else ""
    mo.md(f"""**Ring Vendor:** {ring_vendor}{delivery_info}""")
    return


@app.cell
def _(participantidentifier):
    from stage_calculation import show_heatmap_for_stage, participant_first_w1_day, get_participant_delivery_info, get_delivery_week, get_current_gestational_week
    first_w1_day = participant_first_w1_day(participantidentifier)
    return (
        first_w1_day,
        get_current_gestational_week,
        get_delivery_week,
        get_participant_delivery_info,
        show_heatmap_for_stage,
    )


@app.cell
def _(
    first_w1_day,
    participant_email,
    participantidentifier,
    ring_vendor,
    show_heatmap_for_stage,
):
    stage1_fig_1, stage1_fig_2 = show_heatmap_for_stage(participant_email, participantidentifier, 9, 19, "Prenatal Weeks 9-19 — Weekly Compliance Heatmap", first_w1_day, ring_vendor)
    return stage1_fig_1, stage1_fig_2


@app.cell
def _(fig_to_image, stage1_fig_1):
    fig_to_image(stage1_fig_1)
    return


@app.cell
def _(fig_to_image, stage1_fig_2):
    fig_to_image(stage1_fig_2)
    return


@app.cell
def _(
    first_w1_day,
    participant_email,
    participantidentifier,
    ring_vendor,
    show_heatmap_for_stage,
):
    stage2_fig_1, stage2_fig_2 = show_heatmap_for_stage(participant_email, participantidentifier, 20, 30, "Prenatal Weeks 20-30 — Weekly Compliance Heatmap", first_w1_day, ring_vendor)
    return stage2_fig_1, stage2_fig_2


@app.cell
def _(fig_to_image, stage2_fig_1):
    fig_to_image(stage2_fig_1)
    return


@app.cell
def _(fig_to_image, stage2_fig_2):
    fig_to_image(stage2_fig_2)
    return


@app.cell
def _(
    first_w1_day,
    participant_email,
    participantidentifier,
    ring_vendor,
    show_heatmap_for_stage,
    stage3_last_week,
):
    stage3_fig_1, stage3_fig_2 = show_heatmap_for_stage(participant_email, participantidentifier, 31, stage3_last_week, f"Prenatal Weeks 31-{stage3_last_week} — Weekly Compliance Heatmap", first_w1_day, ring_vendor)
    return stage3_fig_1, stage3_fig_2


@app.cell
def _(fig_to_image, stage3_fig_1):
    fig_to_image(stage3_fig_1)
    return


@app.cell
def _(fig_to_image, stage3_fig_2):
    fig_to_image(stage3_fig_2)
    return


@app.cell
def _(
    delivery_date,
    first_w1_day,
    has_postpartum,
    participant_email,
    participantidentifier,
    postpartum_days,
    ring_vendor,
    show_heatmap_for_stage,
    stage3_extended_last_week,
):
    stage4_fig_1 = None
    stage4_fig_2 = None
    stage4_ext_fig_1 = None
    stage4_ext_fig_2 = None

    if stage3_extended_last_week and stage3_extended_last_week >= 41:
        # Show extended prenatal weeks (W41 through delivery week or current week)
        stage4_ext_fig_1, stage4_ext_fig_2 = show_heatmap_for_stage(
            participant_email, participantidentifier, 41, stage3_extended_last_week,
            f"Prenatal Weeks 41-{stage3_extended_last_week} — Weekly Compliance Heatmap",
            first_w1_day, ring_vendor
        )

    if has_postpartum and delivery_date:
        # Show postpartum 6 weeks from delivery_date
        stage4_fig_1, stage4_fig_2 = show_heatmap_for_stage(
            participant_email, participantidentifier, 1, 6,
            "Postpartum Weeks 1-6 — Weekly Compliance Heatmap",
            first_w1_day, ring_vendor, is_postpartum=True,
            delivery_date=delivery_date, postpartum_days=postpartum_days
        )
    return stage4_ext_fig_1, stage4_ext_fig_2, stage4_fig_1, stage4_fig_2


@app.cell
def _(fig_to_image, stage4_ext_fig_1):
    fig_to_image(stage4_ext_fig_1)
    return


@app.cell
def _(fig_to_image, stage4_ext_fig_2):
    fig_to_image(stage4_ext_fig_2)
    return


@app.cell
def _(fig_to_image, stage4_fig_1):
    fig_to_image(stage4_fig_1)
    return


@app.cell
def _(fig_to_image, stage4_fig_2):
    fig_to_image(stage4_fig_2)
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()

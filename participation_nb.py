import marimo

__generated_with = "0.15.2"
app = marimo.App(width="medium")


@app.cell
def _():

    import marimo as mo
    return (mo,)


@app.cell
def _():
    from sensorfabric.mdh import MDH
    import pandas as pd
    import os
    return MDH, pd


@app.cell
def _(MDH):
    mdh = MDH()
    return (mdh,)


@app.cell
def _(mdh):
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

    We show four stages of participation:

    - Prenatal Weeks 9 - 19
    - Prenatal Weeks 20 - 30
    - Prenatal Weeks 31 - 40
    - Postpartum Weeks 1 - 6 (corresponds to gestational weeks 41 - 46)

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
def _(mo, ring_vendor):
    mo.md(f"""Ring Vendor: {ring_vendor}""")
    return


@app.cell
def _(participantidentifier):
    from stage_calculation import show_heatmap_for_stage, participant_first_w1_day
    first_w1_day = participant_first_w1_day(participantidentifier)
    return first_w1_day, show_heatmap_for_stage


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
def _(mo, stage1_fig_1):
    mo.mpl.interactive(stage1_fig_1)
    return


@app.cell
def _(mo, stage1_fig_2):
    mo.mpl.interactive(stage1_fig_2)
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
def _(mo, stage2_fig_1):
    mo.mpl.interactive(stage2_fig_1)
    return


@app.cell
def _(mo, stage2_fig_2):
    mo.mpl.interactive(stage2_fig_2)
    return


@app.cell
def _(
    first_w1_day,
    participant_email,
    participantidentifier,
    ring_vendor,
    show_heatmap_for_stage,
):
    stage3_fig_1, stage3_fig_2 = show_heatmap_for_stage(participant_email, participantidentifier, 31, 40, "Prenatal Weeks 31-40 — Weekly Compliance Heatmap", first_w1_day, ring_vendor)
    return stage3_fig_1, stage3_fig_2


@app.cell
def _(mo, stage3_fig_1):
    mo.mpl.interactive(stage3_fig_1)
    return


@app.cell
def _(mo, stage3_fig_2):
    mo.mpl.interactive(stage3_fig_2)
    return


@app.cell
def _(
    first_w1_day,
    participant_email,
    participantidentifier,
    ring_vendor,
    show_heatmap_for_stage,
):
    stage4_fig_1, stage4_fig_2 = show_heatmap_for_stage(participant_email, participantidentifier, 41, 46, "Postpartum Weeks 1-6 — Weekly Compliance Heatmap", first_w1_day, ring_vendor)
    return stage4_fig_1, stage4_fig_2


@app.cell
def _(mo, stage4_fig_1):
    mo.mpl.interactive(stage4_fig_1)
    return


@app.cell
def _(mo, stage4_fig_2):
    mo.mpl.interactive(stage4_fig_2)
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


@app.cell
def _():
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()

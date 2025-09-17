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
def _(all_participants_data, mo, pd):
    dropdown = mo.ui.dropdown(
        options=pd.DataFrame(all_participants_data['participants'])['participantIdentifier'], label="Choose a Participant ID", value="BB-4053-1232"
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
    return (participant_email,)


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
    show_heatmap_for_stage,
):
    stage1_fig_1, stage1_fig_2 = show_heatmap_for_stage(participant_email, participantidentifier, 9, 19, "Prenatal Weeks 9-19 — Weekly Compliance Heatmap", first_w1_day)
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
    show_heatmap_for_stage,
):
    stage2_fig_1, stage2_fig_2 = show_heatmap_for_stage(participant_email, participantidentifier, 20, 30, "Prenatal Weeks 20-30 — Weekly Compliance Heatmap", first_w1_day)
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
    show_heatmap_for_stage,
):
    stage3_fig_1, stage3_fig_2 = show_heatmap_for_stage(participant_email, participantidentifier, 31, 40, "Prenatal Weeks 31-40 — Weekly Compliance Heatmap", first_w1_day)
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
    show_heatmap_for_stage,
):
    stage4_fig_1, stage4_fig_2 = show_heatmap_for_stage(participant_email, participantidentifier, 41, 46, "Postpartum Weeks 1-6 — Weekly Compliance Heatmap", first_w1_day)
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


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()

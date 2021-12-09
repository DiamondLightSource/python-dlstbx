import pathlib

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from relion.zocalo.alchemy import ClusterJobInfo, RelionJobInfo, RelionPipelineInfo
from sqlalchemy.orm import Query
from sqlalchemy.orm.session import Session, sessionmaker

from dlstbx.cli.em_usage_collect import _get_sessionmaker

columns = [
    [
        c
        for c in RelionJobInfo.__table__.columns.keys()
        if c not in ("job_id", "pipeline_id")
    ],
    [
        c
        for c in RelionPipelineInfo.__table__.columns.keys()
        # if c not in ("pipeline_id")
    ],
    [
        c
        for c in ClusterJobInfo.__table__.columns.keys()
        if c not in ("cluster", "cluster_id", "auto_proc_program_id")
    ],
]


def _serialise_query(q: Query):
    return q.statement.compile(compile_kwargs={"literal_binds": True}).string


@st.cache(
    hash_funcs={Session: lambda _: None, Query: _serialise_query},
    suppress_st_warning=True,
    allow_output_mutation=True,
)
def _run_query(q: Query, sess: Session):
    st.warning("CACHE MISS")
    print("CACHE MISS")
    return pd.read_sql(q.statement, sess.bind)


def _collect_from_db() -> list:
    _sessionmaker: sessionmaker = _get_sessionmaker()
    with _sessionmaker() as session:
        query = (
            session.query(RelionJobInfo, RelionPipelineInfo, ClusterJobInfo)
            .join(ClusterJobInfo, ClusterJobInfo.cluster_id == RelionJobInfo.cluster_id)
            .join(
                RelionPipelineInfo,
                RelionPipelineInfo.pipeline_id == RelionJobInfo.pipeline_id,
            )
        )
        return _run_query(query, session)


# @st.cache(allow_output_mutation=True)
def _collect_all_data() -> pd.DataFrame:
    df = _collect_from_db()
    df["job_time"] = (df["end_time"] - df["relion_start_time"]).dt.total_seconds()
    df["run_time"] = (df["end_time"] - df["start_time"]).dt.total_seconds()
    df["queue_time"] = df["job_time"] - df["run_time"]
    df["pipeline_id"] = df["pipeline_id"].astype(str)
    preproc = (
        "MotionCorr",
        "CtfFind",
        "Icebreaker_F",
        "Icebreaker_G",
        "crYOLO_AutoPick",
        "AutoPick",
        "Extract",
    )

    def schedule(row):
        if row["job_name"] in preproc:
            return "Preprocessing"
        else:
            return row["job_name"]

    df["schedule"] = df.apply(lambda row: schedule(row), axis=1)
    return df


gpu_jobs = (
    "MotionCorr",
    "CtfFind",
    "Class2D",
    "Class3D",
    "crYOLO_AutoPick",
    "AutoPick",
)
cpu_jobs = ("Icebreaker_F", "Icebreaker_G", "Icebreaker_group")


def _format_x_select(label: str) -> str:
    xmap = {
        "pipeline_id": "Pipeline instances",
        "job_name": "Job type",
        "num_micrographs": "Number of micrographs processed",
    }
    return xmap.get(label)


def _format_y_select(label: str) -> str:
    ymap = {
        "job_time": "Total job time [s]",
        "run_time": "Job run time [s]",
        "queue_time": "Job cluster queue time [s]",
        "count": "Total number of jobs run",
    }
    return ymap.get(label)


def _format_proj_path_label(label: str) -> str:
    if label == "All":
        return label
    proj_path_parts = pathlib.Path(label).parts
    return f"{proj_path_parts[5]}: {proj_path_parts[-3]} [{proj_path_parts[-2]}]"


def _format_hover_select(label: str) -> str:
    hvmap = {
        "cluster_id": "Cluster ID",
        "job_name": "Job type",
        "project_path": "Path to Relion project",
    }
    return hvmap.get(label)


def _format_colour_select(label: str) -> str:
    cmap = {None: "Nothing", "schedule": "Schedule"}
    return cmap.get(label)


def run() -> None:
    if "df" not in st.session_state:
        st.warning("state is fresh")
        st.session_state["df"] = _collect_all_data()
    df = st.session_state["df"]
    allmics = st.sidebar.checkbox("All microscopes", value=True)
    if not allmics:
        mics = ["m02", "m04", "m05", "m06", "m07", "m08", "m10", "m11", "m12"]
        mic_checks = [st.sidebar.checkbox(m) for m in mics]
        selected_mics = [m for m, c in zip(mics, mic_checks) if c]
        df = df[df["microscope"].isin(selected_mics)]
    if allmics:
        job_usage_pie = px.pie(df, values="run_time", names="job_name")
        st.plotly_chart(job_usage_pie)
    xs = ["pipeline_id", "job_name", "num_micrographs"]
    ys = ["job_time", "run_time", "queue_time", "count"]
    hvd = ["cluster_id", "job_name", "project_path"]

    x = st.selectbox("x-axis", xs, format_func=_format_x_select)
    y = st.selectbox("y-axis", ys, format_func=_format_y_select)
    hover_data = st.multiselect("Hover data", hvd, format_func=_format_hover_select)
    colour_by = st.selectbox(
        "Colour by", [None, "schedule"], format_func=_format_colour_select
    )

    if not allmics:
        proj_path = st.selectbox(
            "Project path",
            ["All"] + list(df["project_path"].unique()),
            format_func=_format_proj_path_label,
        )
        if proj_path != "All":
            df = df[df["project_path"] == proj_path]

    if y != "count":
        fig = px.bar(
            df,
            x=x,
            y=y,
            labels={
                x: _format_x_select(x),
                y: _format_y_select(y),
                **{h: _format_hover_select(h) for h in hover_data},
            },
            hover_data=hover_data,
            color=colour_by,
        )
        st.plotly_chart(fig)
    else:
        x_vals = list(df[x].unique())
        y_vals = [len(df[df[x] == pid]) for pid in x_vals]
        count_fig = go.Figure([go.Bar(x=x_vals, y=y_vals)])
        count_fig.update_layout(
            xaxis_title=_format_x_select(x), yaxis_title=_format_y_select(y)
        )
        st.plotly_chart(count_fig)

    with st.expander("See aggregated time usage"):
        total_num_pipelines = len(df["pipeline_id"].unique())
        total_num_jobs = len(df.index)
        total_run_time = df["run_time"].sum()
        total_gpu_time = df[df["job_name"].isin(gpu_jobs)]["run_time"].sum() * 4
        wasted_gpu_time = (
            df[(df["job_name"].isin(gpu_jobs)) & (df["num_micrographs"] == 0)][
                "run_time"
            ].sum()
            * 4
        )
        wasted_cpu_time = df[
            (df["job_name"].isin(cpu_jobs)) & (df["num_micrographs"] == 0)
        ]["run_time"].sum()
        total_cpu_time = df[df["job_name"].isin(cpu_jobs)]["run_time"].sum()
        st.write(f"Total number of jobs: {total_num_jobs}")
        st.write(f"Total number of pipelines: {total_num_pipelines}")
        st.write(f"Cluster time usage (human hours): {total_run_time/3600:.1f}")
        st.write(f"GPU usage (GPU hours): {total_gpu_time/3600:.1f}")
        st.write(f"Wasted GPU usage (GPU hours): {wasted_gpu_time/3600:.1f}")
        st.write(f"CPU usage (node hours): {total_cpu_time/3600:.1f}")
        st.write(f"Wasted CPU usage (node hours): {wasted_cpu_time/3600:.1f}")
        st.write(
            f"Number of micrographs motion corrected: {int(df[df['job_name'] == 'MotionCorr']['num_micrographs'].sum())}"
        )
        useful_jobs_pie = go.Figure(
            data=[
                go.Pie(
                    values=[total_gpu_time, wasted_gpu_time],
                    labels=["Useful GPU time", "Wasted GPU time"],
                )
            ]
        )
        st.plotly_chart(useful_jobs_pie, use_container_width=True)
        useful_cpu_jobs_pie = go.Figure(
            data=[
                go.Pie(
                    values=[total_cpu_time, wasted_cpu_time],
                    labels=["Useful CPU time", "Wasted CPU time"],
                )
            ]
        )
        st.plotly_chart(useful_cpu_jobs_pie, use_container_width=True)


if __name__ == "__main__":
    run()

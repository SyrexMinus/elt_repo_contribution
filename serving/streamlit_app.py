import json
import re
from pathlib import Path

import plotly.graph_objects as go
import streamlit as st
from snowflake.snowpark.context import get_active_session


def main():
    st.set_page_config(page_title="Repository contribution", layout="wide")
    st.title("Repository contribution")
    st.button("Refresh")
    session = get_active_session()
    queries = [
        Path(__file__).with_name("queries") / filename
        for filename in (
            "pr_count_per_directory.sql",
            "approved_review_count_by_author_and_reviewer.sql",
            "author_pr_and_diff_share_per_directory.sql",
            "time_to_ship_percentiles_per_author.sql",
        )
    ]
    defaults = _parameters(session, queries[0].read_text().split("*/", 1)[1])
    repositories = [
        row["REPO"]
        for row in session.sql(
            "SELECT DISTINCT REPO FROM SILVER.PULL ORDER BY REPO"
        ).collect()
    ]
    repository, merged_from, merged_until = st.columns([2, 1, 1])
    parameters = {
        "repo": repository.selectbox(
            "Repository",
            repositories,
            index=repositories.index(defaults["REPO"])
            if defaults["REPO"] in repositories
            else 0,
            key="repo",
        ),
        "merged_from": merged_from.date_input(
            "merged_from (UTC, inclusive)",
            value=defaults["MERGED_FROM"].date(),
            format="YYYY-MM-DD",
            key="merged_from",
        ),
        "merged_until": merged_until.date_input(
            "merged_until (UTC, inclusive)",
            value=defaults["MERGED_UNTIL"].date(),
            format="YYYY-MM-DD",
            key="merged_until",
        ),
    }
    for query in queries:
        source = query.read_text()
        header, sql = source.split("*/", 1)
        chart = json.loads(header[len("/* chart") :])
        st.subheader(chart["title"])
        if "min_diff" in sql:
            defaults = _parameters(session, sql)
            min_diff, max_diff = st.columns(2)
            parameters["min_diff"] = min_diff.number_input(
                "min_diff (inclusive)",
                min_value=0,
                value=int(defaults["MIN_DIFF"]),
                step=1,
                help="Added and deleted lines in the entire PR.",
                key="min_diff",
            )
            parameters["max_diff"] = max_diff.number_input(
                "max_diff (inclusive)",
                min_value=0,
                value=(
                    int(defaults["MAX_DIFF"])
                    if defaults["MAX_DIFF"] is not None
                    else None
                ),
                step=1,
                placeholder="No upper limit",
                help="Added and deleted lines in the entire PR; leave empty for no limit.",
                key="max_diff",
            )
        rows = _query_rows(session, sql, parameters)
        rows.columns = rows.columns.str.lower()
        if rows.empty:
            st.info("No rows for the query parameters.")
            continue
        if "filter" in chart:
            field = chart["filter"]["field"]
            directories = sorted(rows[field].unique())
            selected = st.selectbox(
                chart["filter"]["label"],
                directories,
                index=directories.index(chart["filter"]["default"]),
                format_func=lambda value, labels=chart["filter"]["labels"]: labels.get(
                    value, value
                ),
                key=query.stem,
            )
            rows = rows[rows[field] == selected]
        if chart.get("type") == "treemap":
            st.plotly_chart(_treemap(rows), width="stretch", key=query.stem)
        elif chart.get("type") == "sankey":
            st.plotly_chart(_sankey(rows), width="stretch", key=query.stem)
        else:
            st.vega_lite_chart(rows, chart["spec"], width="stretch")
        with st.expander("Data"):
            st.dataframe(rows, hide_index=True, width="stretch")


def _treemap(rows):
    sibling_counts = rows.groupby("parent_dir")["pr_count"].sum().to_dict()
    tile_values = {"/": 1.0}
    for row in sorted(rows.itertuples(index=False), key=lambda row: row.dir.count("/")):
        if row.dir != "/":
            tile_values[row.dir] = (
                tile_values[row.parent_dir]
                * row.pr_count
                / sibling_counts[row.parent_dir]
            )
    return go.Figure(
        go.Treemap(
            ids=rows["dir"].tolist(),
            labels=[
                "All PRs"
                if path == "/"
                else "Root files"
                if path == "."
                else path.rsplit("/", 1)[-1]
                for path in rows["dir"]
            ],
            parents=rows["parent_dir"].tolist(),
            values=[tile_values[path] for path in rows["dir"]],
            branchvalues="total",
            customdata=rows[
                ["pr_count", "parent_pr_count", "pr_share"]
            ].values.tolist(),
            texttemplate="%{label}<br>%{customdata[0]} PRs",
            hovertemplate=(
                "%{id}<br>PRs: %{customdata[0]}<br>Parent PRs: %{customdata[1]}"
                "<br>Share of parent PRs: %{customdata[2]:.1%}<extra></extra>"
            ),
        ),
        layout={"height": 600, "margin": {"t": 25, "r": 10, "b": 10, "l": 10}},
    )


def _sankey(rows):
    interaction_counts = [
        rows.groupby(field)["approved_pr_count"]
        .sum()
        .sort_index(key=lambda logins: logins.str.lower())
        .sort_values(ascending=False, kind="stable")
        for field in ("author", "reviewer")
    ]
    authors, reviewers = [counts.index.tolist() for counts in interaction_counts]
    node_count = max(len(authors), len(reviewers))
    plot_height = max(535, 40 * node_count)
    node_y = []
    for node_interactions in interaction_counts:
        node_centres = (
            (node_interactions.cumsum() - node_interactions / 2)
            / node_interactions.sum()
            * (plot_height - 20 * (node_count - 1))
        )
        node_y.extend(
            (centre + 20 * index + 10 * (node_count - len(node_interactions)))
            / plot_height
            for index, centre in enumerate(node_centres)
        )
    return go.Figure(
        go.Sankey(
            arrangement="fixed",
            textfont={"color": "black"},
            node={
                "label": authors + reviewers,
                "customdata": ["Author"] * len(authors) + ["Reviewer"] * len(reviewers),
                "color": ["#2563eb"] * len(authors) + ["#0d9488"] * len(reviewers),
                "x": [0.01] * len(authors) + [0.99] * len(reviewers),
                "y": node_y,
                "pad": 20,
                "line": {"width": 0},
                "hovertemplate": "%{customdata}: %{label}<extra></extra>",
            },
            link={
                "source": rows["author"]
                .map({name: index for index, name in enumerate(authors)})
                .tolist(),
                "target": rows["reviewer"]
                .map(
                    {name: len(authors) + index for index, name in enumerate(reviewers)}
                )
                .tolist(),
                "value": rows["approved_pr_count"].tolist(),
                "hovertemplate": "%{source.label} → %{target.label}<br>Approved PRs: %{value}<extra></extra>",
            },
        ),
        layout={
            "height": plot_height + 65,
            "margin": {"t": 55, "r": 10, "b": 10, "l": 10},
            "annotations": [
                {
                    "text": role,
                    "x": x,
                    "y": 1,
                    "xref": "paper",
                    "yref": "paper",
                    "xanchor": anchor,
                    "yanchor": "bottom",
                    "yshift": 20,
                    "showarrow": False,
                    "font": {"color": "black", "size": 16},
                }
                for role, x, anchor in (
                    ("author", 0, "left"),
                    ("reviewer", 1, "right"),
                )
            ],
        },
    )


def _parameters(session, sql):
    return session.sql(
        re.search(r"(?s)\bwith parameters as \((.*?)\n\)", sql)[1]
    ).collect()[0]


def _query_rows(session, sql, parameters):
    bindings = []

    def bind(match):
        name = match[2]
        sql_type = (
            "timestamp_ntz"
            if name.startswith("merged_")
            else "varchar"
            if name == "repo"
            else "number"
        )
        if parameters[name] is None:
            return f"{match[1]}null::{sql_type} as {name}{match[3]}"
        bindings.append(parameters[name])
        return f"{match[1]}?::{sql_type} as {name}{match[3]}"

    sql = re.sub(
        r"(?m)^([ \t]*)[^\n]+ as (repo|merged_from|merged_until|min_diff|max_diff)(,?)$",
        bind,
        sql,
    )
    return session.sql(sql, params=bindings).to_pandas()


if __name__ == "__main__":
    main()

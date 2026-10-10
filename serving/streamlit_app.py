import json
from pathlib import Path

import streamlit as st
from snowflake.snowpark.context import get_active_session


def main():
    st.set_page_config(page_title="Repository contribution", layout="wide")
    st.title("Repository contribution")
    st.button("Refresh")
    session = get_active_session()
    for query in sorted(Path(__file__).with_name("queries").glob("*.sql")):
        source = query.read_text()
        header, sql = source.split("*/", 1)
        chart = json.loads(header[len("/* chart") :])
        rows = session.sql(sql).to_pandas()
        rows.columns = rows.columns.str.lower()
        st.subheader(chart["title"])
        if rows.empty:
            st.info("No rows for the query parameters.")
            continue
        if "filter" in chart:
            field = chart["filter"]["field"]
            selected = st.selectbox(
                chart["filter"]["label"], sorted(rows[field].unique()), key=query.stem
            )
            rows = rows[rows[field] == selected]
        st.vega_lite_chart(rows, chart["spec"], width="stretch")
        with st.expander("Data"):
            st.dataframe(rows, hide_index=True, width="stretch")


if __name__ == "__main__":
    main()

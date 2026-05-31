import streamlit as st
import plotly.express as px


def render_chart():
    """
    Render chart section when chart mode is enabled.
    """

    if not st.session_state.get("show_chart"):
        return

    df = st.session_state.get("result_df")

    if df is None or df.empty:
        return

    st.subheader("📈 Chart Generator")

    numeric_cols = (
        df.select_dtypes(include=["number"])
        .columns
        .tolist()
    )

    if not numeric_cols:
        st.warning(
            "No numeric columns available for charting."
        )
        return

    chart_type = st.selectbox(
        "Chart Type",
        [
            "Bar",
            "Line",
            "Pie",
            "Scatter",
            "Area",
            "Box",
        ],
    )

    color_col = st.selectbox(
        "Color By",
        ["None"]
        + df.select_dtypes(
            include=["object"]
        ).columns.tolist(),
    )

    x_col = st.selectbox(
        "X-axis",
        df.columns.tolist(),
    )

    y_col = st.selectbox(
        "Y-axis",
        numeric_cols,
    )

    try:

        color_arg = (
            color_col
            if color_col != "None"
            else None
        )

        if chart_type == "Bar":
            fig = px.bar(
                df,
                x=x_col,
                y=y_col,
                color=color_arg,
            )

        elif chart_type == "Line":
            fig = px.line(
                df,
                x=x_col,
                y=y_col,
                color=color_arg,
            )

        elif chart_type == "Pie":
            fig = px.pie(
                df,
                names=x_col,
                values=y_col,
            )

        elif chart_type == "Scatter":
            fig = px.scatter(
                df,
                x=x_col,
                y=y_col,
                color=color_arg,
            )

        elif chart_type == "Area":
            fig = px.area(
                df,
                x=x_col,
                y=y_col,
                color=color_arg,
            )

        elif chart_type == "Box":
            fig = px.box(
                df,
                x=x_col,
                y=y_col,
                color=color_arg,
            )

        fig.update_layout(
            template="plotly_dark"
        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )

    except Exception as e:

        st.error(
            f"Chart error: {e}"
        )

import streamlit as st


def show_left_aligned_table(
    df,
    rows_before_scroll: int = 10
):
    """
    Display dataframe with:
    - left aligned columns
    - sticky header
    - horizontal scrolling
    - dark theme styling
    """

    if df is None:
        return

    styler = (
        df.style
        .format(precision=2)
        .set_table_styles(
            [
                {
                    "selector": "th",
                    "props": [
                        ("text-align", "left !important"),
                        ("white-space", "nowrap"),
                        ("border", "1px solid black"),
                        ("background-color", "#0E1C26"),
                        ("color", "white"),
                        ("position", "sticky"),
                        ("top", "0"),
                        ("z-index", "1"),
                    ],
                },
                {
                    "selector": "td",
                    "props": [
                        ("text-align", "left !important"),
                        ("white-space", "nowrap"),
                        ("border", "1px solid black"),
                        ("background-color", "#0E1C26"),
                        ("color", "white"),
                    ],
                },
                {
                    "selector": "table",
                    "props": [
                        ("border-collapse", "collapse"),
                        ("border", "1px solid black"),
                    ],
                },
            ],
            overwrite=False,
        )
        .set_properties(
            **{
                "text-align": "left",
                "white-space": "nowrap",
                "border": "1px solid black",
                "background-color": "#0E1C26",
                "color": "white",
            }
        )
    )

    html_table = styler.to_html()

    row_height = 32
    header_height = 38

    max_height_px = (
        header_height +
        rows_before_scroll * row_height
    )

    if len(df) > rows_before_scroll:

        html_render = f"""
        <div class="styled-scrollbox"
             style="
                max-height:{max_height_px}px;
                overflow-y:auto;
                overflow-x:auto;
                border:1px solid #1a2a3a;
                margin-bottom:0.75rem;">
            {html_table}
        </div>
        """

    else:
        html_render = html_table

    st.markdown(
        html_render,
        unsafe_allow_html=True
    )

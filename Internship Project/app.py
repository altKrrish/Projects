import streamlit as st

from ui.styles import apply_styles

from core.session import (
    initialize_session_state
)

from prompts.system_prompt import (
    initialize_system_prompt
)

from ui.chat import render_chat
from ui.charts import render_chart
from ui.footer import render_footer


st.set_page_config(
    page_title="SalesBot",
    layout="wide",
    page_icon="🤖"
)

apply_styles()

initialize_session_state()
initialize_system_prompt()

st.markdown(
    "<h1 class='main-title'>SalesBot</h1>",
    unsafe_allow_html=True
)

st.markdown(
    "<div class='subtext'>Your AI-powered Sales Insights Assistant</div>",
    unsafe_allow_html=True
)

render_chat()

render_footer()

render_chart()

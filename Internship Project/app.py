import streamlit as st

from ui.styles import apply_styles
from core.session import initialize_session_state
from prompts.system_prompt import initialize_system_prompt
from ui.chat import render_chat
from ui.charts import render_chart

st.set_page_config(
    page_title="SalesBot",
    layout="wide",
    page_icon="🤖"
)

apply_styles()
initialize_session_state()
initialize_system_prompt()

render_chat()
render_chart()

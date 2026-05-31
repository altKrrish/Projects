import streamlit as st


def initialize_session_state():
    defaults = {
        "result_df": None,
        "question": "",
        "ready_to_run": False,
        "show_chart": False,
        "chat_messages": [],
        "show_batch_upload": False,
        "batch_results": [],
        "awaiting_clarification": False,
        "clarification_details": {},
        "sql_generated": False,
        "sql_query": None,
        "llm_explanation": None,
        "follow_up_suggestions": [],
        "generating_suggestions": False,
    }

    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def reset_chat_state():
    """
    Resets the conversation while preserving app configuration.
    """

    st.session_state.chat_messages = []
    st.session_state.question = ""
    st.session_state.result_df = None
    st.session_state.show_chart = False

    st.session_state.show_batch_upload = False
    st.session_state.batch_results = []

    st.session_state.follow_up_suggestions = []
    st.session_state.generating_suggestions = False

    st.session_state.awaiting_clarification = False
    st.session_state.clarification_details = {}

    st.session_state.sql_generated = False
    st.session_state.sql_query = None
    st.session_state.llm_explanation = None
    st.session_state.ready_to_run = False

    if "system_prompt" in st.session_state:
        del st.session_state["system_prompt"]

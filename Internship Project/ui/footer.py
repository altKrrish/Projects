import streamlit as st

from core.session import reset_chat_state


def render_footer():

    with st._bottom:

        col1, col2, col3 = st.columns(3)

        # ----------------------------------------
        # New Chat
        # ----------------------------------------
        with col1:

            if st.button(
                "➕ New Chat",
                use_container_width=True
            ):

                reset_chat_state()

                st.rerun()

        # ----------------------------------------
        # Toggle Chart
        # ----------------------------------------
        with col2:

            if st.button(
                "📈 Toggle Chart",
                use_container_width=True
            ):

                st.session_state.show_chart = (
                    not st.session_state.get(
                        "show_chart",
                        False
                    )
                )

                st.rerun()

        # ----------------------------------------
        # Batch Upload
        # ----------------------------------------
        with col3:

            if st.button(
                "📎 Batch Upload",
                use_container_width=True
            ):

                st.session_state.show_batch_upload = (
                    not st.session_state.get(
                        "show_batch_upload",
                        False
                    )
                )

                st.session_state.question = ""

                st.rerun()

import html
import streamlit as st

from core.database import execute_sql
from core.llm import (
    generate_sql_with_context,
    generate_follow_up_questions,
)

from core.session import reset_chat_state

from utils.helpers import show_left_aligned_table
from utils.validation import validate_question


def render_chat_history():
    """
    Render all chat messages.
    """

    chat_container = st.container()

    with chat_container:

        for message in st.session_state.chat_messages:

            if message["role"] in ["user", "assistant"]:

                with st.chat_message(
                    message["role"],
                    avatar="🤠" if message["role"] == "user" else "⚙️"
                ):

                    st.markdown(
                        message["content"],
                        unsafe_allow_html=True
                    )

            elif message["role"] == "dataframe":

                show_left_aligned_table(
                    message["content"]
                )


def render_clarification_ui():
    """
    Ambiguity clarification dropdown.
    """

    if not st.session_state.get(
        "awaiting_clarification"
    ):
        return

    details = (
        st.session_state
        .clarification_details
    )

    if details.get("type") != "ambiguity":
        return

    st.info(
        f"Ambiguity detected for "
        f"'{details['term']}'"
    )

    choice = st.selectbox(
        f"Select meaning for "
        f"'{details['term']}'",
        details["options"],
        key=f"amb_{details['term']}"
    )

    if st.button(
        "✅ Confirm Selection"
    ):

        st.session_state.question = (
            st.session_state.question.replace(
                details["term"],
                choice
            )
        )

        st.session_state.awaiting_clarification = False
        st.session_state.clarification_details = {}

        issues = validate_question(
            st.session_state.question
        )

        if issues:

            st.session_state.awaiting_clarification = True

            st.session_state.clarification_details = (
                issues[0]
            )

        else:

            st.session_state.ready_to_run = True

        st.rerun()


def render_year_prompt():
    """
    Ask user for missing year.
    """

    if not st.session_state.get(
        "awaiting_clarification"
    ):
        return

    details = (
        st.session_state
        .clarification_details
    )

    if details.get("type") != "year":
        return

    with st.chat_message(
        "assistant",
        avatar="⚙️"
    ):

        st.markdown(
            details["message"]
        )


def render_follow_up_buttons():
    """
    Suggested next questions.
    """

    suggestions = st.session_state.get(
        "follow_up_suggestions",
        []
    )

    if not suggestions:
        return

    st.markdown(
        "🤔 **Suggested next questions:**"
    )

    cols = st.columns(
        len(suggestions)
    )

    for idx, suggestion in enumerate(
        suggestions
    ):

        with cols[idx]:

            if st.button(
                suggestion,
                key=f"suggestion_{idx}",
                use_container_width=True
            ):

                st.session_state.question = (
                    suggestion
                )

                st.session_state.chat_messages.append(
                    {
                        "role": "user",
                        "content": suggestion
                    }
                )

                st.session_state.ready_to_run = True

                st.session_state.awaiting_clarification = False

                st.session_state.follow_up_suggestions = []

                st.session_state.result_df = None

                st.rerun()
  def render_chat():

    # --------------------------------------------
    # Existing Messages
    # --------------------------------------------
    render_chat_history()

    render_year_prompt()

    render_follow_up_buttons()

    render_clarification_ui()

    # --------------------------------------------
    # User Input
    # --------------------------------------------
    user_input = st.chat_input(
        "Ask a sales-related question..."
    )

    if user_input:

        st.session_state.follow_up_suggestions = []

        follow_up_phrases = [
            "for the same",
            "how about",
            "what about",
            "also",
        ]

        # ----------------------------
        # Follow-up Query
        # ----------------------------
        if any(
            phrase in user_input.lower()
            for phrase in follow_up_phrases
        ):

            st.session_state.question = user_input

            st.session_state.chat_messages.append(
                {
                    "role": "user",
                    "content": user_input,
                }
            )

            st.session_state.awaiting_clarification = False
            st.session_state.ready_to_run = True

        # ----------------------------
        # Missing Year Response
        # ----------------------------
        elif (
            st.session_state.get(
                "awaiting_clarification"
            )
            and st.session_state
            .clarification_details
            .get("type")
            == "year"
        ):

            st.session_state.question = (
                f"{st.session_state.question} "
                f"{user_input}"
            )

            if (
                st.session_state.chat_messages
                and st.session_state.chat_messages[-1]["role"]
                == "user"
            ):
                st.session_state.chat_messages[-1][
                    "content"
                ] = st.session_state.question

            st.session_state.awaiting_clarification = False
            st.session_state.clarification_details = {}

            issues = validate_question(
                st.session_state.question
            )

            if issues:

                st.session_state.awaiting_clarification = True

                st.session_state.clarification_details = (
                    issues[0]
                )

            else:

                st.session_state.ready_to_run = True

        # ----------------------------
        # New Question
        # ----------------------------
        else:

            st.session_state.question = user_input

            st.session_state.chat_messages.append(
                {
                    "role": "user",
                    "content": user_input,
                }
            )

            st.session_state.awaiting_clarification = False
            st.session_state.clarification_details = {}

            issues = validate_question(
                user_input
            )

            if issues:

                st.session_state.awaiting_clarification = True

                st.session_state.clarification_details = (
                    issues[0]
                )

            else:

                st.session_state.ready_to_run = True

        st.rerun()

    # --------------------------------------------
    # Generate SQL
    # --------------------------------------------
    if st.session_state.get(
        "ready_to_run"
    ):

        question = (
            st.session_state.question
        )

        with st.spinner(
            "⚙️ Generating Query..."
        ):

            (
                explanation,
                sql_query,
                generation_error,
            ) = generate_sql_with_context(
                question,
                st.session_state.chat_messages,
            )

        if generation_error or not sql_query:

            error_message = (
                generation_error
                or explanation
                or "Model did not generate SQL."
            )

            st.session_state.chat_messages.append(
                {
                    "role": "assistant",
                    "content": f"❌ {error_message}",
                }
            )

            st.session_state.ready_to_run = False
            st.session_state.question = ""

        else:

            st.session_state.sql_query = (
                sql_query
            )

            st.session_state.llm_explanation = (
                explanation
            )

            escaped_sql = html.escape(
                sql_query
            )

            sql_html = f"""
<div style="display:flex;justify-content:left;">
<details>
<summary>View Generated SQL</summary>
<pre>
<code class="language-sql">
{escaped_sql}
</code>
</pre>
</details>
</div>
"""

            assistant_response = (
                f"{explanation}\n\n"
                f"{sql_html}"
            )

            st.session_state.chat_messages.append(
                {
                    "role": "assistant",
                    "content": assistant_response,
                }
            )

            st.session_state.sql_generated = True
            st.session_state.ready_to_run = False

        st.rerun()

    # --------------------------------------------
    # Execute SQL
    # --------------------------------------------
    elif st.session_state.get(
        "sql_generated"
    ):

        with st.spinner(
            "⚙️ Executing Query..."
        ):

            (
                df_result,
                exec_error,
            ) = execute_sql(
                st.session_state.sql_query
            )

        if exec_error:

            status_message = (
                f"❌ Query execution failed: "
                f"{exec_error}"
            )

        elif df_result.empty:

            status_message = (
                "✅ Query executed "
                "successfully but "
                "returned no rows."
            )

        else:

            status_message = (
                "✅ Query executed successfully!"
            )

        st.session_state.chat_messages.append(
            {
                "role": "assistant",
                "content": status_message,
            }
        )

        st.session_state.result_df = df_result

        if (
            df_result is not None
            and not df_result.empty
        ):

            st.session_state.chat_messages.append(
                {
                    "role": "dataframe",
                    "content": df_result,
                }
            )

            st.session_state.generating_suggestions = True

        else:

            st.session_state.sql_query = None
            st.session_state.llm_explanation = None
            st.session_state.question = ""

        st.session_state.sql_generated = False

        st.rerun()

    # --------------------------------------------
    # Generate Suggestions
    # --------------------------------------------
    elif st.session_state.get(
        "generating_suggestions"
    ):

        with st.spinner(
            "🤔 Thinking of next steps..."
        ):

            last_question = ""

            for msg in reversed(
                st.session_state.chat_messages
            ):

                if msg["role"] == "user":

                    last_question = (
                        msg["content"]
                    )

                    break

            if (
                last_question
                and st.session_state.result_df
                is not None
            ):

                st.session_state.follow_up_suggestions = (
                    generate_follow_up_questions(
                        original_question=last_question,
                        sql_query=st.session_state.sql_query,
                        df_columns=(
                            st.session_state.result_df.columns.tolist()
                        ),
                    )
                )

        st.session_state.generating_suggestions = False

        st.session_state.sql_query = None
        st.session_state.llm_explanation = None
        st.session_state.question = ""

        st.rerun()

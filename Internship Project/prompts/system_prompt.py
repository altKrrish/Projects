import streamlit as st

from core.database import get_database_schema
from schema_manager import get_llm_explanation
@st.cache_resource
def format_schema_for_prompt(
    schema: dict,
    one_big_llm_hint: str = ""
):
    formatted = []

    for table, cols in sorted(schema.items()):

        base = table.split(".")[-1]

        alias = "".join(
            [
                word[0]
                for word in base.split("_")
                if word
            ]
        ).lower()

        formatted.append(
            f"Table: {table} (alias: {alias})"
        )

        formatted.append(
            f"    Columns: ({', '.join(f'`{c}`' for c in cols)})"
        )

    if one_big_llm_hint:

        formatted.append(
            "\n### 🧠 LLM Schema Summary:\n"
        )

        formatted.append(
            one_big_llm_hint
        )

    return "\n".join(formatted)
  def initialize_system_prompt():

    if "system_prompt" in st.session_state:
        return

    schema = get_database_schema()

    if not schema:
        st.error(
            "Could not load schema from database."
        )
        st.stop()

    big_llm_hint = get_llm_explanation(
        schema
    )

    formatted_schema = (
        format_schema_for_prompt(
            schema,
            one_big_llm_hint=big_llm_hint
        )
    )

    system_prompt_string = f"""
YOUR HUGE PROMPT HERE

Part 1:
{formatted_schema}

Part 2:
...
Part 3:
...
Part 4:
...
"""
        st.session_state.system_prompt = (
        system_prompt_string
    )

    if not st.session_state.chat_messages:

        st.session_state.chat_messages = [
            {
                "role": "assistant",
                "content": (
                    "Hello, How can I help you?"
                )
            }
        ]

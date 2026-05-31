import os
import re

import streamlit as st
from dotenv import load_dotenv

import google.generativeai as genai


# --------------------------------------------------
# Gemini Setup
# --------------------------------------------------
load_dotenv("Secrets.env")

GOOGLE_API = os.getenv("GOOGLE_API")

genai.configure(api_key=GOOGLE_API)


# --------------------------------------------------
# SQL Generation
# --------------------------------------------------
def generate_sql_with_context(
    new_question,
    current_chat_messages
):
    """
    Generates SQL using conversation history.
    """

    gemini_history = []

    for msg in current_chat_messages:

        if msg["role"] not in ["user", "assistant"]:
            continue

        role = (
            "model"
            if msg["role"] == "assistant"
            else "user"
        )

        clean_content = (
            msg["content"]
            .split("\n\n<details>")[0]
        )

        gemini_history.append(
            {
                "role": role,
                "parts": [clean_content]
            }
        )

    try:

        model = genai.GenerativeModel(
            "gemini-2.5-flash",
            system_instruction=st.session_state.get(
                "system_prompt",
                ""
            ),
            generation_config=genai.types.GenerationConfig(
                temperature=0.2,
                top_p=0.93,
                top_k=40
            )
        )

        chat = model.start_chat(
            history=gemini_history
        )

        response = chat.send_message(
            new_question
        )

        assistant_reply = response.text

    except Exception as e:

        return (
            None,
            None,
            f"Gemini failed: {e}"
        )

    sql = None
    explanation = assistant_reply

    if "```sql" in assistant_reply:

        parts = assistant_reply.split(
            "```sql"
        )

        explanation = parts[0].strip()

        sql = (
            parts[1]
            .split("```")[0]
            .strip()
        )

    elif assistant_reply.lower().startswith(
        "select"
    ):
        sql = assistant_reply.strip()

    return (
        explanation,
        sql,
        None
    )


# --------------------------------------------------
# Follow-up Question Suggestions
# --------------------------------------------------
def generate_follow_up_questions(
    original_question,
    sql_query,
    df_columns
):
    """
    Generate contextual next questions.
    """

    prompt = f"""
Based on the user's last question and the result columns,
suggest 3 useful follow-up questions.

PREVIOUS QUESTION:
"{original_question}"

SQL:
"{sql_query}"

RESULT COLUMNS:
{', '.join(df_columns)}

Return ONLY a Python-style list.

Example:
[
 "Can you break this down by sales channel?",
 "How does this compare to last year?",
 "What are the top 5 products?"
]
"""

    try:

        model = genai.GenerativeModel(
            "gemini-2.5-flash"
        )

        response = model.generate_content(
            prompt,
            generation_config=genai.types.GenerationConfig(
                temperature=0.7
            )
        )

        suggestions = re.findall(
            r'"(.*?)"',
            response.text
        )

        return suggestions[:3]

    except Exception as e:

        st.warning(
            f"Could not generate suggestions: {e}"
        )

        return []

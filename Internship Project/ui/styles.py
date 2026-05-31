import streamlit as st


def apply_styles():
    st.markdown(
        """
        <style>

        .stApp,
        body {
            background-color: #0E1C26 !important;
        }

        .main-title {
            color: #00E3FF !important;
            text-align: center;
            font-size: 40px;
            font-weight: 900;
            text-shadow: 2px 2px #032B3E !important;
        }

        .subtext {
            color: #FFD700 !important;
            text-align: center;
            font-size: 16px;
        }

        .stChatMessage {
            background-color: #1a2a3a !important;
            border-radius: 10px !important;
            border: 1px solid #1a2a3a !important;
        }

        .stChatMessage * {
            color: #FFFFFF !important;
        }

        .stButton button {
            background-color: #1a2a3a !important;
            color: #FFFFFF !important;
            border: 1px solid #00E3FF !important;
        }

        .stTextInput [data-baseweb="input"] {
            background-color: #1a2a3a !important;
            color: #FFFFFF !important;
        }

        .styled-scrollbox {
            direction: rtl;
            display: flex;
            justify-content: flex-end;
            scrollbar-width: auto;
            scrollbar-color: #00E3FF #1a2a3a;
        }

        .styled-scrollbox > table {
            direction: ltr;
        }

        .styled-scrollbox::-webkit-scrollbar {
            width: 12px;
            height: 12px;
        }

        .styled-scrollbox::-webkit-scrollbar-track {
            background: #1a2a3a;
            border-radius: 10px;
        }

        .styled-scrollbox::-webkit-scrollbar-thumb {
            background-color: #00E3FF;
            border-radius: 10px;
            border: 3px solid #1a2a3a;
        }

        .styled-scrollbox::-webkit-scrollbar-thumb:hover {
            background-color: #FFD700;
        }

        details > summary {
            background-color: #1a2a3a;
            color: #FFFFFF;
            border: 1px solid #00E3FF;
            border-radius: 5px;
            padding: 5px 10px;
            cursor: pointer;
            display: inline-block;
            margin-top: 10px;
            font-weight: bold;
        }

        details > summary::marker {
            color: #FFD700;
        }

        details[open] > summary {
            background-color: #032B3E;
        }

        </style>
        """,
        unsafe_allow_html=True,
    )

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, inspect
import os
from dotenv import load_dotenv
import plotly.express as px
import google.generativeai as genai
import time
from io import BytesIO
import urllib.parse
import re
import datetime
import html
from schema_manager import get_llm_explanation

st._config.set_option("theme.base", "dark")
load_dotenv("Secrets.env")
uid = os.getenv("DB_UID") # Your DB UID
pwd = os.getenv("DB_PWD") # Your DB PWD
st.set_page_config("YourAppName", layout="wide", page_icon="🤖") # Your App Name

st.markdown("""
    <style>
        .stApp, body {
            background-color: #0E1C26 !important;
        }
        .main-title {
            color: #00E3FF !important; text-align: center; font-size: 40px; font-weight: 900;
            text-shadow: 2px 2px #032B3E !important;
        }
        .subtext {
            color: #FFD700 !important; text-align: center; font-size: 16px;
        }
        .stChatMessage {
            background-color: #1a2a3a !important; border-radius: 10px !important; border: 1px solid #1a2a3a !important;
        }
        .stChatMessage * {
            color: #FFFFFF !important;
        }
        .stButton button {
            background-color: #1a2a3a !important; color: #FFFFFF !important; border: 1px solid #00E3FF !important;
        }
        .stTextInput [data-baseweb="input"] {
            background-color: #1a2a3a !important; color: #FFFFFF !important;
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
""", unsafe_allow_html=True)

genai.configure(api_key=os.getenv("GOOGLE_API")) # Your Google API Key

try:
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=Connection_String;" # Your DB Connection String
        "DATABASE=Database;" # Your Database Name
        f"UID={uid};"
        f"PWD={pwd}"
    )
    _engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
except Exception as e:
    st.error(f"DB connection failed: {e}")
    st.stop()

def initialize_session_state():
    defaults = {
        'result_df': None, 'question': "", 'ready_to_run': False,
        'show_chart': False, 'chat_messages': [], 'show_batch_upload': False,
        'batch_results': [], 'awaiting_clarification': False, 'clarification_details': {},
        'sql_generated': False, 'sql_query': None, 'llm_explanation': None,
        'follow_up_suggestions': [],
        'generating_suggestions': False
    }
    for key, default_value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default_value

def reset_chat_state():
    initialize_session_state()
    st.session_state.chat_messages = []
    st.session_state.question = ""
    st.session_state.result_df = None
    st.session_state.show_chart = False
    st.session_state.show_batch_upload = False
    st.session_state.batch_results = []
    st.session_state.follow_up_suggestions = []
    st.session_state.generating_suggestions = False
    if 'system_prompt' in st.session_state:
        del st.session_state['system_prompt']
    initialize_system_prompt()

@st.cache_data
def get_database_schema(_engine):
    inspector = inspect(_engine)
    schema = {}
    schemas_to_include = ['your_schema_name'] # Your DB schema names
    for idx, current_schema_name in enumerate(schemas_to_include):
        for table_name in inspector.get_table_names(schema=current_schema_name):
            full_table_name = f"{current_schema_name}.{table_name}"
            try:
                schema[full_table_name] = [col['name'] for col in inspector.get_columns(table_name, schema=current_schema_name)]
            except Exception as e:
                print(f"Could not retrieve columns for {full_table_name}: {e}")
        if idx < len(schemas_to_include) - 1:
            schema[f"--- GAP ({current_schema_name} done) ---"] = []
    return schema

def show_left_aligned_table(df, rows_before_scroll: int = 10):
    if df is None: return
    styler = (df.style.format(precision=2).set_table_styles([
        {"selector": "th", "props": [("text-align", "left !important"), ("white-space", "nowrap"), ("border", "1px solid black"), ("background-color", "#0E1C26"), ("color", "white"), ("position", "sticky"), ("top", "0"), ("z-index", "1")]},
        {"selector": "td", "props": [("text-align", "left !important"), ("white-space", "nowrap"), ("border", "1px solid black"), ("background-color", "#0E1C26"), ("color", "white")]},
        {"selector": "table", "props": [("border-collapse", "collapse"), ("border", "1px solid black")]}],
        overwrite=False).set_properties(**{"text-align": "left", "white-space": "nowrap", "border": "1px solid black", "background-color": "#0E1C26", "color": "white"}))
    html_table = styler.to_html()
    row_h, header_h = 32, 38
    max_height_px = header_h + rows_before_scroll * row_h
    if len(df) > rows_before_scroll:
        html_render = f'<div class="styled-scrollbox" style="max-height:{max_height_px}px; overflow-y:auto; overflow-x:auto; border:1px solid #1a2a3a; margin-bottom:0.75rem;">{html_table}</div>'
    else:
        html_render = html_table
    st.markdown(html_render, unsafe_allow_html=True)

@st.cache_resource
def format_schema_for_prompt(schema: dict, one_big_llm_hint: str = "") -> str:
    formatted = []
    for table, cols in sorted(schema.items()):
        base = table.split('.')[-1]
        alias = ''.join([w[0] for w in base.split('_') if w]).lower()
        formatted.append(f"Table: {table} (alias: {alias})")
        formatted.append(f"    Columns: ({', '.join(f'`{c}`' for c in cols)})")
    if one_big_llm_hint:
        formatted.append("\n### 🧠 LLM Schema Summary:\n")
        formatted.append(one_big_llm_hint)
    return "\n".join(formatted)

def validate_question(question):
    detected_issues = []
    question_padded = f" {question.lower()} "
    known_terms = {
        "volume": ["Sold Quantity", "Cancelled Quantity", "Quantity"], # Placeholder columns
        "date": ["OrderDate", "DispatchDate", "DeliveryDate", "CancelDate", "CreatedDate"], # Placeholder columns
        "price": ["UnitPrice", "NetPrice", "PriceAfterDiscount"], # Placeholder columns
        "top product": ["Highest TotalNetAmount", "Highest Quantity"], # Placeholder columns
    }
    for term, options in known_terms.items():
        if f" {term.strip()} " in question_padded:
            detected_issues.append({'type': 'ambiguity', 'term': term.strip(), 'options': options})
            return detected_issues

    time_spec_found = False
    if "year"or"last" in question_padded:
        time_spec_found = True
    if not time_spec_found:
        potential_years = re.findall(r'\b\d{4}\b', question)
        if potential_years:
            current_year = datetime.datetime.now().year
            for year_str in potential_years:
                year = int(year_str)
                if 2019 < year <= current_year + 1: # Placeholder year range
                    time_spec_found = True
                    break
    if not time_spec_found:
        detected_issues.append({'type': 'year', 'message': "No time period specified. Please specify the 'YEAR' or 'FY' (e.g., 2025)."})
    return detected_issues

def generate_sql_with_context(new_question, current_chat_messages):
    gemini_history = []
    for msg in current_chat_messages:
        if msg["role"] in ["user", "assistant"]:
            role = "model" if msg["role"] == "assistant" else "user"
            gemini_history.append({"role": role, "parts": [msg["content"].split("\n\n<details>")[0]]})
    try:
        model = genai.GenerativeModel("gemini-2.5-flash", system_instruction=st.session_state.get("system_prompt", ""), generation_config=genai.types.GenerationConfig(temperature=0.2, top_p=0.93, top_k=40))
        chat = model.start_chat(history=gemini_history)
        response = chat.send_message(new_question)
        assistant_reply = response.text
    except Exception as e:
        return None, None, f"Gemini failed: {e}"
    sql, explanation = None, assistant_reply
    if "```sql" in assistant_reply:
        parts = assistant_reply.split("```sql")
        explanation = parts[0].strip()
        sql = parts[1].split("```")[0].strip()
    elif assistant_reply.lower().startswith("select"):
        sql = assistant_reply.strip()
    return explanation, sql, None

def execute_sql(sql):
    try:
        return pd.read_sql(sql, _engine), None
    except Exception as e:
        return None, f"Query failed: {e}"

def generate_follow_up_questions(original_question, sql_query, df_columns):
    """Generates contextual follow-up questions using a cost-effective LLM."""
    prompt = f"""
    Based on the user's last question and the data columns from the result, suggest 3 insightful and relevant follow-up questions a data analyst might ask next.
    The goal is to explore the data further, such as breaking it down by another dimension, comparing time periods, or focusing on top/bottom performers.

    PREVIOUS QUESTION: "{original_question}"
    GENERATED SQL QUERY: "{sql_query}"
    RESULTING DATA COLUMNS: {', '.join(df_columns)}

    Return ONLY a Python-style list of 3 short, clear question strings.
    Example: ["Can you break this down by sales channel?", "How does this compare to the previous year?", "What are the top 5 products in this category?"]
    """
    try:
        model = genai.GenerativeModel("gemini-2.5-flash")
        response = model.generate_content(prompt, generation_config=genai.types.GenerationConfig(temperature=0.7))
        suggestions = re.findall(r'"(.*?)"', response.text)
        return suggestions[:3] 
    except Exception as e:
        st.warning(f"Could not generate follow-up questions: {e}")
        return []

def initialize_system_prompt():
    if 'system_prompt' in st.session_state: return
    schema = get_database_schema(_engine)
    if not schema:
        st.error("Could not load schema from database.")
        st.stop()
    big_llm_hint = get_llm_explanation(schema) # Your LLM explanation (schema_manager.py)
    formatted_schema_with_explanation = format_schema_for_prompt(schema, one_big_llm_hint=big_llm_hint)
    system_prompt_string = f"""
You are an expert-level T-SQL Architect. Your sole function is to generate a single, optimized, and syntactically correct ,simple T-SQL query for SQL Server based on the user's request and the rules below.
You will follow this four-step internal process:
- Deconstruct Request: Silently analyze the user's goal to identify all required metrics, dimensions, and filters.
- Apply Logic: Methodically apply all relevant business logic, error handling, and metric formulas.
- Generate Query: Construct the T-SQL query, strictly adhering to all syntax, performance, and naming conventions.
- Format Output: Present the final response using the precise markdown structure specified in Part 4.
---
Part 1: Database Schema Reference:
{formatted_schema_with_explanation}
---
Part 2: Core Directives & Rules
You must follow these rules without exception.

A. T-SQL Syntax, Naming & Performance

- Always Round off to 2 decimal places.
- Global Naming Convention: All table and column references must be prefixed with `your_schema.` (e.g., `your_schema.YourTable.YourColumn`, `alias.YourColumn`).
- Engine & Compatibility: Generate T-SQL for SQL Server only. Do not use functions from other SQL dialects (e.g., `DATE_TRUNC`, `STRING_AGG`, `LPAD`).
- CTEs (Common Table Expressions): Always begin the query with a CTE. Precede the first `WITH` clause with a semicolon (;). Remove any unused CTEs from the final query.
- Joins: Default to using `LEFT JOIN`. Use `INNER JOIN` only for relationships that are mandatory and non-nullable. Do not use `FULL OUTER JOIN`.
- Table Hints: Apply `WITH (NOLOCK)` to every table, view, or CTE in all `FROM` and `JOIN` clauses.
- Pagination: For TOP N queries, use `TOP (N)` or `OFFSET ... FETCH`. Do not use `LIMIT`.
- Aliasing: Use short, intuitive table aliases (e.g., `so` for `your_schema.Sales_SalesOrder`). Every column reference must be prefixed with the correct alias (e.g., `so.OrderID`). Do not use periods in aliases themselves.

B. Error Handling & Type Safety
- Once a table is given an alias(short), Never use schema name again (e.g., use so.OrderID, not your_schema.so.OrderID).
- Safe Casting: Use `TRY_CAST` or `TRY_CONVERT` instead of `CAST` to prevent type conversion errors.
- Safe Division: To prevent divide-by-zero errors, always wrap the denominator with `NULLIF(expression, 0)`.
- Safe Aggregation: To prevent integer overflow in large sums, use `SUM(TRY_CAST(expression AS BIGINT))`.
- Date Handling: Do not cast directly from `INT` to `DATE`. Use appropriate conversion logic.
- Ensure all columns are referenced with the correct table aliases and joins.
- Identify where implicit or explicit conversion between string (VARCHAR) and integer is happening.
- Fix the query by ensuring correct data type handling without changing the logic.
- TOP N WITH TIES clause is not allowed without a corresponding ORDER BY clause
- Revise the SQL to eliminate the 'Conversion failed when converting the nvarchar value ''TV'' to data type int' error: do not cast alphanumeric NVARCHARs to INT; instead align datatypes (cast the INT side to NVARCHAR or use TRY_CONVERT for numeric-only rows)
- Only prepend 'your_schema. or any other alias' if the table name is not already schema-qualified (doesn't contain a period).
- Correct any syntax errors related to the NOLOCK hint in the SELECT statement.
- Use proper date functions (YEAR(), FORMAT()) instead of string manipulation for filtering and grouping to ensure reliable and readable SQL.

C. Standard Business Logic & Filters (Apply to ALL Queries)

- Only show top 1000 results.
- Always display the total values by default for the question asked.
- Default Date Range: `[YourDateColumn] >= 'YYYY-MM-DD'` # Your default date filter
- When date is specified use full range for that day (eg. so.OrderDate >= 'Date' and so.OrderDate < 'Date+1).
- Exclude Cancelled Orders: `[YourStatusIDColumn] NOT IN (17, 20, 21) AND [YourCancelDateColumn] IS NULL` # Your specific filters
- Exclude Returned Items: `ISNULL([YourItemStatusID], 0) NOT IN (20)` # Your specific filters
- Include Valid Products: `[YourProductIDColumn] > 0` # Your specific filters
- Fiscal Year Logic: A fiscal year (e.g., 'FY2024') runs from April 1st to March 31st. Translate this to `[YourDateColumn] >= 'YYYY-MM-DD' AND [YourDateColumn] < 'YYYY-MM-DD'`.
- Specific Column Mapping:
- Target (only use these) = 'your_schema.Sales_SaleTargets.Sales', Mobile Target = 'your_schema.Sales_MobileTargets.Sales',[Always use Daily/Hourly filtering  (e.g. 'your_schema.Sales_SaleTargets.Type' = 'Daily')],[When asked monthly target 'SUM 'Daily' sales for that month'.], Dont use TargetDate use Date. Use 
    
    - Always use `your_schema.Sales_SalesOrderlines.SalesChannelID` for `SalesChannelID`. # Your specific column mapping
    - Always use `your_schema.Auction_TVAuctionPrice.TargetPrice` for `TargetPrice`. # Your specific column mapping
    - Quantity = "your_schema.Sales_SalesOrderlines.QTY" # Your specific column mapping
    - TotalNetAmount = "your_schema.Sales_SalesOrderlines.TotalNetAmt" # Your specific column mapping
    - ProductID = "your_schema.Sales_SalesOrderlines.ProductID" # Your specific column mapping
    - Discount = "your_schema.Discount_Discounts" # Your specific column mapping
    - Source/Channel = "your_schema.Sales_MstSalesChannels" # Your specific column mapping
    
D. Advanced Logic & Metric Formulas

- Metric Formulas(If the user asks for any words, use ONLY its exact SQL expression listed below. Do NOT search for, infer, or replace with other columns or words)
    *sol = your_schema.Sales_SalesOrderLines , BP = your_schema.System_MstBudgetPay # Your specific aliases and tables
    
    - DepartmentID : "your_schema.Sales_SalesOrders" # Your specific column mapping
    - Mobile = "your_schema.System_MstDepartments.DepartmentID = 15" # Your specific filter
    - Gross Sales: `SUM([YourColumn])` # Your formula
    - Unique Customers: `COUNT(DISTINCT [YourCustomerID])` # Your formula
    - Order Count: `COUNT(DISTINCT [YourOrderID])` # Your formula
    - Margin: `[Your Margin Formula]` # Your specific formula
    - Margin %: `[Your Margin % Formula]` # Your specific formula
    - Margin Loss: `[Your Margin Loss Formula]` # Your specific formula
    - AuctionDuration (in minutes): `[Your Auction Duration Formula]` # Your specific formula
    - GCPM = `[Your GCPM Formula]` # Your specific formula
    - PnP = `[Your PnP Formula]` # Your specific formula
    - New Customers:`[Your New Customers Logic]` # Your specific logic
    - Cash Sales: `[Your Cash Sales Formula]` # Your specific formula
    - When asked for specific type of sales use 'your_schema.Sales_MstSalesChannels'(eg. Mobile FPC = 'your_schema.Sales_MstSalesChannels.Name like'%Mobile FPC%'). # Your specific logic
- Top/Best-Selling Logic: When asked for top-selling products, order results descending by `SUM([YourQuantityColumn])`. # Your specific logic
- Flexible Product search: If asked to find a specific product, search for the keyword across all relevant name and description columns using `LIKE '%keyword%'`.
- When filtering for channels, do not use exact matches. Instead, use 'LIKE' with wildcards to include any name containing "TV" or "Web". [For example:(your_schema.Sales_MstSalesChannels.Name LIKE '%TV%' OR your_schema.Sales_MstSalesChannels.Name LIKE '%Web%')] # Your specific logic
- Customer Acquisition Logic: To find a customer's first purchase, find the `MIN([YourOrderDate])` for each `[YourCustomerID]` after the `[YourCreatedDate]`. # Your specific logic
- Only use Columns with Tables they exist in.
- Weekly Performance Classification: If requested, classify week-over-week performance changes as 'Growth', 'Decline', 'Stable' using a `CASE` statement comparing the current week's metric to the 
previous week's.
- Mainly join using [YourJoiningColumn1],[YourJoiningColumn2]
---
Part 3: Final Output Format
You must present your final answer using the exact format below. Provide no other commentary or text outside this structure.
T-SQL Query
-- Your generated T-SQL query goes here
Explanation
A 2-3 line summary explaining the query's objective, the logic used to fulfill the request, and any specific error-prevention techniques applied.
Validation
-- This query checks for data existence based on the primary filters.
-- It should return a count > 0 if data is available.
SELECT COUNT(*)
FROM your_schema.Sales_SalesOrder AS so WITH (NOLOCK)
WHERE /* Add the primary date or fiscal year filter from the main query here */;
Validation Points:
- Verify the final `SELECT` statement includes all columns and metrics requested.
- Check that Gross Sales and Order Count are positive values.
- Ensure Order Count is greater than or equal to Unique Customers.
"""
    st.session_state.system_prompt = system_prompt_string
    if not st.session_state.chat_messages:
        st.session_state.chat_messages = [{"role": "assistant", "content": "Hello, How can I help you?"}]

# --- Initialize App State and Prompt ---
initialize_session_state()
initialize_system_prompt()

# --- Main App UI ---
st.markdown("<h1 class='main-title'>YourAppName</h1>", unsafe_allow_html=True) # Your App Name
st.markdown("<div class='subtext'>Your AI-powered Sales Insights Assistant</div>", unsafe_allow_html=True)

chat_container = st.container()
with chat_container:
    for message in st.session_state.chat_messages:
        if message["role"] in ["user", "assistant"]:
            with st.chat_message(message["role"], avatar="🤠" if message["role"] == "user" else "⚙️"):
                st.markdown(message["content"], unsafe_allow_html=True)
        elif message["role"] == "dataframe":
            show_left_aligned_table(message["content"])

    if st.session_state.get('awaiting_clarification'):
        details = st.session_state.clarification_details
        if details['type'] == 'year':
            with st.chat_message("assistant", avatar="⚙️"):
                st.markdown(details['message'])

# --- Display Follow-up Suggestion Buttons ---
if st.session_state.get("follow_up_suggestions"):
    st.markdown("🤔 **Suggested next questions:**")
    num_suggestions = len(st.session_state.follow_up_suggestions)
    cols = st.columns(num_suggestions)
    for i, suggestion in enumerate(st.session_state.follow_up_suggestions):
        with cols[i]:
            if st.button(suggestion, key=f"suggestion_{i}", use_container_width=True):
                st.session_state.question = suggestion
                st.session_state.chat_messages.append({"role": "user", "content": suggestion})
                st.session_state.ready_to_run = True
                st.session_state.awaiting_clarification = False
                st.session_state.follow_up_suggestions = [] 
                st.session_state.result_df = None
                st.rerun()

if st.session_state.get("show_batch_upload"):
    st.subheader("📥 Batch Upload from Excel")
    # Batch upload logic remains the same...

if not st.session_state.get("show_batch_upload"):
    if st.session_state.get('awaiting_clarification') and st.session_state.clarification_details.get('type') == 'ambiguity':
        details = st.session_state.clarification_details
        st.info(f"Ambiguity detected for **'{details['term']}'**:")
        choice = st.selectbox(f"Select meaning for '{details['term']}':", details['options'], key=f"amb_{details['term']}")
        if st.button("✅ Confirm Selection"):
            st.session_state.question = st.session_state.question.replace(details['term'], choice)
            st.session_state.awaiting_clarification = False
            st.session_state.clarification_details = {}
            issues = validate_question(st.session_state.question)
            if issues:
                st.session_state.clarification_details = issues[0]
                st.session_state.awaiting_clarification = True
            else:
                st.session_state.ready_to_run = True
            st.rerun()

    user_input = st.chat_input("Ask a sales-related question...")

    if user_input:
        st.session_state.follow_up_suggestions = [] 
        follow_up_phrases = ["for the same", "how about", "what about","also"]
        if any(phrase in user_input.lower() for phrase in follow_up_phrases):
            st.session_state.question = user_input
            st.session_state.chat_messages.append({"role": "user", "content": user_input})
            st.session_state.awaiting_clarification = False
            st.session_state.ready_to_run = True

        elif st.session_state.get('awaiting_clarification') and st.session_state.clarification_details.get('type') == 'year':
            st.session_state.question = f"{st.session_state.question} {user_input}"
            if st.session_state.chat_messages and st.session_state.chat_messages[-1]["role"] == "user":
                st.session_state.chat_messages[-1]["content"] = st.session_state.question
            st.session_state.awaiting_clarification = False
            st.session_state.clarification_details = {}
            issues = validate_question(st.session_state.question)
            if issues:
                st.session_state.clarification_details = issues[0]
                st.session_state.awaiting_clarification = True
            else:
                st.session_state.ready_to_run = True
        
        else:
            st.session_state.question = user_input
            st.session_state.chat_messages.append({"role": "user", "content": user_input})
            st.session_state.awaiting_clarification = False
            st.session_state.clarification_details = {}
            issues = validate_question(user_input)
            if issues:
                st.session_state.clarification_details = issues[0]
                st.session_state.awaiting_clarification = True
            else:
                st.session_state.ready_to_run = True
                
        st.rerun()

    if st.session_state.get('ready_to_run'):
        q = st.session_state.question
        with st.spinner("⚙️ Generating Query..."):
            gen_explanation, sql_query, gen_error = generate_sql_with_context(q, st.session_state.chat_messages)
        
        if gen_error or not sql_query:
            error_message = gen_error or (gen_explanation or "The model did not generate a SQL query.")
            st.session_state.chat_messages.append({"role": "assistant", "content": f"❌ {error_message}"})
            st.session_state.ready_to_run = False
            st.session_state.question = ""
        else:
            st.session_state.sql_query = sql_query
            st.session_state.llm_explanation = gen_explanation
            escaped_sql = html.escape(sql_query)
            sql_expander_html = f"""<div style="display: flex; justify-content: left; align-items: left;">
<details>
    <summary>View Generated SQL</summary>
    <pre><code class="language-sql">{escaped_sql}</code></pre>
</details>
</div>"""
            response_content = f"{gen_explanation}\n\n{sql_expander_html}"
            st.session_state.chat_messages.append({"role": "assistant", "content": response_content})
            st.session_state.ready_to_run = False
            st.session_state.sql_generated = True
        st.rerun()

    # MODIFIED: Logic split into two parts for sequential display
    elif st.session_state.get('sql_generated'):
        with st.spinner("⚙️ Executing Query..."):
            df_result, exec_error = execute_sql(st.session_state.sql_query)

        exec_status_message = ""
        if exec_error:
            exec_status_message = f"❌ Query execution failed: {exec_error}"
        elif df_result.empty:
            exec_status_message = "✅ Query executed successfully, but returned no results."
        else:
            exec_status_message = "✅ Query executed successfully!"
        
        st.session_state.chat_messages.append({"role": "assistant", "content": exec_status_message})
        st.session_state.result_df = df_result
    
        if df_result is not None and not df_result.empty:
            st.session_state.chat_messages.append({"role": "dataframe", "content": df_result})
            # Set flag to generate suggestions on the next rerun, AFTER results are shown
            st.session_state.generating_suggestions = True
        else:
            # If no results, clear query state now as no suggestions will be generated
            st.session_state.sql_query = None
            st.session_state.llm_explanation = None
            st.session_state.question = ""
        
        st.session_state.sql_generated = False # Prevent this block from re-running
        st.rerun()

    # NEW: This block runs AFTER the results are displayed to the user
    elif st.session_state.get('generating_suggestions'):
        with st.spinner("🤔 Thinking of next steps..."):
            last_user_question = ""
            for msg in reversed(st.session_state.chat_messages):
                if msg["role"] == "user":
                    last_user_question = msg["content"]
                    break
            
            if last_user_question and st.session_state.result_df is not None:
                st.session_state.follow_up_suggestions = generate_follow_up_questions(
                    original_question=last_user_question,
                    sql_query=st.session_state.sql_query,
                    df_columns=st.session_state.result_df.columns.tolist()
                )

        # Clean up all temporary states after suggestions are generated
        st.session_state.generating_suggestions = False
        st.session_state.sql_query = None
        st.session_state.llm_explanation = None
        st.session_state.question = ""
        st.rerun()


# --- Footer and Charting Logic (Unchanged) ---
with st._bottom:
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("➕ New Chat", use_container_width=True):
            reset_chat_state()
            st.rerun()
    with col2:
        if st.button("📈 Toggle Chart", use_container_width=True):
            st.session_state.show_chart = not st.session_state.show_chart
            st.rerun()
    with col3:
        if st.button("📎 Batch Upload", use_container_width=True):
            st.session_state.show_batch_upload = not st.session_state.get("show_batch_upload", False)
            st.session_state.question = ""
            st.rerun()

if st.session_state.show_chart and st.session_state.result_df is not None and not st.session_state.result_df.empty:
    df = st.session_state.result_df
    st.subheader("📈 Chart Generator")
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    if not numeric_cols:
        st.warning("No numeric columns for charting.")
    else:
        chart_type = st.selectbox("Chart Type", ["Bar", "Line", "Pie", "Scatter", "Area", "Box"])
        color_col = st.selectbox("Color By", ["None"] + df.select_dtypes(include=['object']).columns.tolist())
        x_col = st.selectbox("X-axis", df.columns.tolist())
        y_col = st.selectbox("Y-axis", numeric_cols)
        try:
            color_arg = color_col if color_col != "None" else None
            if chart_type == "Bar": fig = px.bar(df, x=x_col, y=y_col, color=color_arg)
            elif chart_type == "Line": fig = px.line(df, x=x_col, y=y_col, color=color_arg)
            elif chart_type == "Pie": fig = px.pie(df, names=x_col, values=y_col)
            elif chart_type == "Scatter": fig = px.scatter(df, x=x_col, y=y_col, color=color_arg)
            elif chart_type == "Area": fig = px.area(df, x=x_col, y=y_col, color=color_arg)
            elif chart_type == "Box": fig = px.box(df, x=x_col, y=y_col, color=color_arg)
            fig.update_layout(template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Chart error: {e}")

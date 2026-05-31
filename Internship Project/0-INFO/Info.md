# 🤖 SalesBot – AI-Powered Sales Insights Assistant

An intelligent Streamlit-based analytics application that converts natural language business questions into optimized SQL queries, executes them against a SQL Server database, and presents results through interactive tables and visualizations.

## 🚀 Features

### 💬 Natural Language to SQL

* Ask business questions in plain English.
* Powered by Google Gemini for SQL generation.
* Context-aware conversations with chat history support.
* Automatic follow-up question handling.

### 🗄️ Database Intelligence

* Auto-discovers database schema.
* Uses SQLAlchemy for SQL Server connectivity.
* Schema-aware query generation.
* Built-in query validation and execution.

### 🔍 Smart Query Validation

* Detects ambiguous business terms.
* Requests clarification when needed.
* Enforces time-period validation.
* Applies business rules automatically.

### 📊 Interactive Analytics

* Dynamic result tables.
* Scrollable and styled data grids.
* Interactive chart generation using Plotly.
* Multiple chart types:

  * Bar Charts
  * Line Charts
  * Pie Charts
  * Scatter Plots
  * Area Charts
  * Box Plots

### 🧠 AI-Powered Insights

* Generates contextual SQL explanations.
* Suggests relevant follow-up business questions.
* Maintains conversation context across queries.

### 📥 Batch Processing

* Excel-based batch uploads.
* Bulk query execution support.
* Automated result processing.

---

# 🏗️ Architecture

```text
User Question
      │
      ▼
Streamlit UI
      │
      ▼
Question Validation
      │
      ▼
Gemini 2.5 Flash
      │
      ▼
SQL Generation
      │
      ▼
SQL Server Execution
      │
      ▼
Result Visualization
      │
      ▼
Follow-up Suggestions
```

---

# 📦 Technology Stack

| Component       | Technology              |
| --------------- | ----------------------- |
| Frontend        | Streamlit               |
| AI Engine       | Google Gemini 2.5 Flash |
| Database        | Microsoft SQL Server    |
| ORM             | SQLAlchemy              |
| Data Processing | Pandas                  |
| Visualization   | Plotly                  |
| Configuration   | Python Dotenv           |

---

# ⚙️ Installation

## 1. Clone Repository

```bash
git clone https://github.com/yourusername/your-repository.git

cd your-repository
```

## 2. Create Virtual Environment

```bash
python -m venv venv
```

### Windows

```bash
venv\Scripts\activate
```

### Linux / Mac

```bash
source venv/bin/activate
```

## 3. Install Dependencies

```bash
pip install -r requirements.txt
```

---

# 🔐 Environment Variables

Create a file named:

```text
Secrets.env
```

Add the following variables:

```env
DB_UID=your_database_username
DB_PWD=your_database_password

GOOGLE_API=your_gemini_api_key
```

---

# 🗄️ Database Configuration

Update the SQL Server connection details in the application:

```python
SERVER=your_server_name
DATABASE=your_database_name
```

Ensure SQL Server ODBC Driver 17 is installed:

```text
ODBC Driver 17 for SQL Server
```

Download:
https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server

---

# ▶️ Running the Application

```bash
streamlit run app.py
```

Application will be available at:

```text
http://localhost:8501
```

---

# 🧩 Key Functionalities

## Natural Language Queries

Example questions:

```text
What were the total sales in FY2025?
```

```text
Show top 10 products by quantity sold.
```

```text
Compare TV and Web sales channels for last year.
```

```text
What is the margin percentage by department?
```

---

## SQL Generation

The application:

* Reads database schema.
* Builds contextual prompts.
* Generates optimized T-SQL.
* Applies business logic automatically.
* Displays generated SQL for transparency.

---

## Data Visualization

Users can instantly visualize results using:

* Bar Charts
* Line Charts
* Pie Charts
* Scatter Charts
* Area Charts
* Box Charts

With configurable:

* X-Axis
* Y-Axis
* Color Dimensions

---

# 🛡️ Built-In Safety Rules

The AI engine automatically enforces:

* SQL Server compatible syntax
* NOLOCK hints
* Safe casting using TRY_CAST
* Divide-by-zero prevention
* Fiscal year handling
* Business-specific filters
* Top 1000 result limits
* Query validation checks

---

# 📈 Example Workflow

```text
User:
"Top 10 products by sales in FY2025"

↓
Gemini generates SQL

↓
SQL executes against SQL Server

↓
Results displayed in Streamlit

↓
Charts generated

↓
Suggested follow-up questions:
• Show by sales channel
• Compare with FY2024
• Show bottom 10 products
```

---

# 🔧 Future Enhancements

* Authentication & Role-Based Access
* Query History
* Dashboard Saving
* Scheduled Reports
* Export to Excel/PDF
* Multi-Database Support
* Azure OpenAI Integration
* KPI Monitoring Dashboard

---


# 👨‍💻 Author

Developed as an AI-powered enterprise analytics assistant for transforming natural language business questions into actionable SQL insights.

⭐ If you find this project useful, consider giving it a star on GitHub.


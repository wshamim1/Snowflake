# Snowflake Python Project

A complete Python-based toolkit for working with Snowflake, featuring database operations, Jupyter notebooks, and Streamlit applications.

## Table of Contents

- [Features](#features)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Examples](#examples)
- [Security](#security)
- [Contributing](#contributing)

## Features

- ✅ **Database Operations**: Complete CRUD operations (Create, Read, Update, Delete) for Snowflake
- 📊 **Interactive Notebooks**: Jupyter notebooks for data analysis (local & portable)
- 🚀 **Streamlit Web App**: Interactive explorer with SQL runner, table management, CRUD forms, and dynamic charting
- 📈 **Data Visualization**: Bar and pie charts with dynamic grouping and metrics
- 🔒 **Secure**: Environment-based credential management with .env files
- 🛠️ **Modular**: Clean separation of concerns with reusable components
- 🔄 **Dual-Mode Connectivity**: Works both locally (via .env) and in Snowflake-hosted environments

## Project Structure

```
.
├── src/
│   ├── db/
│   │   └── snowflake_connection.py    # Database connection handler
│   └── operations/
│       ├── create_table_operations.py # Table creation
│       ├── select_operations.py       # SELECT queries
│       ├── insert_operations.py       # INSERT queries
│       ├── update_operations.py       # UPDATE queries
│       └── delete_operations.py       # DELETE queries
├── apps/
│   ├── notebooks/
│   │   └── quickstart.ipynb          # Jupyter notebook starter
│   └── streamlit/
│       └── app.py                    # Streamlit web app
├── docs/                              # Documentation
├── .env.example                       # Example environment file
├── .gitignore                         # Git ignore rules
├── requirements.txt                   # Python dependencies
└── README.md                          # This file
```

## Prerequisites

- Python 3.8+
- Snowflake account
- Virtual environment (recommended)

## Installation

1. **Clone the repository** (if applicable):

   ```bash
   git clone <repository-url>
   cd Snowflake
   ```

2. **Create a virtual environment**:

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**:

   ```bash
   pip install -r requirements.txt
   ```

## Configuration

1. **Copy the example environment file**:

   ```bash
   cp .env.example .env
   ```

2. **Edit `.env` with your Snowflake credentials**:

   ```env
   SNOWFLAKE_ACCOUNT=your-account
   SNOWFLAKE_USER=your-username
   SNOWFLAKE_PASSWORD=your-password
   SNOWFLAKE_WAREHOUSE=your-warehouse
   SNOWFLAKE_DATABASE=your-database
   SNOWFLAKE_SCHEMA=your-schema
   ```

   **Note**: The app auto-detects your connection (Snowflake session or local .env).

## Usage

All commands should be run from the project root directory.

### Database Operations

**Create a table**:

```bash
python src/operations/create_table_operations.py [table_name]
```

**Select data**:

```bash
python src/operations/select_operations.py ["SQL QUERY"]
```

**Insert data**:

```bash
python src/operations/insert_operations.py
```

**Update data**:

```bash
python src/operations/update_operations.py ["UPDATE SQL"]
```

**Delete data**:

```bash
python src/operations/delete_operations.py ["DELETE SQL"]
```

### Streamlit Application

Launch the interactive data explorer:

```bash
streamlit run apps/streamlit/app.py
```

Then open your browser to `http://localhost:8501`.

**Features**:
- **Create Table Tab**: Set up the SALES_DEMO table with 12 sample records
- **SQL Runner Tab**: Execute custom SQL queries
- **Insert/Update/Delete Tab**: CRUD operations with forms
- **Bar Chart Tab**: Dynamic visualization with multiple grouping options

### Jupyter Notebooks

Start Jupyter and open the quickstart notebook:

```bash
jupyter notebook apps/notebooks/quickstart.ipynb
```

Or use VS Code with the Jupyter extension.

## Examples

### Example 1: Using the Streamlit App (Recommended)

```bash
# Start the app
streamlit run apps/streamlit/app.py

# In the UI:
# 1. Go to "Create Table" tab
# 2. Click "Create / Refresh Table with Sample Data"
# 3. Switch to other tabs to query, insert, update, delete, or visualize data
```

### Example 2: SQL Query with Streamlit

In the Streamlit app's "SQL Runner" tab:

```sql
SELECT REGION, SUM(SALES_AMOUNT) as total_sales FROM SALES_DEMO GROUP BY REGION;
```

### Example 3: Interactive Analysis with Jupyter

```bash
# Start Jupyter
jupyter notebook apps/notebooks/quickstart.ipynb

# Or use the portable standalone notebook
jupyter notebook apps/notebooks/snowflake_standalone.ipynb
```

### Example 4: Command-Line Database Operations

```bash
# Create a table
python src/operations/create_table_operations.py my_table

# Query data
python src/operations/select_operations.py "SELECT * FROM my_table LIMIT 10"

# Insert data
python src/operations/insert_operations.py

# Update data
python src/operations/update_operations.py

# Delete data
python src/operations/delete_operations.py
```

## Security

⚠️ **Important Security Notes**:

- Never commit `.env` to version control (it's in `.gitignore`)
- Use `.env.example` as a template for initial setup
- Rotate Snowflake credentials regularly
- Use least-privilege access roles in Snowflake
- Consider using Snowflake key-pair authentication for production
- Don't share `.env` files or expose credentials in logs

---

**Quick Start**:

1. Set up `.env` with your Snowflake credentials
2. Install dependencies: `pip install -r requirements.txt`
3. Run Streamlit: `streamlit run apps/streamlit/app.py`
4. Click "Create Table" tab → "Create / Refresh Table with Sample Data"
5. Explore the other tabs for SQL queries, CRUD operations, and charting

**Need help?** Check the `docs/` folder or review the examples above.

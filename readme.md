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

- ✅ **Database Operations**: Complete CRUD operations for Snowflake
- 📊 **Interactive Notebooks**: Jupyter notebooks for data analysis
- 🚀 **Streamlit App**: Web-based query explorer
- 🔒 **Secure**: Environment-based credential management
- 🛠️ **Modular**: Clean separation of concerns

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
   SNOWFLAKE_TABLE=your-table
   ```

3. **Optional query configurations**:

   ```env
   SNOWFLAKE_SELECT_QUERY=SELECT * FROM TABLE1;
   SNOWFLAKE_INSERT_QUERY=INSERT INTO TABLE1 (ID, NAME) VALUES (1, 'Test');
   SNOWFLAKE_UPDATE_QUERY=UPDATE TABLE1 SET NAME = 'Updated' WHERE ID = 1;
   SNOWFLAKE_DELETE_QUERY=DELETE FROM TABLE1 WHERE ID = 1;
   ```

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

Launch the interactive query explorer:

```bash
streamlit run apps/streamlit/app.py
```

Then open your browser to `http://localhost:8501`.

### Jupyter Notebooks

Start Jupyter and open the quickstart notebook:

```bash
jupyter notebook apps/notebooks/quickstart.ipynb
```

Or use VS Code with the Jupyter extension.

## Examples

### Example 1: Create and Query a Table

```bash
# Create table
python src/operations/create_table_operations.py my_table

# Query data
python src/operations/select_operations.py "SELECT * FROM my_table"
```

### Example 2: Using Environment Variables

Set `SNOWFLAKE_SELECT_QUERY` in `.env`, then:

```bash
python src/operations/select_operations.py
```

### Example 3: Interactive Analysis

```bash
# Start Streamlit app
streamlit run apps/streamlit/app.py

# Or use Jupyter notebook
jupyter notebook apps/notebooks/quickstart.ipynb
```

## Security

⚠️ **Important Security Notes**:

- Never commit `.env` to version control
- Use `.env.example` as a template only
- Rotate credentials regularly
- Use least-privilege access roles in Snowflake
- Consider using Snowflake key-pair authentication for production

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

**Need help?** Check the `docs/` folder or open an issue.

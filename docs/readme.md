# Snowflake Setup Guide

## Project Structure

- `src/db/` → connection layer
- `src/operations/` → CRUD + create-table scripts
- `apps/notebooks/` → notebook files
- `apps/streamlit/` → Streamlit app
- `docs/` → documentation

## Requirements

- Python 3.x
- Packages from `requirements.txt`

## Installation

1. Install dependencies:

     ```sh
     pip install -r requirements.txt
     ```

2. Configure `.env` in project root.

## Usage

Run from project root:

- Create table:

    ```sh
    python src/operations/create_table_operations.py
    ```

- Select:

    ```sh
    python src/operations/select_operations.py
    ```

- Insert:

    ```sh
    python src/operations/insert_operations.py
    ```

- Update:

    ```sh
    python src/operations/update_operations.py
    ```

- Delete:

    ```sh
    python src/operations/delete_operations.py
    ```

- Start Streamlit app:

    ```sh
    streamlit run apps/streamlit/app.py
    ```

- Open notebook:

    - `apps/notebooks/quickstart.ipynb`

## Security Note

- Never commit the `.env` file to version control.

# Snowflake Setup Guide

## Introduction
This repository provides a simple setup to connect to and execute queries on Snowflake using Python.

## Requirements
- Python 3.x
- `snowflake-connector-python` package
- `python-dotenv` package

## Installation
1. Install dependencies:
    ```sh
    pip install snowflake-connector-python python-dotenv
    ```
2. Create a `.env` file and provide your Snowflake credentials.

## Usage
- Use `select_operations.py` to fetch data.
- Use `insert_operations.py` to insert records.
- Use `update_operations.py` to update records.
- Use `delete_operations.py` to delete records.

## Example Query Execution
```sh
python select_operations.py
```

## Security Note
- Never commit the `.env` file to version control.

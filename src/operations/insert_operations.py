

import os
import sys
from pathlib import Path
from typing import Optional

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.db.snowflake_connection import SnowflakeConnection

class InsertOperations:
    """
    Handles INSERT queries in Snowflake.
    """
    @staticmethod
    def _parse_table_name(table_name: str) -> Optional[tuple[str, str, str]]:
        """
        Parses table name into (database, schema, table).
        Supported formats:
        - table
        - schema.table
        - database.schema.table
        """
        parts = [p.strip().strip('"') for p in table_name.split(".") if p.strip()]
        db = os.getenv("SNOWFLAKE_DATABASE", "").strip().strip('"')
        schema = os.getenv("SNOWFLAKE_SCHEMA", "").strip().strip('"')

        if len(parts) == 1:
            if not db or not schema:
                return None
            return db, schema, parts[0]
        if len(parts) == 2:
            if not db:
                return None
            return db, parts[0], parts[1]
        if len(parts) == 3:
            return parts[0], parts[1], parts[2]
        return None

    @staticmethod
    def _table_exists(conn, table_name: str) -> bool:
        """
        Checks whether the target table exists and is visible to current role.
        """
        parsed = InsertOperations._parse_table_name(table_name)
        if parsed is None:
            print(
                "Could not resolve table name. Provide SNOWFLAKE_TABLE as table, schema.table, or database.schema.table, and ensure SNOWFLAKE_DATABASE/SNOWFLAKE_SCHEMA are set."
            )
            return False

        database, schema, table = parsed
        check_query = (
            f"SELECT COUNT(*) FROM {database}.INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s"
        )

        cursor = conn.cursor()
        try:
            cursor.execute(check_query, (schema.upper(), table.upper()))
            count = cursor.fetchone()[0]
            return count > 0
        except Exception as e:
            print(f"Error checking table existence: {e}")
            return False
        finally:
            cursor.close()

    @staticmethod
    def _get_table_columns(conn, table_name: str) -> list[str]:
        """
        Returns ordered column names for a table.
        """
        parsed = InsertOperations._parse_table_name(table_name)
        if parsed is None:
            return []

        database, schema, table = parsed
        query = (
            f"SELECT COLUMN_NAME FROM {database}.INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
            "ORDER BY ORDINAL_POSITION"
        )

        cursor = conn.cursor()
        try:
            cursor.execute(query, (schema.upper(), table.upper()))
            return [row[0] for row in cursor.fetchall()]
        except Exception:
            return []
        finally:
            cursor.close()

    @staticmethod
    def get_insert_template(table_name: str) -> Optional[str]:
        """
        Builds a valid INSERT template based on actual table columns.
        """
        conn = SnowflakeConnection.get_connection()
        if conn is None:
            return None
        try:
            if not InsertOperations._table_exists(conn, table_name):
                return None
            columns = InsertOperations._get_table_columns(conn, table_name)
            if not columns:
                return None
            column_list = ", ".join(columns)
            values_list = ", ".join(["'<value>'" for _ in columns])
            return f"INSERT INTO {table_name} ({column_list}) VALUES ({values_list});"
        finally:
            conn.close()

    @staticmethod
    def insert_data(query: str, table_name: Optional[str] = None) -> bool:
        """
        Executes an INSERT query.

        Args:
            query (str): The SQL INSERT query to execute.
            table_name (Optional[str]): Optional table name to validate before insert.

        Returns:
            bool: True if the insert was successful, False otherwise.
        """
        conn = SnowflakeConnection.get_connection()
        if conn is None:
            print("No connection available.")
            return False
        try:
            if table_name and not InsertOperations._table_exists(conn, table_name):
                print(f"Table '{table_name}' does not exist or not authorized.")
                return False

            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            return True
        except Exception as e:
            print(f"Error executing insert: {e}")
            return False
        finally:
            try:
                cursor.close()
            except Exception:
                pass
            conn.close()

if __name__ == "__main__":
    table_name = os.getenv("SNOWFLAKE_TABLE", "").strip()
    query_from_env = os.getenv("SNOWFLAKE_INSERT_QUERY", "").strip()
    columns_from_env = os.getenv("SNOWFLAKE_INSERT_COLUMNS", "").strip()
    values_from_env = os.getenv("SNOWFLAKE_INSERT_VALUES", "").strip()

    if query_from_env:
        sample_query = query_from_env
    elif table_name and columns_from_env and values_from_env:
        columns = [c.strip() for c in columns_from_env.split(",") if c.strip()]
        values = [v.strip() for v in values_from_env.split(",")]
        if len(columns) != len(values):
            print("SNOWFLAKE_INSERT_COLUMNS and SNOWFLAKE_INSERT_VALUES must have the same number of items.")
            sample_query = ""
        else:
            escaped_values = ["'" + v.replace("'", "''") + "'" for v in values]
            sample_query = (
                f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(escaped_values)});"
            )
    elif table_name:
        template = InsertOperations.get_insert_template(table_name)
        if template:
            print("No insert query provided.")
            print("Use this template in SNOWFLAKE_INSERT_QUERY (or set SNOWFLAKE_INSERT_COLUMNS/SNOWFLAKE_INSERT_VALUES):")
            print(template)
        else:
            print("Could not build template. Check SNOWFLAKE_TABLE and table access.")
        sample_query = ""
    else:
        sample_query = "INSERT INTO your_table (column1, column2) VALUES ('value1', 'value2');"
        print(
            "Tip: Set SNOWFLAKE_TABLE or SNOWFLAKE_INSERT_QUERY in .env to avoid placeholder table names."
        )

    success = bool(sample_query) and InsertOperations.insert_data(sample_query, table_name=table_name or None)
    if success:
        print("Insert successful.")
    else:
        print("Insert failed.")

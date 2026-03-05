
import os
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.db.snowflake_connection import SnowflakeConnection


class CreateTableOperations:
    """
    Handles CREATE TABLE queries in Snowflake.
    """
    @staticmethod
    def create_table(query: str) -> bool:
        """
        Executes a CREATE TABLE query.

        Args:
            query (str): The SQL CREATE TABLE query to execute.

        Returns:
            bool: True if successful, False otherwise.
        """
        conn = SnowflakeConnection.get_connection()
        if conn is None:
            print("No connection available.")
            return False

        try:
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            return True
        except Exception as e:
            print(f"Error creating table: {e}")
            return False
        finally:
            try:
                cursor.close()
            except Exception:
                pass
            conn.close()


if __name__ == "__main__":
    cli_table_name = sys.argv[1].strip() if len(sys.argv) > 1 else ""
    table_name = cli_table_name or os.getenv("SNOWFLAKE_TABLE", "").strip() or "your_table"
    create_sql = os.getenv("SNOWFLAKE_CREATE_TABLE_SQL", "").strip()

    if create_sql:
        sample_query = create_sql
    else:
        sample_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT,
            name STRING,
            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        );
        """
        print("Tip: Set SNOWFLAKE_CREATE_TABLE_SQL in .env for a custom CREATE TABLE statement.")

    success = CreateTableOperations.create_table(sample_query)
    if success:
        if create_sql and not table_name:
            print("Table created (or already exists).")
        else:
            print(f"Table '{table_name}' created (or already exists).")
    else:
        print("Create table failed.")

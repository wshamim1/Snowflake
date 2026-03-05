
import os
import sys
from pathlib import Path
from typing import Any, List, Optional

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.db.snowflake_connection import SnowflakeConnection

class SelectOperations:
    """
    Handles SELECT queries in Snowflake.
    """
    @staticmethod
    def fetch_data(query: str) -> Optional[List[Any]]:
        """
        Executes a SELECT query and returns the results.

        Args:
            query (str): The SQL SELECT query to execute.

        Returns:
            Optional[List[Any]]: The fetched results, or None if an error occurs.
        """
        conn = SnowflakeConnection.get_connection()
        if conn is None:
            print("No connection available.")
            return None
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            return results
        except Exception as e:
            print(f"Error executing query: {e}")
            return None
        finally:
            try:
                cursor.close()
            except Exception:
                pass
            conn.close()

if __name__ == "__main__":
    cli_query = " ".join(sys.argv[1:]).strip()
    env_query = os.getenv("SNOWFLAKE_SELECT_QUERY", "").strip()
    table_name = os.getenv("SNOWFLAKE_TABLE", "").strip()

    if cli_query:
        sample_query = cli_query
    elif env_query:
        sample_query = env_query
    elif table_name:
        sample_query = f"SELECT * FROM {table_name};"
    else:
        sample_query = "SELECT CURRENT_VERSION();"
        print("Tip: Set SNOWFLAKE_SELECT_QUERY in .env or pass a query via CLI.")

    result = SelectOperations.fetch_data(sample_query)
    if result is not None:
        print(result)
    else:
        print("Query failed.")

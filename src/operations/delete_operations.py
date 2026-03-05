
import os
import sys
from pathlib import Path
from typing import Optional

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.db.snowflake_connection import SnowflakeConnection


class DeleteOperations:
    """
    Handles DELETE queries in Snowflake.
    """
    @staticmethod
    def delete_data(query: str) -> bool:
        """
        Executes a DELETE query.

        Args:
            query (str): The SQL DELETE query to execute.

        Returns:
            bool: True if the delete was successful, False otherwise.
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
            print(f"Error executing delete: {e}")
            return False
        finally:
            try:
                cursor.close()
            except Exception:
                pass
            conn.close()

if __name__ == "__main__":
    cli_query = " ".join(sys.argv[1:]).strip()
    env_query = os.getenv("SNOWFLAKE_DELETE_QUERY", "").strip()
    table_name = os.getenv("SNOWFLAKE_TABLE", "").strip()

    if cli_query:
        sample_query = cli_query
    elif env_query:
        sample_query = env_query
    elif table_name:
        print("No delete query provided.")
        print(
            f"Set SNOWFLAKE_DELETE_QUERY in .env, e.g.: DELETE FROM {table_name} WHERE ID = 1;"
        )
        sample_query = ""
    else:
        print("Tip: Set SNOWFLAKE_DELETE_QUERY (or SNOWFLAKE_TABLE) in .env.")
        sample_query = ""

    success = bool(sample_query) and DeleteOperations.delete_data(sample_query)
    if success:
        print("Delete successful.")
    else:
        print("Delete failed.")

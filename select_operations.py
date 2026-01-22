
from typing import Any, List, Optional
from snowflake_connection import SnowflakeConnection

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
    sample_query = "SELECT CURRENT_VERSION();"
    result = SelectOperations.fetch_data(sample_query)
    if result is not None:
        print(result)
    else:
        print("Query failed.")

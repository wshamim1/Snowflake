

from typing import Optional
from snowflake_connection import SnowflakeConnection

class InsertOperations:
    """
    Handles INSERT queries in Snowflake.
    """
    @staticmethod
    def insert_data(query: str) -> bool:
        """
        Executes an INSERT query.

        Args:
            query (str): The SQL INSERT query to execute.

        Returns:
            bool: True if the insert was successful, False otherwise.
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
            print(f"Error executing insert: {e}")
            return False
        finally:
            try:
                cursor.close()
            except Exception:
                pass
            conn.close()

if __name__ == "__main__":
    sample_query = "INSERT INTO your_table (column1, column2) VALUES ('value1', 'value2');"
    success = InsertOperations.insert_data(sample_query)
    if success:
        print("Insert successful.")
    else:
        print("Insert failed.")

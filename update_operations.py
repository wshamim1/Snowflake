
from typing import Optional
from snowflake_connection import SnowflakeConnection

class UpdateOperations:
    """
    Handles UPDATE queries in Snowflake.
    """
    @staticmethod
    def update_data(query: str) -> bool:
        """
        Executes an UPDATE query.

        Args:
            query (str): The SQL UPDATE query to execute.

        Returns:
            bool: True if the update was successful, False otherwise.
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
            print(f"Error executing update: {e}")
            return False
        finally:
            try:
                cursor.close()
            except Exception:
                pass
            conn.close()

if __name__ == "__main__":
    sample_query = "UPDATE your_table SET column1 = 'new_value' WHERE column2 = 'condition';"
    success = UpdateOperations.update_data(sample_query)
    if success:
        print("Update successful.")
    else:
        print("Update failed.")

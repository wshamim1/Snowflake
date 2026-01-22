
from typing import Optional
from snowflake_connection import SnowflakeConnection

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
    sample_query = "DELETE FROM your_table WHERE column2 = 'condition';"
    success = DeleteOperations.delete_data(sample_query)
    if success:
        print("Delete successful.")
    else:
        print("Delete failed.")

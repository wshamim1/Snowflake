

from typing import Optional, List, Dict, Any, Tuple
import pandas as pd
from snowflake_connection import SnowflakeConnection

class InsertOperations:
    """
    Handles INSERT queries in Snowflake with various input formats.
    """
    
    @staticmethod
    def insert_data(query: str, params: Optional[Dict] = None) -> bool:
        """
        Executes an INSERT query.

        Args:
            query (str): The SQL INSERT query to execute.
            params (Optional[Dict]): Query parameters for parameterized queries.

        Returns:
            bool: True if the insert was successful, False otherwise.
        """
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                cursor.close()
                return True
        except Exception as e:
            print(f"Error executing insert: {e}")
            return False

    @staticmethod
    def insert_single_row(
        table_name: str,
        data: Dict[str, Any]
    ) -> bool:
        """
        Inserts a single row into a table.

        Args:
            table_name (str): Name of the table.
            data (Dict[str, Any]): Dictionary with column names as keys and values.

        Returns:
            bool: True if successful, False otherwise.
        """
        columns = ", ".join(data.keys())
        placeholders = ", ".join([f":{key}" for key in data.keys()])
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        return InsertOperations.insert_data(query, data)

    @staticmethod
    def insert_multiple_rows(
        table_name: str,
        data: List[Dict[str, Any]]
    ) -> Tuple[bool, int]:
        """
        Inserts multiple rows into a table using batch insert.

        Args:
            table_name (str): Name of the table.
            data (List[Dict[str, Any]]): List of dictionaries with column names and values.

        Returns:
            Tuple[bool, int]: Success status and number of rows inserted.
        """
        if not data:
            return False, 0
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                
                # Get columns from first row
                columns = ", ".join(data[0].keys())
                placeholders = ", ".join([f":{key}" for key in data[0].keys()])
                query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                
                # Execute batch insert
                cursor.executemany(query, data)
                rows_inserted = cursor.rowcount
                cursor.close()
                
                return True, rows_inserted
        except Exception as e:
            print(f"Error executing batch insert: {e}")
            return False, 0

    @staticmethod
    def insert_from_dataframe(
        table_name: str,
        df: pd.DataFrame,
        chunk_size: int = 10000
    ) -> Tuple[bool, int]:
        """
        Inserts data from a pandas DataFrame into a table.

        Args:
            table_name (str): Name of the table.
            df (pd.DataFrame): DataFrame containing the data.
            chunk_size (int): Number of rows to insert per batch.

        Returns:
            Tuple[bool, int]: Success status and number of rows inserted.
        """
        try:
            # Convert DataFrame to list of dictionaries
            data = df.to_dict('records')
            
            total_inserted = 0
            # Insert in chunks
            for i in range(0, len(data), chunk_size):
                chunk = data[i:i + chunk_size]
                success, rows = InsertOperations.insert_multiple_rows(table_name, chunk)
                if not success:
                    return False, total_inserted
                total_inserted += rows
            
            return True, total_inserted
        except Exception as e:
            print(f"Error inserting from DataFrame: {e}")
            return False, 0

    @staticmethod
    def insert_from_select(
        target_table: str,
        select_query: str
    ) -> Tuple[bool, int]:
        """
        Inserts data into a table from a SELECT query.

        Args:
            target_table (str): Name of the target table.
            select_query (str): SELECT query to get data from.

        Returns:
            Tuple[bool, int]: Success status and number of rows inserted.
        """
        query = f"INSERT INTO {target_table} {select_query}"
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                rows_inserted = cursor.rowcount
                cursor.close()
                return True, rows_inserted
        except Exception as e:
            print(f"Error executing insert from select: {e}")
            return False, 0

    @staticmethod
    def insert_or_ignore(
        table_name: str,
        data: Dict[str, Any],
        unique_columns: List[str]
    ) -> bool:
        """
        Inserts a row only if it doesn't already exist based on unique columns.

        Args:
            table_name (str): Name of the table.
            data (Dict[str, Any]): Dictionary with column names and values.
            unique_columns (List[str]): Columns to check for uniqueness.

        Returns:
            bool: True if successful, False otherwise.
        """
        # Build WHERE clause for checking existence
        where_conditions = " AND ".join([f"{col} = :{col}" for col in unique_columns])
        check_query = f"SELECT COUNT(*) FROM {table_name} WHERE {where_conditions}"
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                
                # Check if row exists
                cursor.execute(check_query, {col: data[col] for col in unique_columns})
                count = cursor.fetchone()[0]
                
                if count == 0:
                    # Row doesn't exist, insert it
                    columns = ", ".join(data.keys())
                    placeholders = ", ".join([f":{key}" for key in data.keys()])
                    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                    cursor.execute(insert_query, data)
                
                cursor.close()
                return True
        except Exception as e:
            print(f"Error in insert_or_ignore: {e}")
            return False

    @staticmethod
    def bulk_insert_with_staging(
        table_name: str,
        data: List[Dict[str, Any]],
        stage_name: str = "@~"
    ) -> Tuple[bool, int]:
        """
        Performs bulk insert using internal stage for better performance.

        Args:
            table_name (str): Name of the target table.
            data (List[Dict[str, Any]]): List of dictionaries with data.
            stage_name (str): Stage to use (default: user stage).

        Returns:
            Tuple[bool, int]: Success status and number of rows inserted.
        """
        try:
            # Convert data to DataFrame
            df = pd.DataFrame(data)
            
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                
                # Use Snowflake's write_pandas for efficient bulk loading
                from snowflake.connector.pandas_tools import write_pandas
                
                success, nchunks, nrows, _ = write_pandas(
                    conn=conn,
                    df=df,
                    table_name=table_name,
                    auto_create_table=False
                )
                
                cursor.close()
                return success, nrows
        except Exception as e:
            print(f"Error in bulk insert with staging: {e}")
            return False, 0

if __name__ == "__main__":
    print("=== Testing Insert Operations ===\n")
    
    # Note: These are example tests. Adjust table names and data as needed.
    
    # Test 1: Single row insert
    print("1. Testing single row insert:")
    # Uncomment and modify for your table:
    # success = InsertOperations.insert_single_row(
    #     "test_table",
    #     {"id": 1, "name": "John Doe", "email": "john@example.com"}
    # )
    # print(f"   Success: {success}\n")
    print("   Skipped (modify table name and data)\n")
    
    # Test 2: Multiple rows insert
    print("2. Testing multiple rows insert:")
    # Uncomment and modify for your table:
    # data = [
    #     {"id": 2, "name": "Jane Smith", "email": "jane@example.com"},
    #     {"id": 3, "name": "Bob Johnson", "email": "bob@example.com"}
    # ]
    # success, count = InsertOperations.insert_multiple_rows("test_table", data)
    # print(f"   Success: {success}, Rows inserted: {count}\n")
    print("   Skipped (modify table name and data)\n")
    
    # Test 3: Insert from DataFrame
    print("3. Testing insert from DataFrame:")
    # Uncomment and modify for your table:
    # df = pd.DataFrame({
    #     "id": [4, 5, 6],
    #     "name": ["Alice", "Charlie", "David"],
    #     "email": ["alice@example.com", "charlie@example.com", "david@example.com"]
    # })
    # success, count = InsertOperations.insert_from_dataframe("test_table", df)
    # print(f"   Success: {success}, Rows inserted: {count}\n")
    print("   Skipped (modify table name and data)\n")
    
    # Test 4: Insert from SELECT
    print("4. Testing insert from SELECT:")
    # Uncomment and modify for your tables:
    # success, count = InsertOperations.insert_from_select(
    #     "target_table",
    #     "SELECT * FROM source_table WHERE condition = true"
    # )
    # print(f"   Success: {success}, Rows inserted: {count}\n")
    print("   Skipped (modify table names)\n")
    
    print("=== Tests Complete ===")
    print("Note: Uncomment and modify the test cases with your actual table names and data.")

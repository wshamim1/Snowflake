
from typing import Any, List, Optional, Dict, Tuple
import pandas as pd
from snowflake_connection import SnowflakeConnection

class SelectOperations:
    """
    Handles SELECT queries in Snowflake with various output formats.
    """
    
    @staticmethod
    def fetch_data(query: str, params: Optional[Dict] = None) -> Optional[List[Tuple]]:
        """
        Executes a SELECT query and returns the results as a list of tuples.

        Args:
            query (str): The SQL SELECT query to execute.
            params (Optional[Dict]): Query parameters for parameterized queries.

        Returns:
            Optional[List[Tuple]]: The fetched results, or None if an error occurs.
        """
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                results = cursor.fetchall()
                cursor.close()
                return results
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

    @staticmethod
    def fetch_one(query: str, params: Optional[Dict] = None) -> Optional[Tuple]:
        """
        Executes a SELECT query and returns the first result.

        Args:
            query (str): The SQL SELECT query to execute.
            params (Optional[Dict]): Query parameters for parameterized queries.

        Returns:
            Optional[Tuple]: The first result, or None if an error occurs.
        """
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                result = cursor.fetchone()
                cursor.close()
                return result
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

    @staticmethod
    def fetch_as_dict(query: str, params: Optional[Dict] = None) -> Optional[List[Dict]]:
        """
        Executes a SELECT query and returns results as a list of dictionaries.

        Args:
            query (str): The SQL SELECT query to execute.
            params (Optional[Dict]): Query parameters for parameterized queries.

        Returns:
            Optional[List[Dict]]: Results as list of dictionaries, or None if error occurs.
        """
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                # Get column names
                columns = [desc[0] for desc in cursor.description]
                
                # Fetch all results and convert to dictionaries
                results = []
                for row in cursor.fetchall():
                    results.append(dict(zip(columns, row)))
                
                cursor.close()
                return results
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

    @staticmethod
    def fetch_as_dataframe(query: str, params: Optional[Dict] = None) -> Optional[pd.DataFrame]:
        """
        Executes a SELECT query and returns results as a pandas DataFrame.

        Args:
            query (str): The SQL SELECT query to execute.
            params (Optional[Dict]): Query parameters for parameterized queries.

        Returns:
            Optional[pd.DataFrame]: Results as DataFrame, or None if error occurs.
        """
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                # Get column names
                columns = [desc[0] for desc in cursor.description]
                
                # Fetch all results
                data = cursor.fetchall()
                cursor.close()
                
                # Create DataFrame
                df = pd.DataFrame(data, columns=columns)
                return df
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

    @staticmethod
    def fetch_with_pagination(
        query: str,
        page_size: int = 1000,
        params: Optional[Dict] = None
    ) -> Optional[List[Tuple]]:
        """
        Executes a SELECT query with pagination for large result sets.

        Args:
            query (str): The SQL SELECT query to execute.
            page_size (int): Number of rows to fetch per batch.
            params (Optional[Dict]): Query parameters for parameterized queries.

        Returns:
            Optional[List[Tuple]]: All results, or None if error occurs.
        """
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                all_results = []
                while True:
                    batch = cursor.fetchmany(page_size)
                    if not batch:
                        break
                    all_results.extend(batch)
                
                cursor.close()
                return all_results
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

    @staticmethod
    def get_table_info(table_name: str) -> Optional[List[Dict]]:
        """
        Gets column information for a table.

        Args:
            table_name (str): Name of the table.

        Returns:
            Optional[List[Dict]]: Column information, or None if error occurs.
        """
        query = f"DESCRIBE TABLE {table_name}"
        return SelectOperations.fetch_as_dict(query)

    @staticmethod
    def get_row_count(table_name: str, where_clause: str = "") -> Optional[int]:
        """
        Gets the row count for a table.

        Args:
            table_name (str): Name of the table.
            where_clause (str): Optional WHERE clause (without WHERE keyword).

        Returns:
            Optional[int]: Row count, or None if error occurs.
        """
        query = f"SELECT COUNT(*) FROM {table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"
        
        result = SelectOperations.fetch_one(query)
        return result[0] if result else None

    @staticmethod
    def execute_query_with_stats(query: str) -> Optional[Dict[str, Any]]:
        """
        Executes a query and returns results with execution statistics.

        Args:
            query (str): The SQL SELECT query to execute.

        Returns:
            Optional[Dict]: Dictionary with results and stats, or None if error occurs.
        """
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                
                # Get query ID for statistics
                query_id = cursor.sfqid
                
                cursor.close()
                
                return {
                    "results": results,
                    "columns": columns,
                    "row_count": len(results),
                    "query_id": query_id
                }
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

if __name__ == "__main__":
    print("=== Testing Select Operations ===\n")
    
    # Test 1: Basic fetch
    print("1. Testing basic fetch:")
    result = SelectOperations.fetch_data("SELECT CURRENT_VERSION()")
    if result:
        print(f"   Snowflake Version: {result[0][0]}\n")
    
    # Test 2: Fetch one
    print("2. Testing fetch one:")
    result = SelectOperations.fetch_one("SELECT CURRENT_USER(), CURRENT_ROLE()")
    if result:
        print(f"   User: {result[0]}, Role: {result[1]}\n")
    
    # Test 3: Fetch as dictionary
    print("3. Testing fetch as dictionary:")
    result = SelectOperations.fetch_as_dict(
        "SELECT CURRENT_DATABASE() as db, CURRENT_SCHEMA() as schema"
    )
    if result:
        print(f"   {result}\n")
    
    # Test 4: Fetch as DataFrame
    print("4. Testing fetch as DataFrame:")
    result = SelectOperations.fetch_as_dataframe(
        "SELECT 'A' as col1, 1 as col2 UNION ALL SELECT 'B', 2"
    )
    if result is not None:
        print(result)
        print()
    
    # Test 5: Query with stats
    print("5. Testing query with stats:")
    result = SelectOperations.execute_query_with_stats(
        "SELECT 1 as num UNION ALL SELECT 2 UNION ALL SELECT 3"
    )
    if result:
        print(f"   Row count: {result['row_count']}")
        print(f"   Query ID: {result['query_id']}\n")
    
    print("=== Tests Complete ===")

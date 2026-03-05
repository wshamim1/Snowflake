"""
Table operations for Snowflake including creation, alteration, and management.
"""

from typing import Optional, Dict, Any, List, Tuple
from snowflake_connection import SnowflakeConnection


class TableOperations:
    """
    Handles table-related operations in Snowflake.
    """
    
    @staticmethod
    def create_table(
        table_name: str,
        columns: Dict[str, str],
        if_not_exists: bool = True
    ) -> bool:
        """
        Creates a new table.

        Args:
            table_name (str): Name of the table.
            columns (Dict[str, str]): Dictionary of column names and their data types.
            if_not_exists (bool): Add IF NOT EXISTS clause.

        Returns:
            bool: True if successful, False otherwise.
        """
        column_defs = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
        exists_clause = "IF NOT EXISTS " if if_not_exists else ""
        query = f"CREATE TABLE {exists_clause}{table_name} ({column_defs})"
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                cursor.close()
                return True
        except Exception as e:
            print(f"Error creating table: {e}")
            return False

    @staticmethod
    def create_table_as_select(
        table_name: str,
        select_query: str,
        replace: bool = False
    ) -> bool:
        """
        Creates a table from a SELECT query (CTAS).

        Args:
            table_name (str): Name of the new table.
            select_query (str): SELECT query to populate the table.
            replace (bool): Use CREATE OR REPLACE.

        Returns:
            bool: True if successful, False otherwise.
        """
        replace_clause = "OR REPLACE " if replace else ""
        query = f"CREATE {replace_clause}TABLE {table_name} AS {select_query}"
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                cursor.close()
                return True
        except Exception as e:
            print(f"Error creating table from select: {e}")
            return False

    @staticmethod
    def drop_table(table_name: str, if_exists: bool = True) -> bool:
        """
        Drops a table.

        Args:
            table_name (str): Name of the table.
            if_exists (bool): Add IF EXISTS clause.

        Returns:
            bool: True if successful, False otherwise.
        """
        exists_clause = "IF EXISTS " if if_exists else ""
        query = f"DROP TABLE {exists_clause}{table_name}"
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                cursor.close()
                return True
        except Exception as e:
            print(f"Error dropping table: {e}")
            return False

    @staticmethod
    def add_column(
        table_name: str,
        column_name: str,
        data_type: str,
        default_value: Optional[Any] = None
    ) -> bool:
        """
        Adds a new column to an existing table.

        Args:
            table_name (str): Name of the table.
            column_name (str): Name of the new column.
            data_type (str): Data type of the new column.
            default_value (Optional[Any]): Default value for the column.

        Returns:
            bool: True if successful, False otherwise.
        """
        query = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {data_type}"
        if default_value is not None:
            if isinstance(default_value, str):
                query += f" DEFAULT '{default_value}'"
            else:
                query += f" DEFAULT {default_value}"
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                cursor.close()
                return True
        except Exception as e:
            print(f"Error adding column: {e}")
            return False

    @staticmethod
    def drop_column(table_name: str, column_name: str) -> bool:
        """
        Drops a column from a table.

        Args:
            table_name (str): Name of the table.
            column_name (str): Name of the column to drop.

        Returns:
            bool: True if successful, False otherwise.
        """
        query = f"ALTER TABLE {table_name} DROP COLUMN {column_name}"
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                cursor.close()
                return True
        except Exception as e:
            print(f"Error dropping column: {e}")
            return False

    @staticmethod
    def rename_column(
        table_name: str,
        old_name: str,
        new_name: str
    ) -> bool:
        """
        Renames a column in a table.

        Args:
            table_name (str): Name of the table.
            old_name (str): Current column name.
            new_name (str): New column name.

        Returns:
            bool: True if successful, False otherwise.
        """
        query = f"ALTER TABLE {table_name} RENAME COLUMN {old_name} TO {new_name}"
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                cursor.close()
                return True
        except Exception as e:
            print(f"Error renaming column: {e}")
            return False

    @staticmethod
    def rename_table(old_name: str, new_name: str) -> bool:
        """
        Renames a table.

        Args:
            old_name (str): Current table name.
            new_name (str): New table name.

        Returns:
            bool: True if successful, False otherwise.
        """
        query = f"ALTER TABLE {old_name} RENAME TO {new_name}"
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                cursor.close()
                return True
        except Exception as e:
            print(f"Error renaming table: {e}")
            return False

    @staticmethod
    def clone_table(
        source_table: str,
        target_table: str,
        at_timestamp: Optional[str] = None
    ) -> bool:
        """
        Clones a table using Snowflake's zero-copy cloning.

        Args:
            source_table (str): Name of the source table.
            target_table (str): Name of the target table.
            at_timestamp (Optional[str]): Timestamp for Time Travel clone.

        Returns:
            bool: True if successful, False otherwise.
        """
        query = f"CREATE TABLE {target_table} CLONE {source_table}"
        if at_timestamp:
            query += f" AT(TIMESTAMP => '{at_timestamp}'::TIMESTAMP)"
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                cursor.close()
                return True
        except Exception as e:
            print(f"Error cloning table: {e}")
            return False

    @staticmethod
    def get_table_schema(table_name: str) -> Optional[List[Dict[str, Any]]]:
        """
        Gets the schema information for a table.

        Args:
            table_name (str): Name of the table.

        Returns:
            Optional[List[Dict]]: List of column information dictionaries.
        """
        query = f"DESCRIBE TABLE {table_name}"
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                results = []
                for row in cursor.fetchall():
                    results.append(dict(zip(columns, row)))
                cursor.close()
                return results
        except Exception as e:
            print(f"Error getting table schema: {e}")
            return None

    @staticmethod
    def list_tables(schema_name: Optional[str] = None) -> Optional[List[str]]:
        """
        Lists all tables in the current or specified schema.

        Args:
            schema_name (Optional[str]): Schema name (uses current if not specified).

        Returns:
            Optional[List[str]]: List of table names.
        """
        query = "SHOW TABLES"
        if schema_name:
            query += f" IN SCHEMA {schema_name}"
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                tables = [row[1] for row in cursor.fetchall()]  # Table name is in second column
                cursor.close()
                return tables
        except Exception as e:
            print(f"Error listing tables: {e}")
            return None

    @staticmethod
    def table_exists(table_name: str) -> bool:
        """
        Checks if a table exists.

        Args:
            table_name (str): Name of the table.

        Returns:
            bool: True if table exists, False otherwise.
        """
        query = f"SHOW TABLES LIKE '{table_name}'"
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                result = cursor.fetchall()
                cursor.close()
                return len(result) > 0
        except Exception as e:
            print(f"Error checking table existence: {e}")
            return False

    @staticmethod
    def get_table_stats(table_name: str) -> Optional[Dict[str, Any]]:
        """
        Gets statistics about a table.

        Args:
            table_name (str): Name of the table.

        Returns:
            Optional[Dict]: Dictionary with table statistics.
        """
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                
                # Get row count
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                row_count = cursor.fetchone()[0]
                
                # Get table size
                cursor.execute(f"""
                    SELECT 
                        table_catalog,
                        table_schema,
                        table_name,
                        bytes,
                        row_count
                    FROM information_schema.tables
                    WHERE table_name = '{table_name.upper()}'
                """)
                result = cursor.fetchone()
                
                cursor.close()
                
                if result:
                    return {
                        "database": result[0],
                        "schema": result[1],
                        "table": result[2],
                        "bytes": result[3],
                        "row_count": result[4],
                        "current_row_count": row_count
                    }
                return None
        except Exception as e:
            print(f"Error getting table stats: {e}")
            return None

    @staticmethod
    def add_primary_key(
        table_name: str,
        column_names: List[str],
        constraint_name: Optional[str] = None
    ) -> bool:
        """
        Adds a primary key constraint to a table.

        Args:
            table_name (str): Name of the table.
            column_names (List[str]): List of column names for the primary key.
            constraint_name (Optional[str]): Name for the constraint.

        Returns:
            bool: True if successful, False otherwise.
        """
        columns = ", ".join(column_names)
        constraint = f"CONSTRAINT {constraint_name} " if constraint_name else ""
        query = f"ALTER TABLE {table_name} ADD {constraint}PRIMARY KEY ({columns})"
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                cursor.close()
                return True
        except Exception as e:
            print(f"Error adding primary key: {e}")
            return False


if __name__ == "__main__":
    print("=== Testing Table Operations ===\n")
    
    # Test 1: List tables
    print("1. Testing list tables:")
    tables = TableOperations.list_tables()
    if tables:
        print(f"   Found {len(tables)} tables")
        if tables:
            print(f"   First few: {tables[:5]}\n")
    else:
        print("   No tables found or error occurred\n")
    
    # Test 2: Check if table exists
    print("2. Testing table existence check:")
    # Uncomment and modify:
    # exists = TableOperations.table_exists("test_table")
    # print(f"   Table exists: {exists}\n")
    print("   Skipped (modify table name)\n")
    
    # Test 3: Get table schema
    print("3. Testing get table schema:")
    # Uncomment and modify:
    # schema = TableOperations.get_table_schema("test_table")
    # if schema:
    #     print(f"   Columns: {len(schema)}")
    #     for col in schema[:3]:
    #         print(f"     - {col['name']}: {col['type']}")
    # print()
    print("   Skipped (modify table name)\n")
    
    print("=== Tests Complete ===")
    print("Note: Uncomment and modify the test cases with your actual table names.")

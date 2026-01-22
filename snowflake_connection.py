
import os
from typing import Optional, Dict, Any
from contextlib import contextmanager
import snowflake.connector
from snowflake.connector import SnowflakeConnection as SFConnection
from snowflake.connector.errors import Error as SnowflakeError
from dotenv import load_dotenv

load_dotenv()

class SnowflakeConnection:
    """
    Handles Snowflake database connections using environment variables.
    Supports both direct connections and context manager pattern.
    """
    
    @staticmethod
    def get_connection(
        role: Optional[str] = None,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None
    ) -> Optional[SFConnection]:
        """
        Establishes and returns a Snowflake database connection.

        Args:
            role: Optional role to use (overrides env variable)
            warehouse: Optional warehouse to use (overrides env variable)
            database: Optional database to use (overrides env variable)
            schema: Optional schema to use (overrides env variable)

        Returns:
            Optional[SFConnection]: A Snowflake connection object if successful, None otherwise.
        """
        try:
            conn = snowflake.connector.connect(
                user=os.getenv("SNOWFLAKE_USER"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                warehouse=warehouse or os.getenv("SNOWFLAKE_WAREHOUSE"),
                database=database or os.getenv("SNOWFLAKE_DATABASE"),
                schema=schema or os.getenv("SNOWFLAKE_SCHEMA"),
                role=role or os.getenv("SNOWFLAKE_ROLE"),
            )
            return conn
        except SnowflakeError as e:
            print(f"Snowflake connection error: {e}")
            return None
        except Exception as e:
            print(f"Failed to connect to Snowflake: {e}")
            return None

    @staticmethod
    @contextmanager
    def get_connection_context(
        role: Optional[str] = None,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None
    ):
        """
        Context manager for Snowflake connections.
        Automatically handles connection cleanup.

        Usage:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
        """
        conn = None
        try:
            conn = SnowflakeConnection.get_connection(role, warehouse, database, schema)
            if conn is None:
                raise Exception("Failed to establish connection")
            yield conn
        finally:
            if conn:
                conn.close()

    @staticmethod
    def test_connection() -> Dict[str, Any]:
        """
        Tests the Snowflake connection and returns connection details.

        Returns:
            Dict containing connection status and details
        """
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                
                # Get Snowflake version
                cursor.execute("SELECT CURRENT_VERSION()")
                version = cursor.fetchone()[0]
                
                # Get current user
                cursor.execute("SELECT CURRENT_USER()")
                user = cursor.fetchone()[0]
                
                # Get current role
                cursor.execute("SELECT CURRENT_ROLE()")
                role = cursor.fetchone()[0]
                
                # Get current warehouse
                cursor.execute("SELECT CURRENT_WAREHOUSE()")
                warehouse = cursor.fetchone()[0]
                
                # Get current database
                cursor.execute("SELECT CURRENT_DATABASE()")
                database = cursor.fetchone()[0]
                
                # Get current schema
                cursor.execute("SELECT CURRENT_SCHEMA()")
                schema = cursor.fetchone()[0]
                
                cursor.close()
                
                return {
                    "status": "success",
                    "version": version,
                    "user": user,
                    "role": role,
                    "warehouse": warehouse,
                    "database": database,
                    "schema": schema
                }
        except Exception as e:
            return {
                "status": "failed",
                "error": str(e)
            }

if __name__ == "__main__":
    # Test basic connection
    print("Testing Snowflake connection...")
    connection = SnowflakeConnection.get_connection()
    if connection:
        print("✓ Successfully connected to Snowflake!")
        connection.close()
    else:
        print("✗ Connection to Snowflake failed.")
    
    # Test connection details
    print("\nConnection Details:")
    details = SnowflakeConnection.test_connection()
    if details["status"] == "success":
        print(f"  Version: {details['version']}")
        print(f"  User: {details['user']}")
        print(f"  Role: {details['role']}")
        print(f"  Warehouse: {details['warehouse']}")
        print(f"  Database: {details['database']}")
        print(f"  Schema: {details['schema']}")
    else:
        print(f"  Error: {details['error']}")
    
    # Test context manager
    print("\nTesting context manager...")
    try:
        with SnowflakeConnection.get_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 'Context manager works!' as message")
            result = cursor.fetchone()
            print(f"  {result[0]}")
            cursor.close()
    except Exception as e:
        print(f"  Error: {e}")


import os
from pathlib import Path
from typing import Optional
import snowflake.connector
from snowflake.connector import SnowflakeConnection as SFConnection
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(ROOT_DIR / ".env")

class SnowflakeConnection:
    """
    Handles Snowflake database connections using environment variables.
    """
    @staticmethod
    def get_connection() -> Optional[SFConnection]:
        """
        Establishes and returns a Snowflake database connection.

        Returns:
            Optional[SFConnection]: A Snowflake connection object if successful, None otherwise.
        """
        try:
            conn = snowflake.connector.connect(
                user=os.getenv("SNOWFLAKE_USER"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                database=os.getenv("SNOWFLAKE_DATABASE"),
                schema=os.getenv("SNOWFLAKE_SCHEMA"),
            )
            return conn
        except Exception as e:
            print(f"Failed to connect to Snowflake: {e}")
            return None

if __name__ == "__main__":
    connection = SnowflakeConnection.get_connection()
    if connection:
        print("Successfully connected to Snowflake!")
        connection.close()
    else:
        print("Connection to Snowflake failed.")

"""
Data loading and unloading operations for Snowflake.
Handles COPY INTO, stages, and file formats.
"""

from typing import Optional, Dict, Any, List, Tuple
import pandas as pd
from snowflake_connection import SnowflakeConnection


class DataLoading:
    """
    Handles data loading and unloading operations in Snowflake.
    """
    
    @staticmethod
    def copy_into_table(
        table_name: str,
        stage_name: str,
        file_format: str = "CSV",
        pattern: Optional[str] = None,
        on_error: str = "ABORT_STATEMENT",
        force: bool = False
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Loads data from a stage into a table using COPY INTO.

        Args:
            table_name (str): Target table name.
            stage_name (str): Stage name (e.g., @my_stage or @~/path).
            file_format (str): File format name or type.
            pattern (Optional[str]): File pattern to match.
            on_error (str): Error handling - ABORT_STATEMENT, CONTINUE, SKIP_FILE.
            force (bool): Force reload of files.

        Returns:
            Tuple[bool, Dict]: Success status and load statistics.
        """
        query = f"COPY INTO {table_name} FROM {stage_name}"
        
        if file_format:
            query += f" FILE_FORMAT = (FORMAT_NAME = '{file_format}')"
        
        if pattern:
            query += f" PATTERN = '{pattern}'"
        
        query += f" ON_ERROR = '{on_error}'"
        
        if force:
            query += " FORCE = TRUE"
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                
                # Get load statistics
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                
                stats = {
                    "files_loaded": len(results),
                    "rows_loaded": sum(row[2] for row in results if len(row) > 2),
                    "details": [dict(zip(columns, row)) for row in results]
                }
                
                cursor.close()
                return True, stats
        except Exception as e:
            print(f"Error loading data: {e}")
            return False, {}

    @staticmethod
    def copy_from_table(
        table_name: str,
        stage_name: str,
        file_format: str = "CSV",
        header: bool = True,
        overwrite: bool = True,
        single_file: bool = False
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Unloads data from a table to a stage using COPY INTO.

        Args:
            table_name (str): Source table name.
            stage_name (str): Stage name (e.g., @my_stage or @~/path).
            file_format (str): File format name or type.
            header (bool): Include header row (for CSV).
            overwrite (bool): Overwrite existing files.
            single_file (bool): Create a single file.

        Returns:
            Tuple[bool, Dict]: Success status and unload statistics.
        """
        query = f"COPY INTO {stage_name} FROM {table_name}"
        query += f" FILE_FORMAT = (FORMAT_NAME = '{file_format}'"
        
        if header and file_format.upper() == "CSV":
            query += " HEADER = TRUE"
        
        query += ")"
        
        if overwrite:
            query += " OVERWRITE = TRUE"
        
        if single_file:
            query += " SINGLE = TRUE"
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                
                # Get unload statistics
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                
                stats = {
                    "files_created": len(results),
                    "rows_unloaded": sum(row[1] for row in results if len(row) > 1),
                    "details": [dict(zip(columns, row)) for row in results]
                }
                
                cursor.close()
                return True, stats
        except Exception as e:
            print(f"Error unloading data: {e}")
            return False, {}

    @staticmethod
    def load_csv_from_s3(
        table_name: str,
        s3_path: str,
        aws_key_id: str,
        aws_secret_key: str,
        skip_header: int = 1,
        field_delimiter: str = ",",
        on_error: str = "ABORT_STATEMENT"
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Loads CSV data from S3 directly into a table.

        Args:
            table_name (str): Target table name.
            s3_path (str): S3 path (s3://bucket/path/).
            aws_key_id (str): AWS access key ID.
            aws_secret_key (str): AWS secret access key.
            skip_header (int): Number of header rows to skip.
            field_delimiter (str): Field delimiter character.
            on_error (str): Error handling strategy.

        Returns:
            Tuple[bool, Dict]: Success status and load statistics.
        """
        query = f"""
            COPY INTO {table_name}
            FROM '{s3_path}'
            CREDENTIALS = (
                AWS_KEY_ID = '{aws_key_id}'
                AWS_SECRET_KEY = '{aws_secret_key}'
            )
            FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_DELIMITER = '{field_delimiter}'
                SKIP_HEADER = {skip_header}
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            )
            ON_ERROR = '{on_error}'
        """
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                
                stats = {
                    "files_loaded": len(results),
                    "rows_loaded": sum(row[2] for row in results if len(row) > 2),
                    "details": [dict(zip(columns, row)) for row in results]
                }
                
                cursor.close()
                return True, stats
        except Exception as e:
            print(f"Error loading from S3: {e}")
            return False, {}

    @staticmethod
    def create_stage(
        stage_name: str,
        url: Optional[str] = None,
        credentials: Optional[Dict[str, str]] = None,
        file_format: Optional[str] = None,
        replace: bool = False
    ) -> bool:
        """
        Creates an internal or external stage.

        Args:
            stage_name (str): Name of the stage.
            url (Optional[str]): URL for external stage (s3://, gcs://, azure://).
            credentials (Optional[Dict]): Credentials for external stage.
            file_format (Optional[str]): Default file format for the stage.
            replace (bool): Use CREATE OR REPLACE.

        Returns:
            bool: True if successful, False otherwise.
        """
        replace_clause = "OR REPLACE " if replace else ""
        query = f"CREATE {replace_clause}STAGE {stage_name}"
        
        if url:
            query += f" URL = '{url}'"
        
        if credentials:
            cred_parts = [f"{k} = '{v}'" for k, v in credentials.items()]
            query += f" CREDENTIALS = ({' '.join(cred_parts)})"
        
        if file_format:
            query += f" FILE_FORMAT = (FORMAT_NAME = '{file_format}')"
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                cursor.close()
                return True
        except Exception as e:
            print(f"Error creating stage: {e}")
            return False

    @staticmethod
    def list_stage_files(stage_name: str, pattern: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
        """
        Lists files in a stage.

        Args:
            stage_name (str): Name of the stage.
            pattern (Optional[str]): File pattern to filter.

        Returns:
            Optional[List[Dict]]: List of file information dictionaries.
        """
        query = f"LIST {stage_name}"
        if pattern:
            query += f" PATTERN = '{pattern}'"
        
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
            print(f"Error listing stage files: {e}")
            return None

    @staticmethod
    def remove_stage_files(
        stage_name: str,
        pattern: Optional[str] = None
    ) -> Tuple[bool, int]:
        """
        Removes files from a stage.

        Args:
            stage_name (str): Name of the stage.
            pattern (Optional[str]): File pattern to remove.

        Returns:
            Tuple[bool, int]: Success status and number of files removed.
        """
        query = f"REMOVE {stage_name}"
        if pattern:
            query += f" PATTERN = '{pattern}'"
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                results = cursor.fetchall()
                cursor.close()
                return True, len(results)
        except Exception as e:
            print(f"Error removing stage files: {e}")
            return False, 0

    @staticmethod
    def create_file_format(
        format_name: str,
        format_type: str = "CSV",
        options: Optional[Dict[str, Any]] = None,
        replace: bool = False
    ) -> bool:
        """
        Creates a file format.

        Args:
            format_name (str): Name of the file format.
            format_type (str): Type (CSV, JSON, PARQUET, etc.).
            options (Optional[Dict]): Format-specific options.
            replace (bool): Use CREATE OR REPLACE.

        Returns:
            bool: True if successful, False otherwise.
        """
        replace_clause = "OR REPLACE " if replace else ""
        query = f"CREATE {replace_clause}FILE FORMAT {format_name} TYPE = '{format_type}'"
        
        if options:
            option_parts = []
            for key, value in options.items():
                if isinstance(value, str):
                    option_parts.append(f"{key} = '{value}'")
                elif isinstance(value, bool):
                    option_parts.append(f"{key} = {str(value).upper()}")
                else:
                    option_parts.append(f"{key} = {value}")
            
            if option_parts:
                query += " " + " ".join(option_parts)
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                cursor.close()
                return True
        except Exception as e:
            print(f"Error creating file format: {e}")
            return False

    @staticmethod
    def validate_data_load(
        table_name: str,
        stage_name: str,
        file_format: str = "CSV"
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Validates data load without actually loading it.

        Args:
            table_name (str): Target table name.
            stage_name (str): Stage name.
            file_format (str): File format name.

        Returns:
            Optional[List[Dict]]: List of validation errors, if any.
        """
        query = f"""
            COPY INTO {table_name}
            FROM {stage_name}
            FILE_FORMAT = (FORMAT_NAME = '{file_format}')
            VALIDATION_MODE = 'RETURN_ERRORS'
        """
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                
                columns = [desc[0] for desc in cursor.description]
                errors = []
                for row in cursor.fetchall():
                    errors.append(dict(zip(columns, row)))
                
                cursor.close()
                return errors
        except Exception as e:
            print(f"Error validating data load: {e}")
            return None

    @staticmethod
    def load_dataframe_to_table(
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "append",
        chunk_size: int = 10000
    ) -> Tuple[bool, int]:
        """
        Loads a pandas DataFrame directly into a Snowflake table.

        Args:
            df (pd.DataFrame): DataFrame to load.
            table_name (str): Target table name.
            if_exists (str): Action if table exists - 'fail', 'replace', 'append'.
            chunk_size (int): Number of rows per chunk.

        Returns:
            Tuple[bool, int]: Success status and number of rows loaded.
        """
        try:
            from snowflake.connector.pandas_tools import write_pandas
            
            with SnowflakeConnection.get_connection_context() as conn:
                success, nchunks, nrows, _ = write_pandas(
                    conn=conn,
                    df=df,
                    table_name=table_name,
                    auto_create_table=(if_exists == "replace"),
                    overwrite=(if_exists == "replace"),
                    chunk_size=chunk_size
                )
                
                return success, nrows
        except Exception as e:
            print(f"Error loading DataFrame: {e}")
            return False, 0

    @staticmethod
    def get_copy_history(
        table_name: str,
        days: int = 7
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Gets the copy history for a table.

        Args:
            table_name (str): Table name.
            days (int): Number of days to look back.

        Returns:
            Optional[List[Dict]]: List of copy operations.
        """
        query = f"""
            SELECT *
            FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
                TABLE_NAME => '{table_name}',
                START_TIME => DATEADD('day', -{days}, CURRENT_TIMESTAMP())
            ))
            ORDER BY LAST_LOAD_TIME DESC
        """
        
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
            print(f"Error getting copy history: {e}")
            return None


if __name__ == "__main__":
    print("=== Testing Data Loading Operations ===\n")
    
    # Test 1: List stage files
    print("1. Testing list stage files:")
    # Uncomment and modify:
    # files = DataLoading.list_stage_files("@my_stage")
    # if files:
    #     print(f"   Found {len(files)} files")
    #     for f in files[:3]:
    #         print(f"     - {f['name']}")
    # print()
    print("   Skipped (modify stage name)\n")
    
    # Test 2: Create file format
    print("2. Testing create file format:")
    # Uncomment and modify:
    # success = DataLoading.create_file_format(
    #     "my_csv_format",
    #     "CSV",
    #     {
    #         "FIELD_DELIMITER": ",",
    #         "SKIP_HEADER": 1,
    #         "FIELD_OPTIONALLY_ENCLOSED_BY": '"'
    #     },
    #     replace=True
    # )
    # print(f"   Success: {success}\n")
    print("   Skipped (modify format name)\n")
    
    # Test 3: Load DataFrame
    print("3. Testing load DataFrame:")
    # Uncomment and modify:
    # df = pd.DataFrame({
    #     "id": [1, 2, 3],
    #     "name": ["Alice", "Bob", "Charlie"]
    # })
    # success, rows = DataLoading.load_dataframe_to_table(df, "test_table")
    # print(f"   Success: {success}, Rows loaded: {rows}\n")
    print("   Skipped (modify table name)\n")
    
    print("=== Tests Complete ===")
    print("Note: Uncomment and modify the test cases with your actual stage/table names.")


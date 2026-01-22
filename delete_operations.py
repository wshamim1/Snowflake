
from typing import Optional, Dict, Any, Tuple, List
from snowflake_connection import SnowflakeConnection

class DeleteOperations:
    """
    Handles DELETE queries in Snowflake with various deletion patterns.
    """
    
    @staticmethod
    def delete_data(query: str, params: Optional[Dict] = None) -> Tuple[bool, int]:
        """
        Executes a DELETE query.

        Args:
            query (str): The SQL DELETE query to execute.
            params (Optional[Dict]): Query parameters for parameterized queries.

        Returns:
            Tuple[bool, int]: Success status and number of rows deleted.
        """
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                rows_deleted = cursor.rowcount
                cursor.close()
                return True, rows_deleted
        except Exception as e:
            print(f"Error executing delete: {e}")
            return False, 0

    @staticmethod
    def delete_by_condition(
        table_name: str,
        conditions: Dict[str, Any]
    ) -> Tuple[bool, int]:
        """
        Deletes rows matching specific conditions.

        Args:
            table_name (str): Name of the table.
            conditions (Dict[str, Any]): Conditions for WHERE clause.

        Returns:
            Tuple[bool, int]: Success status and number of rows deleted.
        """
        where_clause = " AND ".join([f"{col} = :{col}" for col in conditions.keys()])
        query = f"DELETE FROM {table_name} WHERE {where_clause}"
        
        return DeleteOperations.delete_data(query, conditions)

    @staticmethod
    def delete_by_id(
        table_name: str,
        id_column: str,
        id_value: Any
    ) -> Tuple[bool, int]:
        """
        Deletes a row by its ID.

        Args:
            table_name (str): Name of the table.
            id_column (str): Name of the ID column.
            id_value (Any): Value of the ID to delete.

        Returns:
            Tuple[bool, int]: Success status and number of rows deleted.
        """
        return DeleteOperations.delete_by_condition(
            table_name,
            {id_column: id_value}
        )

    @staticmethod
    def delete_by_ids(
        table_name: str,
        id_column: str,
        id_values: List[Any]
    ) -> Tuple[bool, int]:
        """
        Deletes multiple rows by their IDs.

        Args:
            table_name (str): Name of the table.
            id_column (str): Name of the ID column.
            id_values (List[Any]): List of ID values to delete.

        Returns:
            Tuple[bool, int]: Success status and number of rows deleted.
        """
        if not id_values:
            return True, 0
        
        # Build IN clause
        placeholders = ", ".join([f":id_{i}" for i in range(len(id_values))])
        query = f"DELETE FROM {table_name} WHERE {id_column} IN ({placeholders})"
        
        # Create parameters dictionary
        params = {f"id_{i}": val for i, val in enumerate(id_values)}
        
        return DeleteOperations.delete_data(query, params)

    @staticmethod
    def delete_with_custom_where(
        table_name: str,
        where_clause: str,
        params: Optional[Dict] = None
    ) -> Tuple[bool, int]:
        """
        Deletes rows with a custom WHERE clause.

        Args:
            table_name (str): Name of the table.
            where_clause (str): Custom WHERE clause (without WHERE keyword).
            params (Optional[Dict]): Parameters for the WHERE clause.

        Returns:
            Tuple[bool, int]: Success status and number of rows deleted.
        """
        query = f"DELETE FROM {table_name} WHERE {where_clause}"
        return DeleteOperations.delete_data(query, params)

    @staticmethod
    def delete_old_records(
        table_name: str,
        date_column: str,
        days_old: int
    ) -> Tuple[bool, int]:
        """
        Deletes records older than specified number of days.

        Args:
            table_name (str): Name of the table.
            date_column (str): Name of the date/timestamp column.
            days_old (int): Number of days to keep (delete older records).

        Returns:
            Tuple[bool, int]: Success status and number of rows deleted.
        """
        query = f"""
            DELETE FROM {table_name}
            WHERE {date_column} < DATEADD('day', -{days_old}, CURRENT_TIMESTAMP())
        """
        return DeleteOperations.delete_data(query)

    @staticmethod
    def delete_duplicates(
        table_name: str,
        unique_columns: List[str],
        keep: str = "first"
    ) -> Tuple[bool, int]:
        """
        Deletes duplicate rows, keeping only one based on criteria.

        Args:
            table_name (str): Name of the table.
            unique_columns (List[str]): Columns that define uniqueness.
            keep (str): Which duplicate to keep - "first" or "last" (based on insertion order).

        Returns:
            Tuple[bool, int]: Success status and number of rows deleted.
        """
        partition_by = ", ".join(unique_columns)
        order_direction = "ASC" if keep == "first" else "DESC"
        
        query = f"""
            DELETE FROM {table_name}
            WHERE ROWID NOT IN (
                SELECT ROWID FROM (
                    SELECT ROWID,
                           ROW_NUMBER() OVER (PARTITION BY {partition_by} ORDER BY ROWID {order_direction}) as rn
                    FROM {table_name}
                )
                WHERE rn = 1
            )
        """
        
        return DeleteOperations.delete_data(query)

    @staticmethod
    def delete_using_subquery(
        table_name: str,
        subquery: str,
        join_column: str
    ) -> Tuple[bool, int]:
        """
        Deletes rows based on a subquery result.

        Args:
            table_name (str): Name of the table.
            subquery (str): Subquery that returns IDs to delete.
            join_column (str): Column to join on.

        Returns:
            Tuple[bool, int]: Success status and number of rows deleted.
        """
        query = f"""
            DELETE FROM {table_name}
            WHERE {join_column} IN ({subquery})
        """
        return DeleteOperations.delete_data(query)

    @staticmethod
    def delete_with_join(
        target_table: str,
        source_table: str,
        join_condition: str
    ) -> Tuple[bool, int]:
        """
        Deletes rows from target table based on join with another table.

        Args:
            target_table (str): Name of the table to delete from.
            source_table (str): Name of the table to join with.
            join_condition (str): JOIN condition.

        Returns:
            Tuple[bool, int]: Success status and number of rows deleted.
        """
        query = f"""
            DELETE FROM {target_table}
            USING {source_table}
            WHERE {join_condition}
        """
        return DeleteOperations.delete_data(query)

    @staticmethod
    def truncate_table(table_name: str) -> Tuple[bool, int]:
        """
        Truncates a table (deletes all rows efficiently).

        Args:
            table_name (str): Name of the table.

        Returns:
            Tuple[bool, int]: Success status (row count will be 0 for TRUNCATE).
        """
        query = f"TRUNCATE TABLE {table_name}"
        return DeleteOperations.delete_data(query)

    @staticmethod
    def soft_delete(
        table_name: str,
        conditions: Dict[str, Any],
        deleted_column: str = "is_deleted",
        deleted_value: Any = True
    ) -> Tuple[bool, int]:
        """
        Performs a soft delete by setting a flag instead of actually deleting.

        Args:
            table_name (str): Name of the table.
            conditions (Dict[str, Any]): Conditions for which rows to soft delete.
            deleted_column (str): Name of the column that marks deletion.
            deleted_value (Any): Value to set for deleted rows.

        Returns:
            Tuple[bool, int]: Success status and number of rows soft deleted.
        """
        where_clause = " AND ".join([f"{col} = :{col}" for col in conditions.keys()])
        query = f"UPDATE {table_name} SET {deleted_column} = :deleted_value WHERE {where_clause}"
        
        params = {**conditions, "deleted_value": deleted_value}
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                rows_updated = cursor.rowcount
                cursor.close()
                return True, rows_updated
        except Exception as e:
            print(f"Error executing soft delete: {e}")
            return False, 0

    @staticmethod
    def delete_with_limit(
        table_name: str,
        where_clause: str,
        limit: int,
        params: Optional[Dict] = None
    ) -> Tuple[bool, int]:
        """
        Deletes a limited number of rows matching conditions.

        Args:
            table_name (str): Name of the table.
            where_clause (str): WHERE clause (without WHERE keyword).
            limit (int): Maximum number of rows to delete.
            params (Optional[Dict]): Parameters for the WHERE clause.

        Returns:
            Tuple[bool, int]: Success status and number of rows deleted.
        """
        # Snowflake doesn't support DELETE with LIMIT directly
        # Use a subquery with LIMIT
        query = f"""
            DELETE FROM {table_name}
            WHERE ROWID IN (
                SELECT ROWID FROM {table_name}
                WHERE {where_clause}
                LIMIT {limit}
            )
        """
        return DeleteOperations.delete_data(query, params)

    @staticmethod
    def batch_delete(
        table_name: str,
        id_column: str,
        id_values: List[Any],
        batch_size: int = 1000
    ) -> Tuple[bool, int]:
        """
        Deletes rows in batches for better performance with large datasets.

        Args:
            table_name (str): Name of the table.
            id_column (str): Name of the ID column.
            id_values (List[Any]): List of ID values to delete.
            batch_size (int): Number of IDs to delete per batch.

        Returns:
            Tuple[bool, int]: Success status and total number of rows deleted.
        """
        total_deleted = 0
        
        try:
            for i in range(0, len(id_values), batch_size):
                batch = id_values[i:i + batch_size]
                success, count = DeleteOperations.delete_by_ids(table_name, id_column, batch)
                
                if not success:
                    return False, total_deleted
                
                total_deleted += count
            
            return True, total_deleted
        except Exception as e:
            print(f"Error in batch delete: {e}")
            return False, total_deleted

if __name__ == "__main__":
    print("=== Testing Delete Operations ===\n")
    
    # Note: These are example tests. Adjust table names and data as needed.
    
    # Test 1: Delete by ID
    print("1. Testing delete by ID:")
    # Uncomment and modify for your table:
    # success, count = DeleteOperations.delete_by_id("test_table", "id", 1)
    # print(f"   Success: {success}, Rows deleted: {count}\n")
    print("   Skipped (modify table name and data)\n")
    
    # Test 2: Delete by multiple IDs
    print("2. Testing delete by multiple IDs:")
    # Uncomment and modify for your table:
    # success, count = DeleteOperations.delete_by_ids("test_table", "id", [2, 3, 4])
    # print(f"   Success: {success}, Rows deleted: {count}\n")
    print("   Skipped (modify table name and data)\n")
    
    # Test 3: Delete old records
    print("3. Testing delete old records:")
    # Uncomment and modify for your table:
    # success, count = DeleteOperations.delete_old_records(
    #     "test_table",
    #     "created_at",
    #     days_old=30
    # )
    # print(f"   Success: {success}, Rows deleted: {count}\n")
    print("   Skipped (modify table name and data)\n")
    
    # Test 4: Soft delete
    print("4. Testing soft delete:")
    # Uncomment and modify for your table:
    # success, count = DeleteOperations.soft_delete(
    #     "test_table",
    #     {"id": 5},
    #     deleted_column="is_deleted",
    #     deleted_value=True
    # )
    # print(f"   Success: {success}, Rows soft deleted: {count}\n")
    print("   Skipped (modify table name and data)\n")
    
    # Test 5: Delete with custom WHERE
    print("5. Testing delete with custom WHERE:")
    # Uncomment and modify for your table:
    # success, count = DeleteOperations.delete_with_custom_where(
    #     "test_table",
    #     "status = :status AND created_at < :date",
    #     {"status": "inactive", "date": "2024-01-01"}
    # )
    # print(f"   Success: {success}, Rows deleted: {count}\n")
    print("   Skipped (modify table name and data)\n")
    
    print("=== Tests Complete ===")
    print("Note: Uncomment and modify the test cases with your actual table names and data.")

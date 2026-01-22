
from typing import Optional, Dict, Any, Tuple, List
from snowflake_connection import SnowflakeConnection

class UpdateOperations:
    """
    Handles UPDATE queries in Snowflake with various update patterns.
    """
    
    @staticmethod
    def update_data(query: str, params: Optional[Dict] = None) -> Tuple[bool, int]:
        """
        Executes an UPDATE query.

        Args:
            query (str): The SQL UPDATE query to execute.
            params (Optional[Dict]): Query parameters for parameterized queries.

        Returns:
            Tuple[bool, int]: Success status and number of rows updated.
        """
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                rows_updated = cursor.rowcount
                cursor.close()
                return True, rows_updated
        except Exception as e:
            print(f"Error executing update: {e}")
            return False, 0

    @staticmethod
    def update_single_row(
        table_name: str,
        set_values: Dict[str, Any],
        where_conditions: Dict[str, Any]
    ) -> Tuple[bool, int]:
        """
        Updates a single row or rows matching conditions.

        Args:
            table_name (str): Name of the table.
            set_values (Dict[str, Any]): Columns to update with new values.
            where_conditions (Dict[str, Any]): Conditions for WHERE clause.

        Returns:
            Tuple[bool, int]: Success status and number of rows updated.
        """
        # Build SET clause
        set_clause = ", ".join([f"{col} = :{col}" for col in set_values.keys()])
        
        # Build WHERE clause
        where_clause = " AND ".join([f"{col} = :where_{col}" for col in where_conditions.keys()])
        
        query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
        
        # Combine parameters
        params = {**set_values}
        params.update({f"where_{k}": v for k, v in where_conditions.items()})
        
        return UpdateOperations.update_data(query, params)

    @staticmethod
    def update_multiple_columns(
        table_name: str,
        updates: Dict[str, Any],
        where_clause: str,
        where_params: Optional[Dict] = None
    ) -> Tuple[bool, int]:
        """
        Updates multiple columns with a custom WHERE clause.

        Args:
            table_name (str): Name of the table.
            updates (Dict[str, Any]): Columns to update with new values.
            where_clause (str): Custom WHERE clause (without WHERE keyword).
            where_params (Optional[Dict]): Parameters for WHERE clause.

        Returns:
            Tuple[bool, int]: Success status and number of rows updated.
        """
        set_clause = ", ".join([f"{col} = :{col}" for col in updates.keys()])
        query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
        
        params = {**updates}
        if where_params:
            params.update(where_params)
        
        return UpdateOperations.update_data(query, params)

    @staticmethod
    def update_with_case(
        table_name: str,
        column_to_update: str,
        case_conditions: List[Tuple[str, Any]],
        else_value: Any,
        where_clause: str = ""
    ) -> Tuple[bool, int]:
        """
        Updates a column using CASE statement for conditional updates.

        Args:
            table_name (str): Name of the table.
            column_to_update (str): Column to update.
            case_conditions (List[Tuple[str, Any]]): List of (condition, value) tuples.
            else_value (Any): Default value if no conditions match.
            where_clause (str): Optional WHERE clause.

        Returns:
            Tuple[bool, int]: Success status and number of rows updated.
        """
        # Build CASE statement
        case_parts = []
        for condition, value in case_conditions:
            if isinstance(value, str):
                case_parts.append(f"WHEN {condition} THEN '{value}'")
            else:
                case_parts.append(f"WHEN {condition} THEN {value}")
        
        if isinstance(else_value, str):
            else_part = f"ELSE '{else_value}'"
        else:
            else_part = f"ELSE {else_value}"
        
        case_statement = f"CASE {' '.join(case_parts)} {else_part} END"
        
        query = f"UPDATE {table_name} SET {column_to_update} = {case_statement}"
        if where_clause:
            query += f" WHERE {where_clause}"
        
        return UpdateOperations.update_data(query)

    @staticmethod
    def update_from_another_table(
        target_table: str,
        source_table: str,
        set_columns: Dict[str, str],
        join_condition: str
    ) -> Tuple[bool, int]:
        """
        Updates a table using data from another table.

        Args:
            target_table (str): Name of the target table.
            source_table (str): Name of the source table.
            set_columns (Dict[str, str]): Mapping of target columns to source columns.
            join_condition (str): JOIN condition between tables.

        Returns:
            Tuple[bool, int]: Success status and number of rows updated.
        """
        # Build SET clause
        set_clause = ", ".join([
            f"{target_table}.{target_col} = {source_table}.{source_col}"
            for target_col, source_col in set_columns.items()
        ])
        
        query = f"""
            UPDATE {target_table}
            SET {set_clause}
            FROM {source_table}
            WHERE {join_condition}
        """
        
        return UpdateOperations.update_data(query)

    @staticmethod
    def increment_column(
        table_name: str,
        column_name: str,
        increment_by: float = 1,
        where_clause: str = "",
        where_params: Optional[Dict] = None
    ) -> Tuple[bool, int]:
        """
        Increments a numeric column by a specified value.

        Args:
            table_name (str): Name of the table.
            column_name (str): Column to increment.
            increment_by (float): Value to increment by.
            where_clause (str): Optional WHERE clause.
            where_params (Optional[Dict]): Parameters for WHERE clause.

        Returns:
            Tuple[bool, int]: Success status and number of rows updated.
        """
        query = f"UPDATE {table_name} SET {column_name} = {column_name} + {increment_by}"
        if where_clause:
            query += f" WHERE {where_clause}"
        
        return UpdateOperations.update_data(query, where_params)

    @staticmethod
    def update_with_subquery(
        table_name: str,
        column_to_update: str,
        subquery: str,
        where_clause: str = ""
    ) -> Tuple[bool, int]:
        """
        Updates a column using a subquery.

        Args:
            table_name (str): Name of the table.
            column_to_update (str): Column to update.
            subquery (str): Subquery to get the new value.
            where_clause (str): Optional WHERE clause.

        Returns:
            Tuple[bool, int]: Success status and number of rows updated.
        """
        query = f"UPDATE {table_name} SET {column_to_update} = ({subquery})"
        if where_clause:
            query += f" WHERE {where_clause}"
        
        return UpdateOperations.update_data(query)

    @staticmethod
    def batch_update(
        table_name: str,
        updates: List[Dict[str, Any]],
        key_column: str
    ) -> Tuple[bool, int]:
        """
        Performs batch updates for multiple rows.

        Args:
            table_name (str): Name of the table.
            updates (List[Dict[str, Any]]): List of dictionaries with updates.
                Each dict must include the key_column.
            key_column (str): Column to use as the key for updates.

        Returns:
            Tuple[bool, int]: Success status and total number of rows updated.
        """
        total_updated = 0
        
        try:
            for update_data in updates:
                if key_column not in update_data:
                    print(f"Warning: {key_column} not found in update data, skipping")
                    continue
                
                key_value = update_data[key_column]
                set_values = {k: v for k, v in update_data.items() if k != key_column}
                
                success, count = UpdateOperations.update_single_row(
                    table_name,
                    set_values,
                    {key_column: key_value}
                )
                
                if success:
                    total_updated += count
                else:
                    return False, total_updated
            
            return True, total_updated
        except Exception as e:
            print(f"Error in batch update: {e}")
            return False, total_updated

    @staticmethod
    def upsert(
        table_name: str,
        data: Dict[str, Any],
        key_columns: List[str]
    ) -> Tuple[bool, int]:
        """
        Performs an upsert (update if exists, insert if not) using MERGE.

        Args:
            table_name (str): Name of the table.
            data (Dict[str, Any]): Data to upsert.
            key_columns (List[str]): Columns to use for matching.

        Returns:
            Tuple[bool, int]: Success status and number of rows affected.
        """
        # Build ON clause
        on_conditions = " AND ".join([f"target.{col} = :{col}" for col in key_columns])
        
        # Build UPDATE SET clause (exclude key columns)
        update_columns = [col for col in data.keys() if col not in key_columns]
        update_set = ", ".join([f"{col} = :{col}" for col in update_columns])
        
        # Build INSERT clause
        all_columns = ", ".join(data.keys())
        all_values = ", ".join([f":{col}" for col in data.keys()])
        
        query = f"""
            MERGE INTO {table_name} AS target
            USING (SELECT {', '.join([f":{col} as {col}" for col in data.keys()])}) AS source
            ON {on_conditions}
            WHEN MATCHED THEN
                UPDATE SET {update_set}
            WHEN NOT MATCHED THEN
                INSERT ({all_columns}) VALUES ({all_values})
        """
        
        return UpdateOperations.update_data(query, data)

if __name__ == "__main__":
    print("=== Testing Update Operations ===\n")
    
    # Note: These are example tests. Adjust table names and data as needed.
    
    # Test 1: Simple update
    print("1. Testing simple update:")
    # Uncomment and modify for your table:
    # success, count = UpdateOperations.update_single_row(
    #     "test_table",
    #     {"name": "Updated Name"},
    #     {"id": 1}
    # )
    # print(f"   Success: {success}, Rows updated: {count}\n")
    print("   Skipped (modify table name and data)\n")
    
    # Test 2: Update multiple columns
    print("2. Testing update multiple columns:")
    # Uncomment and modify for your table:
    # success, count = UpdateOperations.update_multiple_columns(
    #     "test_table",
    #     {"name": "New Name", "email": "new@example.com"},
    #     "id > :min_id",
    #     {"min_id": 5}
    # )
    # print(f"   Success: {success}, Rows updated: {count}\n")
    print("   Skipped (modify table name and data)\n")
    
    # Test 3: Increment column
    print("3. Testing increment column:")
    # Uncomment and modify for your table:
    # success, count = UpdateOperations.increment_column(
    #     "test_table",
    #     "view_count",
    #     increment_by=1,
    #     where_clause="id = :id",
    #     where_params={"id": 1}
    # )
    # print(f"   Success: {success}, Rows updated: {count}\n")
    print("   Skipped (modify table name and data)\n")
    
    # Test 4: Batch update
    print("4. Testing batch update:")
    # Uncomment and modify for your table:
    # updates = [
    #     {"id": 1, "status": "active"},
    #     {"id": 2, "status": "inactive"},
    #     {"id": 3, "status": "pending"}
    # ]
    # success, count = UpdateOperations.batch_update("test_table", updates, "id")
    # print(f"   Success: {success}, Total rows updated: {count}\n")
    print("   Skipped (modify table name and data)\n")
    
    # Test 5: Upsert
    print("5. Testing upsert:")
    # Uncomment and modify for your table:
    # success, count = UpdateOperations.upsert(
    #     "test_table",
    #     {"id": 100, "name": "New User", "email": "newuser@example.com"},
    #     ["id"]
    # )
    # print(f"   Success: {success}, Rows affected: {count}\n")
    print("   Skipped (modify table name and data)\n")
    
    print("=== Tests Complete ===")
    print("Note: Uncomment and modify the test cases with your actual table names and data.")

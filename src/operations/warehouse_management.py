"""
Warehouse management operations for Snowflake.
"""

from typing import Optional, Dict, Any, List
from snowflake_connection import SnowflakeConnection


class WarehouseManagement:
    """
    Handles warehouse-related operations in Snowflake.
    """
    
    @staticmethod
    def create_warehouse(
        warehouse_name: str,
        size: str = "XSMALL",
        auto_suspend: int = 60,
        auto_resume: bool = True,
        initially_suspended: bool = True,
        max_cluster_count: int = 1,
        min_cluster_count: int = 1,
        scaling_policy: str = "STANDARD",
        replace: bool = False
    ) -> bool:
        """
        Creates a new warehouse.

        Args:
            warehouse_name (str): Name of the warehouse.
            size (str): Warehouse size (XSMALL, SMALL, MEDIUM, LARGE, etc.).
            auto_suspend (int): Auto-suspend time in seconds.
            auto_resume (bool): Enable auto-resume.
            initially_suspended (bool): Start in suspended state.
            max_cluster_count (int): Maximum number of clusters.
            min_cluster_count (int): Minimum number of clusters.
            scaling_policy (str): Scaling policy (STANDARD or ECONOMY).
            replace (bool): Use CREATE OR REPLACE.

        Returns:
            bool: True if successful, False otherwise.
        """
        replace_clause = "OR REPLACE " if replace else ""
        query = f"""
            CREATE {replace_clause}WAREHOUSE {warehouse_name}
            WITH
                WAREHOUSE_SIZE = '{size}'
                AUTO_SUSPEND = {auto_suspend}
                AUTO_RESUME = {str(auto_resume).upper()}
                INITIALLY_SUSPENDED = {str(initially_suspended).upper()}
                MAX_CLUSTER_COUNT = {max_cluster_count}
                MIN_CLUSTER_COUNT = {min_cluster_count}
                SCALING_POLICY = '{scaling_policy}'
        """
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                cursor.close()
                return True
        except Exception as e:
            print(f"Error creating warehouse: {e}")
            return False

    @staticmethod
    def alter_warehouse_size(warehouse_name: str, new_size: str) -> bool:
        """
        Changes the size of a warehouse.

        Args:
            warehouse_name (str): Name of the warehouse.
            new_size (str): New warehouse size.

        Returns:
            bool: True if successful, False otherwise.
        """
        query = f"ALTER WAREHOUSE {warehouse_name} SET WAREHOUSE_SIZE = '{new_size}'"
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                cursor.close()
                return True
        except Exception as e:
            print(f"Error altering warehouse size: {e}")
            return False

    @staticmethod
    def resume_warehouse(warehouse_name: str) -> bool:
        """
        Resumes a suspended warehouse.

        Args:
            warehouse_name (str): Name of the warehouse.

        Returns:
            bool: True if successful, False otherwise.
        """
        query = f"ALTER WAREHOUSE {warehouse_name} RESUME"
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                cursor.close()
                return True
        except Exception as e:
            print(f"Error resuming warehouse: {e}")
            return False

    @staticmethod
    def suspend_warehouse(warehouse_name: str) -> bool:
        """
        Suspends a running warehouse.

        Args:
            warehouse_name (str): Name of the warehouse.

        Returns:
            bool: True if successful, False otherwise.
        """
        query = f"ALTER WAREHOUSE {warehouse_name} SUSPEND"
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                cursor.close()
                return True
        except Exception as e:
            print(f"Error suspending warehouse: {e}")
            return False

    @staticmethod
    def drop_warehouse(warehouse_name: str, if_exists: bool = True) -> bool:
        """
        Drops a warehouse.

        Args:
            warehouse_name (str): Name of the warehouse.
            if_exists (bool): Add IF EXISTS clause.

        Returns:
            bool: True if successful, False otherwise.
        """
        exists_clause = "IF EXISTS " if if_exists else ""
        query = f"DROP WAREHOUSE {exists_clause}{warehouse_name}"
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                cursor.close()
                return True
        except Exception as e:
            print(f"Error dropping warehouse: {e}")
            return False

    @staticmethod
    def list_warehouses() -> Optional[List[Dict[str, Any]]]:
        """
        Lists all warehouses.

        Returns:
            Optional[List[Dict]]: List of warehouse information dictionaries.
        """
        query = "SHOW WAREHOUSES"
        
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
            print(f"Error listing warehouses: {e}")
            return None

    @staticmethod
    def get_warehouse_info(warehouse_name: str) -> Optional[Dict[str, Any]]:
        """
        Gets detailed information about a specific warehouse.

        Args:
            warehouse_name (str): Name of the warehouse.

        Returns:
            Optional[Dict]: Warehouse information dictionary.
        """
        query = f"SHOW WAREHOUSES LIKE '{warehouse_name}'"
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                
                columns = [desc[0] for desc in cursor.description]
                row = cursor.fetchone()
                
                cursor.close()
                
                if row:
                    return dict(zip(columns, row))
                return None
        except Exception as e:
            print(f"Error getting warehouse info: {e}")
            return None

    @staticmethod
    def get_warehouse_usage(
        warehouse_name: Optional[str] = None,
        days: int = 7
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Gets warehouse usage statistics.

        Args:
            warehouse_name (Optional[str]): Specific warehouse name (all if None).
            days (int): Number of days to look back.

        Returns:
            Optional[List[Dict]]: List of usage statistics.
        """
        query = f"""
            SELECT
                warehouse_name,
                start_time,
                end_time,
                credits_used,
                credits_used_compute,
                credits_used_cloud_services
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            WHERE start_time >= DATEADD('day', -{days}, CURRENT_TIMESTAMP())
        """
        
        if warehouse_name:
            query += f" AND warehouse_name = '{warehouse_name}'"
        
        query += " ORDER BY start_time DESC"
        
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
            print(f"Error getting warehouse usage: {e}")
            return None

    @staticmethod
    def set_warehouse_parameter(
        warehouse_name: str,
        parameter: str,
        value: Any
    ) -> bool:
        """
        Sets a warehouse parameter.

        Args:
            warehouse_name (str): Name of the warehouse.
            parameter (str): Parameter name (e.g., AUTO_SUSPEND, AUTO_RESUME).
            value (Any): Parameter value.

        Returns:
            bool: True if successful, False otherwise.
        """
        if isinstance(value, bool):
            value_str = str(value).upper()
        elif isinstance(value, str):
            value_str = f"'{value}'"
        else:
            value_str = str(value)
        
        query = f"ALTER WAREHOUSE {warehouse_name} SET {parameter} = {value_str}"
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                cursor.close()
                return True
        except Exception as e:
            print(f"Error setting warehouse parameter: {e}")
            return False

    @staticmethod
    def get_current_warehouse() -> Optional[str]:
        """
        Gets the name of the current warehouse.

        Returns:
            Optional[str]: Current warehouse name.
        """
        query = "SELECT CURRENT_WAREHOUSE()"
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                result = cursor.fetchone()
                cursor.close()
                return result[0] if result else None
        except Exception as e:
            print(f"Error getting current warehouse: {e}")
            return None

    @staticmethod
    def use_warehouse(warehouse_name: str) -> bool:
        """
        Switches to a different warehouse.

        Args:
            warehouse_name (str): Name of the warehouse to use.

        Returns:
            bool: True if successful, False otherwise.
        """
        query = f"USE WAREHOUSE {warehouse_name}"
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                cursor.close()
                return True
        except Exception as e:
            print(f"Error using warehouse: {e}")
            return False

    @staticmethod
    def get_warehouse_load(warehouse_name: str) -> Optional[Dict[str, Any]]:
        """
        Gets current load information for a warehouse.

        Args:
            warehouse_name (str): Name of the warehouse.

        Returns:
            Optional[Dict]: Load information dictionary.
        """
        query = f"""
            SELECT
                warehouse_name,
                avg_running,
                avg_queued_load,
                avg_queued_provisioning,
                avg_blocked
            FROM TABLE(INFORMATION_SCHEMA.WAREHOUSE_LOAD_HISTORY(
                WAREHOUSE_NAME => '{warehouse_name}'
            ))
            ORDER BY start_time DESC
            LIMIT 1
        """
        
        try:
            with SnowflakeConnection.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                
                columns = [desc[0] for desc in cursor.description]
                row = cursor.fetchone()
                
                cursor.close()
                
                if row:
                    return dict(zip(columns, row))
                return None
        except Exception as e:
            print(f"Error getting warehouse load: {e}")
            return None


if __name__ == "__main__":
    print("=== Testing Warehouse Management ===\n")
    
    # Test 1: List warehouses
    print("1. Testing list warehouses:")
    warehouses = WarehouseManagement.list_warehouses()
    if warehouses:
        print(f"   Found {len(warehouses)} warehouses")
        for wh in warehouses[:3]:
            print(f"     - {wh.get('name')}: {wh.get('size')} ({wh.get('state')})")
    print()
    
    # Test 2: Get current warehouse
    print("2. Testing get current warehouse:")
    current = WarehouseManagement.get_current_warehouse()
    if current:
        print(f"   Current warehouse: {current}\n")
    else:
        print("   No current warehouse\n")
    
    # Test 3: Get warehouse info
    print("3. Testing get warehouse info:")
    # Uncomment and modify:
    # info = WarehouseManagement.get_warehouse_info("COMPUTE_WH")
    # if info:
    #     print(f"   Name: {info.get('name')}")
    #     print(f"   Size: {info.get('size')}")
    #     print(f"   State: {info.get('state')}")
    # print()
    print("   Skipped (modify warehouse name)\n")
    
    print("=== Tests Complete ===")


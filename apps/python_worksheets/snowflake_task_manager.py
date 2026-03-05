# Snowflake Task/Job Management via Python
# Create, manage, and schedule Snowflake tasks using remote Python code
# Much easier than using Python worksheets!

import os
from dotenv import load_dotenv
import snowflake.connector


def get_connection():
    """Create a Snowflake connection from .env credentials."""
    load_dotenv()
    
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )
    return conn


def execute_query(conn, query: str):
    """Execute a SQL query and return results."""
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        return results
    finally:
        cursor.close()


def create_sales_analysis_task(conn):
    """
    Create a Snowflake task that runs SQL analysis on SALES_DEMO table.
    Runs daily at 2 AM UTC.
    """
    task_name = "SALES_ANALYSIS_DAILY"
    
    # SQL to execute when task runs
    task_sql = """
    CREATE OR REPLACE TABLE SALES_SUMMARY AS
    SELECT 
        REGION,
        CATEGORY,
        SUM(SALES_AMOUNT) as TOTAL_SALES,
        SUM(QUANTITY) as TOTAL_QUANTITY,
        COUNT(ORDER_ID) as NUM_ORDERS,
        AVG(SALES_AMOUNT) as AVG_SALES
    FROM SALES_DEMO
    GROUP BY REGION, CATEGORY
    ORDER BY TOTAL_SALES DESC;
    """
    
    # Create the task
    create_task_sql = f"""
    CREATE OR REPLACE TASK {task_name}
        WAREHOUSE = {os.getenv('SNOWFLAKE_WAREHOUSE')}
        SCHEDULE = 'USING CRON 0 2 * * * UTC'
        AS
        {task_sql}
    """
    
    execute_query(conn, create_task_sql)
    print(f"✅ Task '{task_name}' created successfully")
    print(f"   Schedule: Daily at 2 AM UTC")
    print(f"   Action: Creates SALES_SUMMARY table from SALES_DEMO")


def create_export_task(conn):
    """
    Create a task that exports sales data to a stage (for external storage).
    Useful for integration with other systems.
    """
    task_name = "SALES_EXPORT_DAILY"
    
    # Create internal stage if not exists
    execute_query(conn, "CREATE STAGE IF NOT EXISTS SALES_EXPORT;")
    
    # Task SQL using Snowpark
    task_sql = """
    COPY INTO @SALES_EXPORT/sales_data_
        FROM (SELECT * FROM SALES_DEMO)
        FILE_FORMAT = (TYPE = 'PARQUET')
        SINGLE = FALSE
        MAX_FILE_SIZE = 5368709120
        OVERWRITE = TRUE;
    """
    
    create_task_sql = f"""
    CREATE OR REPLACE TASK {task_name}
        WAREHOUSE = {os.getenv('SNOWFLAKE_WAREHOUSE')}
        SCHEDULE = 'USING CRON 0 3 * * * UTC'
        AS
        {task_sql}
    """
    
    execute_query(conn, create_task_sql)
    print(f"✅ Task '{task_name}' created successfully")
    print(f"   Schedule: Daily at 3 AM UTC")
    print(f"   Action: Exports SALES_DEMO to @SALES_EXPORT stage as Parquet")


def create_aggregation_task(conn):
    """
    Create a task that generates daily, monthly, and yearly summaries.
    """
    task_name = "SALES_AGGREGATION_DAILY"
    
    task_sql = f"""
    CREATE OR REPLACE TABLE SALES_METRICS AS
    WITH daily_metrics AS (
        SELECT 
            ORDER_DATE as METRIC_DATE,
            'DAILY' as PERIOD,
            SUM(SALES_AMOUNT) as TOTAL_SALES,
            SUM(QUANTITY) as TOTAL_QUANTITY,
            COUNT(ORDER_ID) as NUM_ORDERS
        FROM SALES_DEMO
        GROUP BY ORDER_DATE
    )
    SELECT * FROM daily_metrics
    ORDER BY METRIC_DATE DESC;
    """
    
    create_task_sql = f"""
    CREATE OR REPLACE TASK {task_name}
        WAREHOUSE = {os.getenv('SNOWFLAKE_WAREHOUSE')}
        SCHEDULE = 'USING CRON 0 4 * * * UTC'
        AS
        {task_sql}
    """
    
    execute_query(conn, create_task_sql)
    print(f"✅ Task '{task_name}' created successfully")
    print(f"   Schedule: Daily at 4 AM UTC")
    print(f"   Action: Generates SALES_METRICS summary table")


def list_tasks(conn):
    """List all tasks in the current schema."""
    query = "SHOW TASKS;"
    results = execute_query(conn, query)
    
    if not results:
        print("❌ No tasks found")
        return
    
    print("\n" + "=" * 80)
    print("EXISTING TASKS")
    print("=" * 80)
    for row in results:
        print(f"Task: {row[1]}")
        print(f"  Created: {row[2]}")
        print(f"  Schedule: {row[3]}")
        print(f"  State: {row[4]}")
        print()


def enable_task(conn, task_name: str):
    """Enable a task (resume execution)."""
    query = f"ALTER TASK {task_name} RESUME;"
    execute_query(conn, query)
    print(f"✅ Task '{task_name}' enabled (resumed)")


def disable_task(conn, task_name: str):
    """Disable a task (pause execution)."""
    query = f"ALTER TASK {task_name} SUSPEND;"
    execute_query(conn, query)
    print(f"⏸️  Task '{task_name}' disabled (suspended)")


def delete_task(conn, task_name: str):
    """Delete a task."""
    query = f"DROP TASK IF EXISTS {task_name};"
    execute_query(conn, query)
    print(f"❌ Task '{task_name}' deleted")


def get_task_history(conn, task_name: str):
    """Get execution history for a task."""
    query = f"""
    SELECT 
        QUERY_ID,
        TASK_NAME,
        DATABASE_NAME,
        SCHEMA_NAME,
        QUERY_START_TIME,
        QUERY_EXECUTION_TIME,
        STATE
    FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
        TASK_NAME => '{task_name}'
    ))
    ORDER BY QUERY_START_TIME DESC
    LIMIT 10;
    """
    
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    
    if not results:
        print(f"❌ No execution history found for task '{task_name}'")
        return
    
    print(f"\n{'=' * 80}")
    print(f"TASK EXECUTION HISTORY: {task_name}")
    print(f"{'=' * 80}")
    for row in results:
        print(f"Execution Time: {row[4]}")
        print(f"  Duration: {row[5]} ms")
        print(f"  Status: {row[6]}")
        print()


def main():
    """Main execution."""
    conn = get_connection()
    print("✅ Connected to Snowflake\n")
    
    try:
        # Create multiple tasks
        print("Creating Snowflake tasks...\n")
        
        create_sales_analysis_task(conn)
        print()
        
        create_export_task(conn)
        print()
        
        create_aggregation_task(conn)
        print()
        
        # List all tasks
        list_tasks(conn)
        
        # Optional: Get history of a specific task
        # get_task_history(conn, "SALES_ANALYSIS_DAILY")
        
        # Optional: Manage tasks
        # enable_task(conn, "SALES_ANALYSIS_DAILY")
        # disable_task(conn, "SALES_ANALYSIS_DAILY")
        # delete_task(conn, "SALES_ANALYSIS_DAILY")
        
    finally:
        conn.close()
        print("✅ Connection closed")


if __name__ == "__main__":
    main()

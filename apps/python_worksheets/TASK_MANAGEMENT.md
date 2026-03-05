# Snowflake Task Management Guide

Learn how to create and manage Snowflake Tasks (jobs) using Python.

## What is a Snowflake Task?

A **Task** is a scheduled job that executes SQL/Python code at specified intervals. Similar to cron jobs or scheduled jobs in other systems.

## Two Approaches

### ✅ Approach 1: Remote Python Code (RECOMMENDED)
- **Best for**: Version control, automation, CI/CD pipelines
- **File**: `snowflake_task_manager.py`
- **How**: Run from your local machine
- **Pros**: Easy to maintain, integrate into workflows, schedule via external tools

### ⚠️ Approach 2: Snowflake Python Worksheet
- **Best for**: One-off testing, exploration
- **How**: Run directly in Snowflake
- **Cons**: Manual, harder to version control, requires manual execution

## Quick Start (Remote Python)

### 1. Create Tasks

```bash
python apps/python_worksheets/snowflake_task_manager.py
```

This creates 3 sample tasks:
- **SALES_ANALYSIS_DAILY** - Runs at 2 AM UTC, creates SALES_SUMMARY table
- **SALES_EXPORT_DAILY** - Runs at 3 AM UTC, exports data to stage
- **SALES_AGGREGATION_DAILY** - Runs at 4 AM UTC, creates metrics table

### 2. Enable a Task

```python
from snowflake_task_manager import get_connection, enable_task

conn = get_connection()
enable_task(conn, "SALES_ANALYSIS_DAILY")
conn.close()
```

### 3. Check Task Status

In Snowflake:
```sql
SHOW TASKS;
```

### 4. View Execution History

```python
from snowflake_task_manager import get_connection, get_task_history

conn = get_connection()
get_task_history(conn, "SALES_ANALYSIS_DAILY")
conn.close()
```

## Task Schedule Formats

Snowflake tasks use CRON syntax:

```
USING CRON 0 2 * * * UTC          # Daily at 2 AM UTC
USING CRON 0 */6 * * * UTC        # Every 6 hours
USING CRON 0 0 * * MON UTC        # Weekly on Monday
USING CRON 0 0 1 * * UTC          # Monthly on the 1st
USING CRON '*/15 * * * * UTC'     # Every 15 minutes
```

## Available Functions in `snowflake_task_manager.py`

| Function | Purpose |
|----------|---------|
| `create_sales_analysis_task()` | Creates task for sales analysis |
| `create_export_task()` | Creates task for exporting data |
| `create_aggregation_task()` | Creates task for metrics aggregation |
| `list_tasks()` | Show all tasks in schema |
| `enable_task()` | Resume/enable a task |
| `disable_task()` | Suspend/disable a task |
| `delete_task()` | Delete a task |
| `get_task_history()` | View past executions |

## Complete Example

```python
from snowflake_task_manager import (
    get_connection, 
    create_sales_analysis_task,
    list_tasks,
    get_task_history
)

# Connect
conn = get_connection()

# Create a task
create_sales_analysis_task(conn)

# List all tasks
list_tasks(conn)

# Check execution history
get_task_history(conn, "SALES_ANALYSIS_DAILY")

# Close connection
conn.close()
```

## Managing Tasks in Snowflake UI

### View Tasks
```sql
SHOW TASKS;
```

### View Task History
```sql
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'SALES_ANALYSIS_DAILY'
));
```

### Suspend a Task
```sql
ALTER TASK SALES_ANALYSIS_DAILY SUSPEND;
```

### Resume a Task
```sql
ALTER TASK SALES_ANALYSIS_DAILY RESUME;
```

### Drop a Task
```sql
DROP TASK SALES_ANALYSIS_DAILY;
```

## Creating Custom Tasks

Edit `snowflake_task_manager.py` and add:

```python
def create_custom_task(conn):
    """Create your custom task."""
    task_name = "MY_CUSTOM_TASK"
    
    # Your SQL logic
    task_sql = """
    SELECT * FROM SALES_DEMO LIMIT 10;
    """
    
    # Create task
    create_task_sql = f"""
    CREATE OR REPLACE TASK {task_name}
        WAREHOUSE = {os.getenv('SNOWFLAKE_WAREHOUSE')}
        SCHEDULE = 'USING CRON 0 2 * * * UTC'
        AS
        {task_sql}
    """
    
    execute_query(conn, create_task_sql)
    print(f"✅ Task '{task_name}' created")
```

## Best Practices

1. **Use remote Python scripts** for production tasks
2. **Version control your task definitions** in Git
3. **Test tasks** with SUSPEND before enabling
4. **Monitor execution history** regularly
5. **Use appropriate warehouses** (consider size and cost)
6. **Schedule during off-peak hours** if possible
7. **Add error handling** in task SQL

## Troubleshooting

### Task Not Running
1. Check if task is RESUMED: `SHOW TASKS;`
2. Verify warehouse is active and accessible
3. Check credentials in `.env`

### Task Failed
1. View history: `SHOW TASK HISTORY;`
2. Check warehouse resources
3. Verify SQL syntax and table names
4. Review Snowflake error messages

### Permission Denied
1. Ensure user has EXECUTE permission on warehouse
2. Ensure user can write to schema
3. Check role-based access controls (RBAC)

## Next Steps

- Create custom tasks for your use cases
- Integrate with CI/CD pipelines
- Set up alerts for task failures
- Monitor task execution metrics
- Scale to production workloads

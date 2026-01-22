# Snowflake Python Operations

A comprehensive Python library for interacting with Snowflake data warehouse, featuring organized modules for common database operations and advanced use cases.

## 📋 Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Configuration](#configuration)
- [Modules](#modules)
- [Usage Examples](#usage-examples)
- [Best Practices](#best-practices)
- [Contributing](#contributing)

## ✨ Features

- **Connection Management**: Secure connection handling with context managers
- **CRUD Operations**: Complete Create, Read, Update, Delete functionality
- **Table Management**: Create, alter, clone, and manage tables
- **Data Loading**: Efficient data loading from various sources (CSV, JSON, Parquet, S3, GCS, Azure)
- **Warehouse Management**: Control and monitor warehouse operations
- **Batch Operations**: Optimized batch processing for large datasets
- **Pandas Integration**: Seamless DataFrame to Snowflake table conversion
- **Error Handling**: Comprehensive error handling and logging
- **Type Safety**: Full type hints for better IDE support

## 🚀 Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd Snowflake
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your Snowflake credentials
```

## ⚙️ Configuration

Create a `.env` file in the project root with your Snowflake credentials:

```env
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_WAREHOUSE=your_warehouse_name
SNOWFLAKE_DATABASE=your_database_name
SNOWFLAKE_SCHEMA=your_schema_name
SNOWFLAKE_ROLE=your_role_name
```

## 📦 Modules

### 1. `snowflake_connection.py`
Handles database connections with support for:
- Direct connections
- Context managers
- Connection testing
- Multiple connection parameters

### 2. `select_operations.py`
Query and retrieve data with:
- Basic SELECT queries
- Parameterized queries
- Results as tuples, dictionaries, or DataFrames
- Pagination for large datasets
- Query statistics

### 3. `insert_operations.py`
Insert data using:
- Single row inserts
- Batch inserts
- DataFrame to table
- INSERT from SELECT
- Upsert operations
- Bulk loading with staging

### 4. `update_operations.py`
Update records with:
- Single/multiple column updates
- Conditional updates with CASE
- Updates from another table
- Batch updates
- Increment operations
- Upsert (MERGE) operations

### 5. `delete_operations.py`
Delete data using:
- Delete by ID or conditions
- Bulk deletes
- Delete old records
- Remove duplicates
- Soft deletes
- Batch deletion

### 6. `table_operations.py`
Manage tables with:
- Create/drop tables
- Add/drop/rename columns
- Clone tables (Time Travel)
- Get table schema and statistics
- Primary key management

### 7. `data_loading.py`
Load and unload data:
- COPY INTO operations
- Stage management
- File format creation
- S3/GCS/Azure integration
- Data validation
- Load history tracking

### 8. `warehouse_management.py`
Control warehouses:
- Create/drop warehouses
- Resize warehouses
- Suspend/resume operations
- Monitor usage and credits
- Get warehouse statistics

## 💡 Usage Examples

### Basic Connection Test
```python
from snowflake_connection import SnowflakeConnection

# Test connection
details = SnowflakeConnection.test_connection()
print(f"Connected to: {details['database']}.{details['schema']}")
```

### Query Data
```python
from select_operations import SelectOperations

# Fetch as list of tuples
results = SelectOperations.fetch_data("SELECT * FROM customers LIMIT 10")

# Fetch as DataFrame
df = SelectOperations.fetch_as_dataframe("SELECT * FROM orders WHERE status = 'pending'")

# Fetch with parameters
result = SelectOperations.fetch_one(
    "SELECT * FROM users WHERE id = :user_id",
    {"user_id": 123}
)
```

### Insert Data
```python
from insert_operations import InsertOperations

# Insert single row
success = InsertOperations.insert_single_row(
    "customers",
    {"id": 1, "name": "John Doe", "email": "john@example.com"}
)

# Insert multiple rows
data = [
    {"id": 2, "name": "Jane Smith", "email": "jane@example.com"},
    {"id": 3, "name": "Bob Johnson", "email": "bob@example.com"}
]
success, count = InsertOperations.insert_multiple_rows("customers", data)

# Insert from DataFrame
import pandas as pd
df = pd.read_csv("data.csv")
success, count = InsertOperations.insert_from_dataframe("customers", df)
```

### Update Data
```python
from update_operations import UpdateOperations

# Update single row
success, count = UpdateOperations.update_single_row(
    "customers",
    {"email": "newemail@example.com"},
    {"id": 1}
)

# Batch update
updates = [
    {"id": 1, "status": "active"},
    {"id": 2, "status": "inactive"}
]
success, count = UpdateOperations.batch_update("customers", updates, "id")

# Upsert (insert or update)
success, count = UpdateOperations.upsert(
    "customers",
    {"id": 100, "name": "New Customer", "email": "new@example.com"},
    ["id"]
)
```

### Delete Data
```python
from delete_operations import DeleteOperations

# Delete by ID
success, count = DeleteOperations.delete_by_id("customers", "id", 1)

# Delete multiple IDs
success, count = DeleteOperations.delete_by_ids("customers", "id", [2, 3, 4])

# Delete old records
success, count = DeleteOperations.delete_old_records(
    "logs",
    "created_at",
    days_old=30
)

# Soft delete
success, count = DeleteOperations.soft_delete(
    "customers",
    {"id": 5},
    deleted_column="is_deleted"
)
```

### Table Operations
```python
from table_operations import TableOperations

# Create table
success = TableOperations.create_table(
    "new_table",
    {
        "id": "INT",
        "name": "VARCHAR(100)",
        "created_at": "TIMESTAMP"
    }
)

# Clone table
success = TableOperations.clone_table("source_table", "backup_table")

# Get table schema
schema = TableOperations.get_table_schema("customers")
for col in schema:
    print(f"{col['name']}: {col['type']}")
```

### Data Loading
```python
from data_loading import DataLoading

# Load from stage
success, stats = DataLoading.copy_into_table(
    "customers",
    "@my_stage",
    file_format="CSV"
)
print(f"Loaded {stats['rows_loaded']} rows")

# Load from S3
success, stats = DataLoading.load_csv_from_s3(
    "customers",
    "s3://my-bucket/data/",
    aws_key_id="YOUR_KEY",
    aws_secret_key="YOUR_SECRET"
)

# Load DataFrame
import pandas as pd
df = pd.read_csv("data.csv")
success, rows = DataLoading.load_dataframe_to_table(df, "customers")
```

### Warehouse Management
```python
from warehouse_management import WarehouseManagement

# List warehouses
warehouses = WarehouseManagement.list_warehouses()
for wh in warehouses:
    print(f"{wh['name']}: {wh['size']} - {wh['state']}")

# Resize warehouse
success = WarehouseManagement.alter_warehouse_size("COMPUTE_WH", "MEDIUM")

# Get usage statistics
usage = WarehouseManagement.get_warehouse_usage("COMPUTE_WH", days=7)
total_credits = sum(u['credits_used'] for u in usage)
print(f"Total credits used: {total_credits}")
```

## 🎯 Best Practices

1. **Use Context Managers**: Always use `get_connection_context()` for automatic connection cleanup
2. **Parameterized Queries**: Use parameters to prevent SQL injection
3. **Batch Operations**: Use batch methods for multiple operations to improve performance
4. **Error Handling**: Wrap operations in try-except blocks
5. **Connection Pooling**: Reuse connections when possible
6. **Warehouse Management**: Suspend warehouses when not in use to save costs
7. **Time Travel**: Use table cloning for backups before major changes
8. **Monitoring**: Regularly check warehouse usage and query history

## 📚 Additional Resources

- [Snowflake Documentation](https://docs.snowflake.com/)
- [Python Connector Documentation](https://docs.snowflake.com/en/user-guide/python-connector.html)
- [Snowflake Commands Cheatsheet](snowflake_commands_cheatsheet.md)

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🔒 Security

- Never commit `.env` files with real credentials
- Use environment variables for sensitive information
- Implement proper access controls in Snowflake
- Regularly rotate passwords and access keys
- Use role-based access control (RBAC)

## 📞 Support

For issues, questions, or contributions, please open an issue in the repository.

---

**Note**: This library is designed for educational and development purposes. Always test thoroughly before using in production environments.

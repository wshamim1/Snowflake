# Snowflake Commands Cheatsheet

A comprehensive reference guide for Snowflake SQL commands, Python integration, and best practices.

---

## Table of Contents

1. [Python Connection](#python-connection)
2. [Database Management](#database-management)
3. [Schema Management](#schema-management)
4. [Warehouse Management](#warehouse-management)
5. [Table Management](#table-management)
6. [Data Querying](#data-querying)
7. [Joins](#joins)
8. [Data Loading & Unloading](#data-loading--unloading)
9. [Stages](#stages)
10. [File Formats](#file-formats)
11. [User & Role Management](#user--role-management)
12. [Time Travel & Cloning](#time-travel--cloning)
13. [Transactions](#transactions)
14. [Functions](#functions)
15. [Stored Procedures](#stored-procedures)
16. [Snowpipe](#snowpipe)
17. [Encryption](#encryption)
18. [Query History](#query-history)
19. [Best Practices](#best-practices)

---

## Python Connection

### Basic Connection
```python
import snowflake.connector

conn = snowflake.connector.connect(
    user='YOUR_USER',
    password='YOUR_PASSWORD',
    account='YOUR_ACCOUNT',
    warehouse='YOUR_WAREHOUSE',
    database='YOUR_DATABASE',
    schema='YOUR_SCHEMA'
)

# Create cursor
cursor = conn.cursor()

# Execute query
cursor.execute("SELECT * FROM table_name")

# Fetch results
results = cursor.fetchall()

# Close connection
cursor.close()
conn.close()
```

### Using Context Manager
```python
with snowflake.connector.connect(
    user='YOUR_USER',
    password='YOUR_PASSWORD',
    account='YOUR_ACCOUNT'
) as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT CURRENT_VERSION()")
        print(cursor.fetchone())
```

---

## Database Management

### Create Database
```sql
CREATE DATABASE my_database;
CREATE DATABASE IF NOT EXISTS my_database;
```

### Drop Database
```sql
DROP DATABASE my_database;
DROP DATABASE IF EXISTS my_database;
```

### List All Databases
```sql
SHOW DATABASES;
```

### Use Database
```sql
USE DATABASE my_database;
```

---

## Schema Management

### Create Schema
```sql
CREATE SCHEMA my_schema;
CREATE SCHEMA IF NOT EXISTS my_schema;
```

### Drop Schema
```sql
DROP SCHEMA my_schema;
DROP SCHEMA IF EXISTS my_schema;
```

### List Schemas
```sql
-- In current database
SHOW SCHEMAS;

-- In specific database
SHOW SCHEMAS IN DATABASE my_database;
```

### Use Schema
```sql
USE SCHEMA my_schema;
```

---

## Warehouse Management

### Warehouse Sizes
- **X-SMALL**: 1 credit/hour
- **SMALL**: 2 credits/hour
- **MEDIUM**: 4 credits/hour
- **LARGE**: 8 credits/hour
- **X-LARGE**: 16 credits/hour
- **2X-LARGE**: 32 credits/hour
- **3X-LARGE**: 64 credits/hour
- **4X-LARGE**: 128 credits/hour

### Create Warehouse
```sql
CREATE WAREHOUSE my_warehouse
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;
```

### Alter Warehouse
```sql
-- Change size
ALTER WAREHOUSE my_warehouse SET WAREHOUSE_SIZE = 'MEDIUM';

-- Change auto-suspend time
ALTER WAREHOUSE my_warehouse SET AUTO_SUSPEND = 300;

-- Enable/disable auto-resume
ALTER WAREHOUSE my_warehouse SET AUTO_RESUME = TRUE;
```

### Start/Stop Warehouse
```sql
-- Start warehouse
ALTER WAREHOUSE my_warehouse RESUME;

-- Stop warehouse
ALTER WAREHOUSE my_warehouse SUSPEND;
```

### Drop Warehouse
```sql
DROP WAREHOUSE my_warehouse;
DROP WAREHOUSE IF EXISTS my_warehouse;
```

### List Warehouses
```sql
SHOW WAREHOUSES;
```

---

## Table Management

### Snowflake Data Types

**Numeric Types:**
- `NUMBER(precision, scale)` - Fixed-point numbers
- `INT`, `INTEGER` - Whole numbers
- `FLOAT`, `DOUBLE` - Floating-point numbers

**String Types:**
- `VARCHAR(length)` - Variable-length string
- `STRING` - Unlimited length string
- `CHAR(length)` - Fixed-length string
- `TEXT` - Alias for STRING

**Date/Time Types:**
- `DATE` - Date only
- `TIME` - Time only
- `TIMESTAMP` - Date and time
- `TIMESTAMP_LTZ` - With local timezone
- `TIMESTAMP_NTZ` - Without timezone
- `TIMESTAMP_TZ` - With timezone

**Semi-Structured Types:**
- `VARIANT` - JSON, XML, Avro, Parquet
- `OBJECT` - Key-value pairs
- `ARRAY` - Ordered list

**Other Types:**
- `BOOLEAN` - TRUE/FALSE
- `BINARY` - Binary data

### Create Table
```sql
CREATE OR REPLACE TABLE employees (
    id INT,
    name STRING,
    email VARCHAR(100),
    salary NUMBER(10, 2),
    hire_date DATE,
    is_active BOOLEAN,
    metadata VARIANT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

### Create Table from Select (CTAS)
```sql
CREATE TABLE new_table AS
SELECT * FROM existing_table
WHERE condition;
```

### Insert Data
```sql
-- Single row
INSERT INTO employees (id, name, email, salary)
VALUES (1, 'John Doe', 'john@example.com', 75000.00);

-- Multiple rows
INSERT INTO employees (id, name, email, salary)
VALUES 
    (2, 'Jane Smith', 'jane@example.com', 80000.00),
    (3, 'Bob Johnson', 'bob@example.com', 70000.00);
```

### Update Data
```sql
UPDATE employees
SET salary = salary * 1.10
WHERE department = 'Engineering';
```

### Delete Data
```sql
DELETE FROM employees
WHERE id = 1;
```

### Truncate Table
```sql
TRUNCATE TABLE employees;
```

### Drop Table
```sql
DROP TABLE employees;
DROP TABLE IF EXISTS employees;
```

### Describe Table
```sql
DESCRIBE TABLE employees;
DESC TABLE employees;
```

### Show Tables
```sql
SHOW TABLES;
SHOW TABLES IN SCHEMA my_schema;
```

---

## Data Querying

### Basic SELECT
```sql
-- Select all columns
SELECT * FROM employees;

-- Select specific columns
SELECT id, name, salary FROM employees;

-- With alias
SELECT name AS employee_name, salary AS annual_salary
FROM employees;
```

### WHERE Clause
```sql
SELECT * FROM employees
WHERE salary > 50000
  AND department = 'Engineering'
  AND is_active = TRUE;
```

### ORDER BY
```sql
-- Ascending (default)
SELECT * FROM employees
ORDER BY salary ASC;

-- Descending
SELECT * FROM employees
ORDER BY salary DESC;

-- Multiple columns
SELECT * FROM employees
ORDER BY department ASC, salary DESC;
```

### LIMIT
```sql
SELECT * FROM employees
ORDER BY salary DESC
LIMIT 10;
```

### DISTINCT
```sql
SELECT DISTINCT department FROM employees;
```

### GROUP BY
```sql
SELECT department, COUNT(*) as employee_count, AVG(salary) as avg_salary
FROM employees
GROUP BY department;
```

### HAVING
```sql
SELECT department, AVG(salary) as avg_salary
FROM employees
GROUP BY department
HAVING AVG(salary) > 60000;
```

### Subqueries
```sql
-- In WHERE clause
SELECT * FROM employees
WHERE department_id IN (
    SELECT id FROM departments WHERE location = 'New York'
);

-- In FROM clause
SELECT dept_name, avg_salary
FROM (
    SELECT department, AVG(salary) as avg_salary
    FROM employees
    GROUP BY department
) AS dept_stats;
```

---

## Joins

### INNER JOIN
Returns rows where there is a match in both tables.
```sql
SELECT e.name, d.department_name
FROM employees e
INNER JOIN departments d ON e.department_id = d.id;
```

### LEFT JOIN (LEFT OUTER JOIN)
Returns all rows from the left table and matching rows from the right table.
```sql
SELECT e.name, d.department_name
FROM employees e
LEFT JOIN departments d ON e.department_id = d.id;
```

### RIGHT JOIN (RIGHT OUTER JOIN)
Returns all rows from the right table and matching rows from the left table.
```sql
SELECT e.name, d.department_name
FROM employees e
RIGHT JOIN departments d ON e.department_id = d.id;
```

### FULL OUTER JOIN
Returns all rows when there is a match in either table.
```sql
SELECT e.name, d.department_name
FROM employees e
FULL OUTER JOIN departments d ON e.department_id = d.id;
```

### CROSS JOIN
Returns the Cartesian product of both tables.
```sql
SELECT e.name, d.department_name
FROM employees e
CROSS JOIN departments d;
```

### SELF JOIN
Joins a table with itself.
```sql
SELECT e1.name AS employee, e2.name AS manager
FROM employees e1
JOIN employees e2 ON e1.manager_id = e2.id;
```

### NATURAL JOIN
Implicit join based on columns with the same name.
```sql
SELECT * FROM employees
NATURAL JOIN departments;
```

---

## Data Loading & Unloading

### COPY INTO (Load Data)
```sql
-- From internal stage
COPY INTO my_table
FROM @my_stage
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- From external stage (S3)
COPY INTO my_table
FROM @my_s3_stage
FILE_FORMAT = (TYPE = 'CSV')
ON_ERROR = 'CONTINUE';

-- With pattern matching
COPY INTO my_table
FROM @my_stage
PATTERN = '.*sales.*[.]csv'
FILE_FORMAT = (TYPE = 'CSV');
```

### COPY INTO (Unload Data)
```sql
-- To internal stage
COPY INTO @my_stage
FROM my_table
FILE_FORMAT = (TYPE = 'CSV' HEADER = TRUE);

-- To external stage (S3)
COPY INTO @my_s3_stage
FROM my_table
FILE_FORMAT = (TYPE = 'PARQUET');
```

### COPY Options
```sql
COPY INTO my_table
FROM @my_stage
FILE_FORMAT = (TYPE = 'CSV')
ON_ERROR = 'CONTINUE'        -- CONTINUE, SKIP_FILE, ABORT_STATEMENT
FORCE = TRUE                  -- Reload files
VALIDATION_MODE = 'RETURN_ERRORS';  -- Validate without loading
```

### MERGE Command
```sql
MERGE INTO target_table tgt
USING source_table src
ON tgt.id = src.id
WHEN MATCHED THEN
    UPDATE SET 
        tgt.name = src.name,
        tgt.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (id, name, created_at)
    VALUES (src.id, src.name, CURRENT_TIMESTAMP());
```

---

## Stages

### Internal Stage Types
- **User Stage**: `@~` - Personal stage for each user
- **Table Stage**: `@%table_name` - Specific to a table
- **Named Stage**: `@stage_name` - Shared named stage

### Create Internal Stage
```sql
CREATE OR REPLACE STAGE my_internal_stage
FILE_FORMAT = (TYPE = 'CSV');
```

### Create External Stage (S3)
```sql
CREATE OR REPLACE STAGE my_s3_stage
URL = 's3://my-bucket/path/'
CREDENTIALS = (
    AWS_KEY_ID = 'your_key_id'
    AWS_SECRET_KEY = 'your_secret_key'
)
FILE_FORMAT = (TYPE = 'CSV');
```

### Create External Stage (GCS)
```sql
CREATE OR REPLACE STAGE my_gcs_stage
URL = 'gcs://my-bucket/path/'
CREDENTIALS = (
    GCP_KEY = 'your_gcp_key'
)
FILE_FORMAT = (TYPE = 'CSV');
```

### Create External Stage (Azure)
```sql
CREATE OR REPLACE STAGE my_azure_stage
URL = 'azure://myaccount.blob.core.windows.net/mycontainer/path/'
CREDENTIALS = (
    AZURE_SAS_TOKEN = 'your_sas_token'
)
FILE_FORMAT = (TYPE = 'CSV');
```

### List Files in Stage
```sql
LIST @my_stage;
LIST @my_s3_stage PATTERN = '.*[.]csv';
```

### Show Stages
```sql
SHOW STAGES;
SHOW STAGES IN SCHEMA my_schema;
```

### Describe Stage
```sql
DESCRIBE STAGE my_stage;
DESC STAGE my_stage;
```

### Drop Stage
```sql
DROP STAGE my_stage;
DROP STAGE IF EXISTS my_stage;
```

### Upload Files to Stage (SnowSQL)
```bash
PUT file:///path/to/local/file.csv @my_stage;
PUT file:///path/to/local/*.csv @my_stage AUTO_COMPRESS=TRUE;
```

### Download Files from Stage (SnowSQL)
```bash
GET @my_stage/file.csv file:///path/to/local/;
```

---

## File Formats

### Supported File Formats
- **CSV** - Comma-separated values
- **JSON** - JavaScript Object Notation
- **AVRO** - Apache Avro format
- **ORC** - Optimized Row Columnar
- **PARQUET** - Apache Parquet
- **XML** - Extensible Markup Language

### Create CSV File Format
```sql
CREATE OR REPLACE FILE FORMAT my_csv_format
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = ('NULL', 'null', '')
EMPTY_FIELD_AS_NULL = TRUE
COMPRESSION = 'AUTO';
```

### Create JSON File Format
```sql
CREATE OR REPLACE FILE FORMAT my_json_format
TYPE = 'JSON'
COMPRESSION = 'AUTO'
STRIP_OUTER_ARRAY = TRUE;
```

### Create Parquet File Format
```sql
CREATE OR REPLACE FILE FORMAT my_parquet_format
TYPE = 'PARQUET'
COMPRESSION = 'SNAPPY';
```

### Show File Formats
```sql
SHOW FILE FORMATS;
```

### Describe File Format
```sql
DESCRIBE FILE FORMAT my_csv_format;
```

### Drop File Format
```sql
DROP FILE FORMAT my_csv_format;
```

---

## User & Role Management

### Create User
```sql
CREATE USER john_doe
PASSWORD = 'SecurePassword123!'
DEFAULT_ROLE = 'analyst'
DEFAULT_WAREHOUSE = 'compute_wh'
DEFAULT_NAMESPACE = 'my_db.public'
MUST_CHANGE_PASSWORD = TRUE;
```

### Alter User
```sql
-- Change password
ALTER USER john_doe SET PASSWORD = 'NewPassword456!';

-- Change default role
ALTER USER john_doe SET DEFAULT_ROLE = 'developer';

-- Disable user
ALTER USER john_doe SET DISABLED = TRUE;
```

### Drop User
```sql
DROP USER john_doe;
```

### Show Users
```sql
SHOW USERS;
```

### Create Role
```sql
CREATE ROLE analyst;
```

### Grant Role to User
```sql
GRANT ROLE analyst TO USER john_doe;
```

### Grant Privileges to Role
```sql
-- Database privileges
GRANT USAGE ON DATABASE my_db TO ROLE analyst;

-- Schema privileges
GRANT USAGE ON SCHEMA my_db.public TO ROLE analyst;

-- Table privileges
GRANT SELECT ON TABLE my_table TO ROLE analyst;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE my_table TO ROLE developer;

-- All privileges
GRANT ALL PRIVILEGES ON TABLE my_table TO ROLE admin;

-- Warehouse privileges
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE analyst;
```

### Revoke Privileges
```sql
REVOKE SELECT ON TABLE my_table FROM ROLE analyst;
```

### Show Grants
```sql
-- Show grants to a role
SHOW GRANTS TO ROLE analyst;

-- Show grants on an object
SHOW GRANTS ON TABLE my_table;

-- Show grants to a user
SHOW GRANTS TO USER john_doe;
```

### Drop Role
```sql
DROP ROLE analyst;
```

---

## Time Travel & Cloning

### Query Historical Data
```sql
-- Query at specific timestamp
SELECT * FROM my_table 
AT(TIMESTAMP => '2024-01-15 10:30:00'::TIMESTAMP);

-- Query at offset (seconds ago)
SELECT * FROM my_table 
AT(OFFSET => -3600);  -- 1 hour ago

-- Query before statement
SELECT * FROM my_table 
BEFORE(STATEMENT => '01a8b9c0-0000-1234-5678-9abcdef01234');
```

### Clone Table
```sql
-- Clone current state
CREATE TABLE my_table_clone CLONE my_table;

-- Clone at specific time
CREATE TABLE my_table_clone CLONE my_table 
AT(TIMESTAMP => '2024-01-15 10:30:00'::TIMESTAMP);

-- Clone before statement
CREATE TABLE my_table_clone CLONE my_table 
BEFORE(STATEMENT => '01a8b9c0-0000-1234-5678-9abcdef01234');
```

### Clone Database
```sql
CREATE DATABASE my_db_clone CLONE my_db;
```

### Clone Schema
```sql
CREATE SCHEMA my_schema_clone CLONE my_schema;
```

### Undrop Objects
```sql
-- Undrop table
UNDROP TABLE my_table;

-- Undrop schema
UNDROP SCHEMA my_schema;

-- Undrop database
UNDROP DATABASE my_db;
```

---

## Transactions

### Begin Transaction
```sql
BEGIN;
-- or
BEGIN TRANSACTION;
```

### Commit Transaction
```sql
COMMIT;
-- or
COMMIT WORK;
```

### Rollback Transaction
```sql
ROLLBACK;
-- or
ROLLBACK WORK;
```

### Transaction Example
```sql
BEGIN;
    UPDATE accounts SET balance = balance - 100 WHERE id = 1;
    UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;
```

---

## Functions

### String Functions
```sql
-- Convert to uppercase/lowercase
SELECT UPPER('snowflake');  -- SNOWFLAKE
SELECT LOWER('SNOWFLAKE');  -- snowflake

-- Substring
SELECT SUBSTRING('snowflake', 1, 4);  -- snow

-- Concatenation
SELECT CONCAT('Hello', ' ', 'World');  -- Hello World
SELECT 'Hello' || ' ' || 'World';      -- Hello World

-- Trim
SELECT TRIM('  snowflake  ');          -- snowflake
SELECT LTRIM('  snowflake');           -- snowflake
SELECT RTRIM('snowflake  ');           -- snowflake

-- Length
SELECT LENGTH('snowflake');            -- 9

-- Replace
SELECT REPLACE('Hello World', 'World', 'Snowflake');  -- Hello Snowflake

-- Split
SELECT SPLIT('a,b,c', ',');            -- ["a","b","c"]
```

### Numeric Functions
```sql
-- Rounding
SELECT ROUND(123.456, 2);              -- 123.46
SELECT CEIL(123.456);                  -- 124
SELECT FLOOR(123.456);                 -- 123

-- Absolute value
SELECT ABS(-42);                       -- 42

-- Power and square root
SELECT POWER(2, 3);                    -- 8
SELECT SQRT(16);                       -- 4

-- Trigonometric
SELECT SIN(0);                         -- 0
SELECT COS(0);                         -- 1
```

### Date/Time Functions
```sql
-- Current date/time
SELECT CURRENT_DATE();
SELECT CURRENT_TIME();
SELECT CURRENT_TIMESTAMP();

-- Date arithmetic
SELECT DATEADD('day', 10, '2024-01-01');           -- 2024-01-11
SELECT DATEDIFF('day', '2024-01-01', '2024-01-10'); -- 9

-- Extract parts
SELECT YEAR('2024-01-15');             -- 2024
SELECT MONTH('2024-01-15');            -- 1
SELECT DAY('2024-01-15');              -- 15
SELECT DAYOFWEEK('2024-01-15');        -- 1 (Monday)

-- Format date
SELECT TO_CHAR(CURRENT_DATE(), 'YYYY-MM-DD');
SELECT TO_DATE('2024-01-15', 'YYYY-MM-DD');
```

### Aggregate Functions
```sql
-- Count
SELECT COUNT(*) FROM employees;
SELECT COUNT(DISTINCT department) FROM employees;

-- Sum, Average, Min, Max
SELECT SUM(salary) FROM employees;
SELECT AVG(salary) FROM employees;
SELECT MIN(salary) FROM employees;
SELECT MAX(salary) FROM employees;

-- Standard deviation and variance
SELECT STDDEV(salary) FROM employees;
SELECT VARIANCE(salary) FROM employees;
```

### Conditional Functions
```sql
-- IFF (inline if)
SELECT IFF(salary > 50000, 'High', 'Low') AS salary_band
FROM employees;

-- CASE
SELECT 
    CASE 
        WHEN salary > 80000 THEN 'High'
        WHEN salary BETWEEN 50000 AND 80000 THEN 'Medium'
        ELSE 'Low'
    END AS salary_band
FROM employees;

-- COALESCE (first non-null value)
SELECT COALESCE(NULL, NULL, 'default', 'other');  -- default

-- NVL (replace null)
SELECT NVL(column_name, 'default_value') FROM table_name;

-- NULLIF (return null if equal)
SELECT NULLIF(column1, column2) FROM table_name;
```

### Window Functions
```sql
-- ROW_NUMBER
SELECT name, salary, 
    ROW_NUMBER() OVER (ORDER BY salary DESC) AS row_num
FROM employees;

-- RANK and DENSE_RANK
SELECT name, salary,
    RANK() OVER (ORDER BY salary DESC) AS rank,
    DENSE_RANK() OVER (ORDER BY salary DESC) AS dense_rank
FROM employees;

-- LEAD and LAG
SELECT name, salary,
    LAG(salary) OVER (ORDER BY salary) AS prev_salary,
    LEAD(salary) OVER (ORDER BY salary) AS next_salary
FROM employees;

-- Partition by
SELECT department, name, salary,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS dept_rank
FROM employees;
```

### Conversion Functions
```sql
-- To number
SELECT TO_NUMBER('123.45');
SELECT TRY_TO_NUMBER('invalid');       -- Returns NULL instead of error

-- To date/timestamp
SELECT TO_DATE('2024-01-15', 'YYYY-MM-DD');
SELECT TO_TIMESTAMP('2024-01-15 10:30:00', 'YYYY-MM-DD HH24:MI:SS');

-- To string
SELECT TO_CHAR(123.45, '999.99');
SELECT TO_VARCHAR(CURRENT_DATE(), 'YYYY-MM-DD');
```

### User-Defined Functions (UDF)

#### SQL UDF
```sql
CREATE OR REPLACE FUNCTION add_numbers(a NUMBER, b NUMBER)
RETURNS NUMBER
AS
$$
    a + b
$$;

-- Call the function
SELECT add_numbers(10, 20);  -- 30
```

#### JavaScript UDF
```sql
CREATE OR REPLACE FUNCTION js_multiply(a NUMBER, b NUMBER)
RETURNS NUMBER
LANGUAGE JAVASCRIPT
AS
$$
    return a * b;
$$;

-- Call the function
SELECT js_multiply(5, 6);  -- 30
```

#### Python UDF
```sql
CREATE OR REPLACE FUNCTION py_factorial(n NUMBER)
RETURNS NUMBER
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'factorial'
AS
$$
def factorial(n):
    if n <= 1:
        return 1
    return n * factorial(n - 1)
$$;

-- Call the function
SELECT py_factorial(5);  -- 120
```

### Show and Drop Functions
```sql
-- Show functions
SHOW USER FUNCTIONS;

-- Describe function
DESCRIBE FUNCTION add_numbers(NUMBER, NUMBER);

-- Drop function
DROP FUNCTION IF EXISTS add_numbers(NUMBER, NUMBER);
```

---

## Stored Procedures

### Create Stored Procedure
```sql
CREATE OR REPLACE PROCEDURE update_employee_salary(emp_id NUMBER, increase_pct NUMBER)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var sql_command = `
        UPDATE employees 
        SET salary = salary * (1 + ${INCREASE_PCT} / 100)
        WHERE id = ${EMP_ID}
    `;
    
    try {
        var stmt = snowflake.createStatement({sqlText: sql_command});
        var result = stmt.execute();
        return 'Success: Salary updated';
    } catch (err) {
        return 'Error: ' + err.message;
    }
$$;
```

### Call Stored Procedure
```sql
CALL update_employee_salary(123, 10);
```

### Stored Procedure with Result Set
```sql
CREATE OR REPLACE PROCEDURE get_high_earners(min_salary NUMBER)
RETURNS TABLE(id NUMBER, name STRING, salary NUMBER)
LANGUAGE SQL
AS
$$
    DECLARE
        res RESULTSET DEFAULT (
            SELECT id, name, salary 
            FROM employees 
            WHERE salary > :min_salary
        );
    BEGIN
        RETURN TABLE(res);
    END;
$$;

-- Call and use results
CALL get_high_earners(80000);
```

### Show and Drop Procedures
```sql
-- Show procedures
SHOW PROCEDURES;

-- Describe procedure
DESCRIBE PROCEDURE update_employee_salary(NUMBER, NUMBER);

-- Drop procedure
DROP PROCEDURE IF EXISTS update_employee_salary(NUMBER, NUMBER);
```

---

## Snowpipe

### Create Snowpipe
```sql
CREATE OR REPLACE PIPE my_pipe
AUTO_INGEST = TRUE
AS
COPY INTO my_table
FROM @my_stage
FILE_FORMAT = (FORMAT_NAME = 'my_csv_format');
```

### Create Notification Integration
```sql
-- For AWS S3
CREATE OR REPLACE NOTIFICATION INTEGRATION my_s3_integration
TYPE = QUEUE
ENABLED = TRUE
NOTIFICATION_PROVIDER = AWS_SNS
DIRECTION = INBOUND
AWS_SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:123456789012:my-topic';

-- For Azure
CREATE OR REPLACE NOTIFICATION INTEGRATION my_azure_integration
TYPE = QUEUE
ENABLED = TRUE
NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://myaccount.queue.core.windows.net/myqueue'
AZURE_TENANT_ID = 'your-tenant-id';

-- For GCP
CREATE OR REPLACE NOTIFICATION INTEGRATION my_gcp_integration
TYPE = QUEUE
ENABLED = TRUE
NOTIFICATION_PROVIDER = GCP_PUBSUB
GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/my-project/subscriptions/my-subscription';
```

### Manual Refresh
```sql
ALTER PIPE my_pipe REFRESH;
```

### Pause/Resume Pipe
```sql
-- Pause
ALTER PIPE my_pipe SET PIPE_EXECUTION_PAUSED = TRUE;

-- Resume
ALTER PIPE my_pipe SET PIPE_EXECUTION_PAUSED = FALSE;
```

### Show Pipes
```sql
SHOW PIPES;
```

### Describe Pipe
```sql
DESCRIBE PIPE my_pipe;
```

### Check Pipe Status
```sql
SELECT * FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
    PIPE_NAME => 'my_pipe',
    START_TIME => DATEADD('day', -7, CURRENT_TIMESTAMP())
));
```

### View Copy History
```sql
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'my_table',
    START_TIME => DATEADD('day', -7, CURRENT_TIMESTAMP())
));
```

### Drop Pipe
```sql
DROP PIPE IF EXISTS my_pipe;
```

---

## Encryption

### Default Encryption
Snowflake automatically encrypts all data:
- **At Rest**: AES-256 encryption
- **In Transit**: TLS 1.2+ encryption

### Client-Side Encryption (Base64 Example)
```sql
-- Encrypt function
CREATE OR REPLACE FUNCTION encrypt_base64(input_string STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    return btoa(input_string);
$$;

-- Decrypt function
CREATE OR REPLACE FUNCTION decrypt_base64(encrypted_string STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    return atob(encrypted_string);
$$;

-- Usage
SELECT encrypt_base64('sensitive_data');
SELECT decrypt_base64('c2Vuc2l0aXZlX2RhdGE=');
```

### External Key Management
For production use, integrate with:
- **AWS KMS** - AWS Key Management Service
- **Azure Key Vault** - Azure key management
- **GCP KMS** - Google Cloud Key Management

---

## Query History

### View Query History
```sql
SELECT 
    query_id,
    query_text,
    user_name,
    warehouse_name,
    execution_status,
    start_time,
    end_time,
    total_elapsed_time
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE user_name = CURRENT_USER()
ORDER BY start_time DESC
LIMIT 100;
```

### Filter by Status
```sql
SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE execution_status = 'SUCCESS'
  AND start_time >= DATEADD('day', -1, CURRENT_TIMESTAMP());
```

### Query by Session
```sql
SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION())
WHERE session_id = CURRENT_SESSION();
```

---

## Best Practices

### Connection Management
- Use environment variables for credentials
- Always close connections and cursors
- Use connection pooling for high-volume applications
- Implement retry logic for transient errors

### Error Handling
```python
import snowflake.connector
from snowflake.connector.errors import ProgrammingError

try:
    conn = snowflake.connector.connect(...)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM table_name")
except ProgrammingError as e:
    print(f"Error: {e}")
finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
```

### Performance Optimization
- Use `LIMIT` for exploratory queries
- Leverage clustering keys for large tables
- Use materialized views for frequently accessed aggregations
- Partition large tables appropriately
- Use `RESULT_SCAN` to reuse query results
- Monitor warehouse usage and right-size

### Security Best Practices
- Use role-based access control (RBAC)
- Implement least privilege principle
- Enable multi-factor authentication (MFA)
- Regularly rotate passwords and keys
- Use network policies to restrict access
- Enable query tags for auditing

### Cost Optimization
- Set appropriate `AUTO_SUSPEND` times
- Use `AUTO_RESUME` to avoid manual starts
- Right-size warehouses for workloads
- Use multi-cluster warehouses for concurrency
- Monitor credit usage regularly
- Use resource monitors to set spending limits

### Data Loading Best Practices
- Use `COPY INTO` instead of `INSERT` for bulk loads
- Compress files before loading
- Use appropriate file formats (Parquet for analytics)
- Implement error handling with `ON_ERROR` parameter
- Use Snowpipe for continuous loading
- Validate data before loading

### SQL Best Practices
- Always use `IF EXISTS` / `IF NOT EXISTS` clauses
- Use explicit column names in `INSERT` statements
- Avoid `SELECT *` in production code
- Use CTEs for complex queries
- Comment complex SQL logic
- Use transactions for related operations

---

## Additional Resources

- [Snowflake Documentation](https://docs.snowflake.com/)
- [Snowflake Community](https://community.snowflake.com/)
- [Snowflake University](https://learn.snowflake.com/)
- [Python Connector Documentation](https://docs.snowflake.com/en/user-guide/python-connector.html)

---

**Last Updated**: January 2024

This cheatsheet provides a comprehensive reference for common Snowflake operations. For advanced features and detailed documentation, please refer to the official Snowflake documentation.

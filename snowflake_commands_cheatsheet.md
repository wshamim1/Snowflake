
# Snowflake SQL & Python Commands Cheatsheet


  ### Connecting to Snowflake (Python)
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
  ```

  ---

  ### Basic SQL Commands
  ```sql
  SELECT * FROM table_name;
  INSERT INTO table_name (column1, column2) VALUES ('value1', 'value2');
  UPDATE table_name SET column1 = 'new_value' WHERE column2 = 'condition';
  DELETE FROM table_name WHERE column2 = 'condition';
  ```

  ---

  ### Table Management
  ```sql
  CREATE OR REPLACE TABLE table_name (
      column1 VARCHAR,
      column2 INT
  );
  DROP TABLE IF EXISTS table_name;
  ```

  ---

  ### Warehouse & Database Management
  ```sql
  CREATE WAREHOUSE my_wh WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;
  ALTER WAREHOUSE my_wh RESUME;
  ALTER WAREHOUSE my_wh SUSPEND;
  CREATE DATABASE my_db;
  USE DATABASE my_db;
  USE SCHEMA my_schema;
  USE WAREHOUSE my_wh;
  ```

  ---

  ### Data Loading & Unloading
  ```sql
  COPY INTO my_table FROM @my_stage FILE_FORMAT = (TYPE = 'CSV');
  COPY INTO @my_stage FROM my_table FILE_FORMAT = (TYPE = 'CSV');
  ```

  ---

  ### User & Role Management
  ```sql
  CREATE USER my_user PASSWORD = 'password';
  CREATE ROLE my_role;
  GRANT ROLE my_role TO USER my_user;
  GRANT SELECT ON table_name TO ROLE my_role;
  ```

  ---

  ### Useful Functions
  ```sql
  SELECT UPPER('snowflake');
  SELECT SQRT(16);
  SELECT CURRENT_DATE;
  SELECT COUNT(*) FROM my_table;
  SELECT IFF(column > 0, 'Yes', 'No') FROM my_table;
  SELECT COALESCE(NULL, 'default');
  ```

  ---

  ### Best Practices
  - Use environment variables for credentials in Python.
  - Always close connections and cursors.
  - Use try/except for error handling in Python.
  - Use `IF EXISTS`/`IF NOT EXISTS` to avoid errors on create/drop.

  ---

  This cheatsheet covers the most common Snowflake SQL and Python commands for quick reference. For advanced features (stages, file formats, Snowpipe, UDFs, etc.), see the official Snowflake documentation.
Database:
Create Database
CREATE DATABASE <database_name>;
Drop Database
DROP DATABASE <database_name>;
List All Database
show databases;
Schema Management
Create Schema
CREATE SCHEMA <schema_name>;
Drop Schema
DROP SCHEMA <schema_name>;
List Schema
use database <database_name>;
show schemas;


Or 

show schemas in <database_name>;
Switch to Database/Schema
USE DATABASE <database_name>;
USE SCHEMA <schema_name>;
Warehouse Management
List of warehouses size:
Press enter or click to view image in full size

List all Warehouse
show warehouses;
Create Warehouse
CREATE WAREHOUSE <warehouse_name>
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;
Alter Warehouse
ALTER WAREHOUSE <warehouse_name>
  SET WAREHOUSE_SIZE = 'MEDIUM';
Start/Stop Warehouse
-- Start warehouse
ALTER WAREHOUSE <warehouse_name> RESUME;
-- Stop warehouse
ALTER WAREHOUSE <warehouse_name> SUSPEND;
Drop Warehouse
DROP WAREHOUSE <warehouse_name>;
Table Management
Create Table
CREATE OR REPLACE TABLE <table_name> (
  id INT,
  name STRING,
  created_at TIMESTAMP
);
Here are the list of different data types :
Press enter or click to view image in full size

Press enter or click to view image in full size

Press enter or click to view image in full size

Press enter or click to view image in full size

Press enter or click to view image in full size

Create Table from Select
CREATE TABLE <new_table> AS
SELECT * FROM <existing_table> WHERE <condition>;
Insert Data into Table
INSERT INTO <table_name> (id, name, created_at)
VALUES (1, 'John Doe', CURRENT_TIMESTAMP);
Update Table Data
UPDATE <table_name>
SET name = 'Jane Doe'
WHERE id = 1;
Delete Table Data
DELETE FROM <table_name> WHERE id = 1;
Drop Table
DROP TABLE <table_name>;
Querying Data
Select Data
SELECT * FROM <table_name>;

-- Conditional select
SELECT * FROM <table_name> WHERE <condition>;
-- Select specific columns
SELECT column1, column2 FROM <table_name>;
Filtering and Sorting
-- Filtering with WHERE clause
SELECT * FROM <table_name> WHERE <condition>;

-- Sorting results
SELECT * FROM <table_name> ORDER BY <column_name> ASC|DESC;
Aggregation
-- Aggregate functions
SELECT COUNT(*), AVG(column), SUM(column)
FROM <table_name>
GROUP BY <group_column>;
Joins:
Press enter or click to view image in full size

Inner Join
Description: Returns rows where there is a match in both tables.
SELECT a.*, b.*
FROM <table_a> a
JOIN <table_b> b ON a.id = b.id;
Left Join (Left Outer Join)
Description: Returns all rows from the left table, and the matching rows from the right table. If no match, the result is NULL for the right table's columns.
SELECT a.*, b.*
FROM <table_a> a
LEFT JOIN <table_b> b ON a.id = b.id;
Right Join (Right Outer Join)
Description: Returns all rows from the right table, and the matching rows from the left table. If no match, the result is NULL for the left table's columns.
SELECT a.*, b.*
FROM <table_a> a
RIGHT JOIN <table_b> b ON a.id = b.id;
Full Join (Full Outer Join)
Description: Returns all rows when there is a match in either the left or right table. Non-matching rows will have NULL values for the missing side.
SELECT a.*, b.*
FROM <table_a> a
FULL JOIN <table_b> b ON a.id = b.id;
Cross Join
Description: Returns the Cartesian product of both tables. Every row in the left table is combined with every row in the right table.
SELECT a.*, b.*
FROM <table_a> a
CROSS JOIN <table_b> b;
Self Join
Description: Joins a table with itself. Useful for hierarchical data or recursive queries.
SELECT a.*, b.*
FROM <table_a> a
JOIN <table_a> b ON a.parent_id = b.id;
Natural Join
Description: Performs an implicit join based on columns with the same name in both tables. Essentially an inner join without an explicit condition.
SELECT *
FROM <table_a>
NATURAL JOIN <table_b>;
Subqueries
SELECT * FROM <table_name>
WHERE id IN (SELECT id FROM <other_table> WHERE <condition>);
File Management
Load Data into Table (COPY INTO)
COPY INTO <table_name>
FROM @<stage_name>
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');
Unload Data from Table (COPY INTO)
COPY INTO @<stage_name>
FROM <table_name>
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');
Merge Command
The MERGE command is used to merge data from a source into a target table, typically to update existing records and insert new ones.

Syntax:
MERGE INTO <target_table> AS tgt
USING <source_table> AS src
ON tgt.id = src.id
WHEN MATCHED THEN
    UPDATE SET tgt.name = src.name
WHEN NOT MATCHED THEN
    INSERT (id, name) VALUES (src.id, src.name);
WHEN MATCHED: Updates records in the target table if the join condition is met.
WHEN NOT MATCHED: Inserts new records if the join condition fails (i.e., new data).
Example:
MERGE INTO customers tgt
USING new_customers src
ON tgt.customer_id = src.customer_id
WHEN MATCHED THEN
    UPDATE SET tgt.email = src.email, tgt.phone = src.phone
WHEN NOT MATCHED THEN
    INSERT (customer_id, email, phone) VALUES (src.customer_id, src.email, src.phone);
File Format Types in Snowflake
Snowflake supports several file formats for loading and unloading data. Here are the common file formats:

Press enter or click to view image in full size

Example of Creating File Format:
CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"';

CREATE OR REPLACE FILE FORMAT my_json_format
  TYPE = 'JSON';

CREATE OR REPLACE FILE FORMAT my_parquet_format
  TYPE = 'PARQUET';
S3 or GCP File Management
Loading Data into a Table (COPY INTO) from S3 or GCP
You can load data from cloud storage platforms like Amazon S3 or Google Cloud Storage (GCS) using the COPY INTO command.

S3 Example:
COPY INTO <table_name>
FROM s3://<bucket_name>/<path> CREDENTIALS = (
  AWS_KEY_ID = '<your_aws_key>'
  AWS_SECRET_KEY = '<your_aws_secret>'
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');
GCP Example:
COPY INTO <table_name>
FROM 'gcs://<bucket_name>/<path>' CREDENTIALS = (
  GCP_KEY = '<your_gcp_key>'
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');
Unloading Data from a Table (COPY INTO) to S3 or GCP
S3 Example:
COPY INTO s3://<bucket_name>/<path>
FROM <table_name>
CREDENTIALS = (
  AWS_KEY_ID = '<your_aws_key>'
  AWS_SECRET_KEY = '<your_aws_secret>'
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');
GCP Example:
COPY INTO 'gcs://<bucket_name>/<path>'
FROM <table_name>
CREDENTIALS = (
  GCP_KEY = '<your_gcp_key>'
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');
Additional Copy Options
AUTO_DETECT: When set to TRUE, Snowflake automatically detects the structure of the files (columns, types, etc.).
ON_ERROR: Defines the behavior when errors are encountered.
CONTINUE: Continue loading, skip problematic rows.
SKIP_FILE: Skip the entire file if any error occurs.
ABORT_STATEMENT: Abort the process if any error occurs.
Example with Options:
COPY INTO <table_name>
FROM @my_stage
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE'
AUTO_DETECT = TRUE;
Stages:
A stage is a location where data files are stored before being loaded into Snowflake tables.

Internal Stage: Managed by Snowflake.
External Stage: An external location such as AWS S3 or GCP.
Create Internal Stage:
CREATE OR REPLACE STAGE my_internal_stage
FILE_FORMAT = (TYPE = 'CSV');
Create External Stage (S3 Example):
CREATE OR REPLACE STAGE my_s3_stage
URL = 's3://<bucket_name>/<path>'
CREDENTIALS = (
  AWS_KEY_ID = '<your_aws_key>'
  AWS_SECRET_KEY = '<your_aws_secret>'
)
FILE_FORMAT = (TYPE = 'CSV');
Listing Stages
You can list all the stages in your Snowflake account or a specific schema using the SHOW STAGES command.

List All Stages
SHOW STAGES;
This command lists both internal and external stages available in the current schema, along with their properties such as storage location, credentials, and file format.

List Stages in a Specific Schema
SHOW STAGES IN SCHEMA my_schema;
Describing Stages
To get detailed information about a specific stage, use the DESCRIBE STAGE command.

DESCRIBE STAGE my_s3_stage;
This will display:

Stage URL (for external stages),
File format associated with the stage,
Credentials used, and
Storage details (for internal stages).
Deleting (Dropping) Stages
When you no longer need a stage, you can drop it using the DROP STAGE command. Dropping a stage does not delete the data from the external storage (for external stages), only the link between Snowflake and the stage is removed.

Drop Internal Stage
DROP STAGE IF EXISTS my_internal_stage;
Drop External Stage
DROP STAGE IF EXISTS my_s3_stage;
The IF EXISTS clause ensures that no error is raised if the stage does not exist.
List Files in an Internal Stage
LIST @my_internal_stage;
List Files in an External Stage (AWS S3 Example)
LIST @my_s3_stage;
This command will display the file names, sizes, and last modified timestamps of files available in the specified stage.

Load Data into Table from Stage
COPY INTO my_table
FROM @my_internal_stage
FILE_FORMAT = (TYPE = 'CSV');
Unload Data from Table to Stage
COPY INTO @my_internal_stage
FROM my_table
FILE_FORMAT = (TYPE = 'CSV');
User and Role Management
Create User
CREATE USER <user_name>
PASSWORD = '<password>'
DEFAULT_ROLE = '<role_name>'
DEFAULT_WAREHOUSE = '<warehouse_name>'
DEFAULT_NAMESPACE = '<db_name.schema_name>';
Alter User
ALTER USER <user_name>
SET PASSWORD = '<new_password>';
Drop User
DROP USER <user_name>;
Create Role
CREATE ROLE <role_name>;
Grant Role to User
GRANT ROLE <role_name> TO USER <user_name>;
Grant Privileges to Role
GRANT SELECT ON <table_name> TO ROLE <role_name>;
Revoke Privileges from Role
REVOKE SELECT ON <table_name> FROM ROLE <role_name>;
Drop Role
DROP ROLE <role_name>;
Time Travel and Cloning
Query Historical Data (Time Travel)
SELECT * FROM <table_name> AT (TIMESTAMP => 'YYYY-MM-DD HH24:MI:SS');
Clone Table
CREATE OR REPLACE TABLE <new_table_name> CLONE <existing_table_name>;
Undrop a Table (Recover Dropped Table)
UNDROP TABLE <table_name>;
Transaction Management
Start Transaction
BEGIN;
Commit Transaction
COMMIT;
Rollback Transaction
ROLLBACK;
Query History
View Query History
SELECT *
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE USER_NAME = '<user_name>'
AND EXECUTION_STATUS = 'SUCCESS';
Functions:
1. System-Defined Functions in Snowflake
Snowflake provides a wide range of built-in system functions, categorized into various types such as string, numeric, date/time, conversion, and aggregate functions.

String Functions
-- Convert to uppercase
SELECT UPPER('snowflake');
-- Get substring
SELECT SUBSTRING('snowflake', 1, 4);  -- Returns 'snow'
-- Trim spaces from both sides
SELECT TRIM('  snowflake  ');
Numeric Functions
-- Calculate the square root
SELECT SQRT(16);  -- Returns 4

-- Round a number to 2 decimal places
SELECT ROUND(123.456, 2);  -- Returns 123.46
-- Find the absolute value
SELECT ABS(-42);  -- Returns 42
Date/Time Functions
-- Get current date
SELECT CURRENT_DATE;

-- Add 10 days to a date
SELECT DATEADD('day', 10, '2024-01-01');  -- Returns '2024-01-11'
-- Calculate the difference between two dates
SELECT DATEDIFF('day', '2024-01-01', '2024-01-10');  -- Returns 9
Aggregate Functions
-- Get sum of a column
SELECT SUM(salary) FROM employees;
-- Get average of a column
SELECT AVG(age) FROM employees;
-- Count number of records
SELECT COUNT(*) FROM employees;
2. User-Defined Functions (UDFs) in Snowflake
Snowflake allows you to create custom functions, known as User-Defined Functions (UDFs), in SQL, JavaScript, or Python.

Creating SQL-Based UDF
CREATE OR REPLACE FUNCTION add_numbers(a NUMBER, b NUMBER)
RETURNS NUMBER
AS
$$
  a + b
$$;
Calling the UDF
SELECT add_numbers(10, 20);  -- Returns 30
Creating JavaScript-Based UDF
CREATE OR REPLACE FUNCTION js_add(a NUMBER, b NUMBER)
RETURNS NUMBER
LANGUAGE JAVASCRIPT
AS
$$
  return a + b;
$$;
Calling JavaScript UDF
SELECT js_add(10, 15);  -- Returns 25
List Existing Functions
You can view the list of user-defined functions in the system using the SHOW FUNCTIONS command.

SHOW USER FUNCTIONS;
Dropping a Function
To remove a user-defined function, use the DROP FUNCTION command.

DROP FUNCTION IF EXISTS add_numbers(NUMBER, NUMBER);
Describing a Function
To view the definition of an existing function:

DESCRIBE FUNCTION add_numbers(NUMBER, NUMBER);
4. Analytical Functions
Snowflake provides advanced analytical functions for performing windowing operations.

ROW_NUMBER()
Assigns a unique row number for each row in the result set.

SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rank
FROM employees;
RANK()
Assigns ranks based on a specified order, with ties receiving the same rank.

SELECT name, salary, RANK() OVER (ORDER BY salary DESC) AS rank
FROM employees;
LEAD() and LAG()
Get values from preceding or following rows.

-- Get the next salary (lead) for each employee
SELECT name, salary, LEAD(salary) OVER (ORDER BY salary DESC) AS next_salary
FROM employees;
5. Conversion Functions
These functions help convert data between different types.

-- Convert string to integer
SELECT TO_NUMBER('123');
-- Convert date to string
SELECT TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD');
-- Convert string to timestamp
SELECT TO_TIMESTAMP('2024-01-01 12:30:00');
6. Conditional Functions
IIF() (Inline If)
SELECT IFF(salary > 5000, 'High', 'Low') AS salary_band FROM employees;
CASE
SELECT
  CASE
    WHEN salary > 5000 THEN 'High'
    WHEN salary BETWEEN 3000 AND 5000 THEN 'Medium'
    ELSE 'Low'
  END AS salary_band
FROM employees;
7. Handling Nulls
COALESCE()
Returns the first non-null value in a list.

SELECT COALESCE(NULL, NULL, 'Default Value');  -- Returns 'Default Value'
NVL()
Replaces NULL with a specified value.

SELECT NVL(NULL, 'Default Value');  -- Returns 'Default Value'
1. Client-Side Encryption/Decryption
For client-side encryption, you’ll need to encrypt your data before loading it into Snowflake. This typically involves using external tools or libraries to encrypt data (such as OpenSSL, AWS KMS, or GCP KMS).

However, if you want to perform encryption and decryption operations within Snowflake, you can use UDFs written in JavaScript or Python to customize these operations.

2. Creating Encryption and Decryption Functions
Encryption Using Base64 Encoding
This is a simple example using Base64 encoding to “encrypt” data, though it is not secure encryption. For proper encryption, external libraries or tools (such as AES encryption) should be used.

CREATE OR REPLACE FUNCTION encrypt_base64(input_string STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  return btoa(input_string);
$$;
Decryption Using Base64 Decoding
CREATE OR REPLACE FUNCTION decrypt_base64(encrypted_string STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  return atob(encrypted_string);
$$;
Usage Example

-- Encrypt a string
SELECT encrypt_base64('my_secret_data');  -- Returns encrypted data

-- Decrypt the string
SELECT decrypt_base64('bXlfc2VjcmV0X2RhdGE=');  -- Returns 'my_secret_data'
3. Using External Encryption Libraries (JavaScript or Python UDFs)
For more secure encryption algorithms like AES (Advanced Encryption Standard), Snowflake UDFs can call external libraries via JavaScript or Python.

Example: AES Encryption in JavaScript UDF
CREATE OR REPLACE FUNCTION aes_encrypt(input_string STRING, key STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  const crypto = require('crypto');
  const cipher = crypto.createCipher('aes-256-cbc', key);
  let encrypted = cipher.update(input_string, 'utf8', 'hex');
  encrypted += cipher.final('hex');
  return encrypted;
$$;
Example: AES Decryption in JavaScript UDF
CREATE OR REPLACE FUNCTION aes_decrypt(encrypted_string STRING, key STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  const crypto = require('crypto');
  const decipher = crypto.createDecipher('aes-256-cbc', key);
  let decrypted = decipher.update(encrypted_string, 'hex', 'utf8');
  decrypted += decipher.final('utf8');
  return decrypted;
$$;
Usage Example

-- Encrypt a string using AES
SELECT aes_encrypt('my_secret_data', 'encryption_key');

-- Decrypt the string using AES
SELECT aes_decrypt('<encrypted_data>', 'encryption_key');
4. External Key Management System (KMS)
For advanced encryption and decryption workflows, you can integrate with an external Key Management System (KMS) like AWS KMS or Google Cloud KMS. Here’s an example of how to manage encryption keys using these services:

Example Using AWS KMS:
Generate a Data Encryption Key (DEK) using AWS KMS.
Encrypt the data on the client-side with the DEK.
Load the encrypted data into Snowflake.
For secure encryption and decryption, use AWS SDK or GCP SDK for key management.

5. Snowflake Encryption Defaults
By default, Snowflake encrypts all data stored in tables, stages, and files using AES-256 encryption. This encryption is managed by Snowflake and does not require additional configuration.

In-Transit Encryption: Data is encrypted with TLS (Transport Layer Security) when moving between the client and Snowflake.
At-Rest Encryption: All data is automatically encrypted at rest using AES-256.
Stored Procedure:
1. Creating a Stored Procedure
CREATE OR REPLACE PROCEDURE procedure_name(argument_name DATA_TYPE, ...)
RETURNS RETURN_DATA_TYPE
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER  -- 'CALLER' or 'OWNER'
AS
$$
  // JavaScript code block
  var result = ...; 
  return result;
$$;
procedure_name: Name of the stored procedure.
argument_name: Input arguments with their data types.
RETURNS: The data type that the procedure returns.
LANGUAGE JAVASCRIPT: Specifies that the procedure is written in JavaScript.
EXECUTE AS CALLER/OWNER: Determines if the procedure runs with the caller's or owner's privileges.
2. Calling a Stored Procedure
CALL procedure_name(argument_value, ...);
procedure_name: The name of the procedure to be called.
argument_value: Pass values that correspond to the arguments defined in the procedure.
3. Handling Errors (Try/Catch)
Within the JavaScript block of the stored procedure, you can handle errors with try/catch.

$$
try {
    // Code block
} catch (err) {
    return 'Error: ' + err.message;
}
$$;
4. Using SQL Inside Stored Procedures
Use the Snowflake-provided snowflake.createStatement function to execute SQL queries within a stored procedure.

var stmt = snowflake.createStatement({sqlText: 'SELECT * FROM MY_TABLE'});
var result = stmt.execute();
5. Looping Through Result Sets
var stmt = snowflake.createStatement({sqlText: 'SELECT col1, col2 FROM my_table'});
var result = stmt.execute();
while (result.next()) {
    var col1_value = result.getColumnValue(1);
    var col2_value = result.getColumnValue(2);
    // Logic for each row
}
6. Returning a Value
return 'Success';  // Can return a string, number, object, etc.
7. Dropping a Stored Procedure
DROP PROCEDURE IF EXISTS procedure_name(argument_type, ...);
You need to specify the data types of the arguments to uniquely identify the procedure.
8. Listing Stored Procedures
SHOW PROCEDURES;
This will list all stored procedures in the current database and schema.

9. Viewing the Definition of a Stored Procedure
DESC PROCEDURE procedure_name(argument_type, ...);
10. Altering a Stored Procedure
Unfortunately, Snowflake does not support altering a stored procedure directly. You need to recreate it using CREATE OR REPLACE PROCEDURE.

Snowpipe:
1. Create a Snowpipe
CREATE OR REPLACE PIPE pipe_name
AUTO_INGEST = TRUE
AS
COPY INTO target_table
FROM @stage_name
FILE_FORMAT = (format_name = 'file_format_name');
pipe_name: Name of the Snowpipe.
AUTO_INGEST: If TRUE, enables automatic loading of data when new files are staged.
target_table: The table where the data will be loaded.
stage_name: The stage (internal/external) from which the files are ingested.
file_format_name: The file format for the staged files.
2. Manually Trigger Snowpipe
If auto-ingestion is not enabled, you can trigger Snowpipe manually.

ALTER PIPE pipe_name REFRESH;
This forces Snowpipe to check for new files in the specified stage and load them.

3. View Snowpipe Status
To check the status of your Snowpipe, including any file load history:

SELECT * FROM table(information_schema.copy_history(table_name => 'target_table', start_time => DATE '2024-09-01'));
You can adjust the start time to review recent ingestion activity.

4. Check Files in Stage
To see what files are staged (and may be loaded by Snowpipe):

LIST @stage_name;
This shows the list of files currently in the stage.

5. Pausing a Snowpipe
ALTER PIPE pipe_name SET PIPE_EXECUTION_PAUSED = TRUE;
This command will pause automatic data ingestion by the specified pipe.

6. Resuming a Snowpipe
ALTER PIPE pipe_name SET PIPE_EXECUTION_PAUSED = FALSE;
This resumes data ingestion after it has been paused.

7. Drop a Snowpipe
To completely remove a Snowpipe:

DROP PIPE IF EXISTS pipe_name;
8. View Existing Pipes
To see a list of all the Snowpipes:

SHOW PIPES;
This will return all the pipes created in the current database and schema.

9. View the Definition of a Snowpipe
DESCRIBE PIPE pipe_name;
This command displays the SQL definition and additional metadata for the specified Snowpipe.

10. Monitor Snowpipe Activity with Query History
To review recent Snowpipe activity, use QUERY_HISTORY:

SELECT * FROM table(information_schema.query_history_by_session())
WHERE query_text ILIKE '%COPY INTO%'
AND execution_status = 'SUCCESS';
This query shows successful Snowpipe execution within the session, helping you monitor its performance.

11. Inspect the Pipe’s Load History
To view the history of files loaded by a specific Snowpipe:

SELECT *
FROM table(information_schema.pipe_usage_history)
WHERE pipe_name = 'pipe_name'
ORDER BY event_time DESC;
12. Create Notification Integration for Snowpipe
To enable Auto-Ingest with external stages (e.g., S3, GCS, Azure), you need to create a notification integration:

CREATE OR REPLACE NOTIFICATION INTEGRATION my_integration
TYPE = QUEUE
ENABLED = TRUE
NOTIFICATION_PROVIDER = 'AZURE'  -- or 'AWS' or 'GCP'
DIRECTION = 'OUTBOUND'
QUEUE_TOPIC = 'my_topic';
This creates the notification integration that Snowpipe will use to trigger automatic data ingestion when files are added to the stage.

13. Recreate a Snowpipe after Modifications
If you modify any related settings (e.g., stage or file format), recreate the Snowpipe with:

CREATE OR REPLACE PIPE pipe_name
AUTO_INGEST = TRUE
AS
COPY INTO target_table
FROM @stage_name
FILE_FORMAT = (FORMAT_NAME = 'file_format_name');
Example: Create a Snowpipe for Auto-Ingest from an S3 Stage
CREATE OR REPLACE PIPE my_snowpipe
AUTO_INGEST = TRUE
AS
COPY INTO my_table
FROM @my_s3_stage
FILE_FORMAT = (FORMAT_NAME = 'my_csv_format');
Example: Check Pipe Status
SELECT * FROM table(information_schema.pipe_usage_history)
WHERE pipe_name = 'my_snowpipe'
ORDER BY event_time DESC;
Conclusion
With Snowflake’s robust and versatile command set, users can easily manage data across various environments and platforms, making complex data operations more straightforward and efficient. Whether working with diverse file formats or loading/unloading data from external cloud storage, the commands covered in this cheat sheet will help you handle Snowflake’s key functionalities with ease. By leveraging these commands, you can ensure more streamlined data operations and better overall performance in your cloud data management tasks.


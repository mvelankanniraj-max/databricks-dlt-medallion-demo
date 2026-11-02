Understanding Auto Loader and Data Validation in Your Databricks Pipeline
I examined the SQL files and pipeline configuration inside your uploaded databricks-dlt-medallion-demo-main.zip.

Your project uses two important Databricks features:
1. Auto Loader — automatically discovers and incrementally ingests new CSV files from your landing folder.
2. Data quality expectations — validate records as they move from Bronze to Silver, dropping records that fail the defined rules.
The key distinction is that Auto Loader tracks which files have been processed, while data quality expectations track how many records pass or fail validation. They serve different purposes and maintain different types of state.
1. Where is Auto Loader enabled?
Your actual project file
01_bronze.sql

sql/01_bronze.sql

CREATE OR REFRESH STREAMING TABLE orders_bronze
COMMENT 'Raw orders ingested using Auto Loader'
AS
SELECT
    order_id,
    customer_id,
    TRY_CAST(order_date AS DATE) AS order_date,
    country,
    TRY_CAST(amount AS DECIMAL(10,2)) AS amount,
    _metadata.file_name AS source_file,
    CURRENT_TIMESTAMP() AS ingestion_timestamp
FROM STREAM read_files(
    '/Volumes/main/dlt_orders_demo/landing/orders/',
    format => 'csv',
    header => true,
    schema => '
        order_id STRING,
        customer_id STRING,
        order_date STRING,
        country STRING,
        amount STRING
    '
);
  



The most important statement is:
FROM STREAM read_files(
    '/Volumes/main/dlt_orders_demo/landing/orders/',
    format => 'csv',
    header => true,
    ...
)


read_files() is what activates Auto Loader in your SQL pipeline. It is Databricks' SQL interface for Auto Loader, equivalent in concept to using cloudFiles in PySpark.

The STREAM keyword tells Databricks that this is an incremental streaming source.
What each part does
SQL statement	Meaning
CREATE OR REFRESH STREAMING TABLE	Creates a streaming table managed by Lakeflow
STREAM read_files(...)	Uses Auto Loader to ingest new files
/Volumes/.../orders/	Location to scan for source files
format => 'csv'	Files are CSV format
header => true	First CSV row contains column names
schema => ...	Defines the expected input columns and types
_metadata.file_name	Captures the source filename for each record
CURRENT_TIMESTAMP()	Records an ingestion timestamp

You do not need to separately enable Auto Loader in the Databricks UI or configure cloudFiles in this project.
How the data flows
Unity Catalog landing volume

/Volumes/main/dlt_orders_demo/landing/orders/
orders_001.csv
orders_002.csv


Auto Loader discovers unprocessed files

read_files()
Reads CSV rows and tracks ingestion progress

orders_bronze
Raw ingested records, including rows that may fail business validation

Silver expectations
Evaluate rules and drop failing rows

orders_silver
Records passing all four validation rules

2. Where does Auto Loader maintain information about processed files?
Suppose you upload:
orders_001.csv


When you run the pipeline, Auto Loader discovers this file and processes its records.

On a later update, suppose you upload:
orders_002.csv


Auto Loader should ingest the new file without rereading the already processed file, provided the pipeline retains its existing streaming state.
How does it remember?
Auto Loader uses checkpoint and streaming state managed by the Lakeflow pipeline.

This state is used to track ingestion progress and which files have already been processed. It is not stored as ordinary rows in your Bronze or Silver table.

In your specific project, you have not configured an explicit checkpoint path. Because you are using a managed Lakeflow pipeline, Databricks manages the checkpoint and associated pipeline storage internally.

For example, you do not have a statement like:
.option("checkpointLocation", "/some/path")



And that is normal for this type of managed pipeline.
Two different storage responsibilities

Storage	What it maintains
Bronze Delta table	Ingested order records
Managed streaming checkpoint	File-processing progress and recovery state


Important: Avoid deleting or resetting the pipeline's streaming state unless you deliberately want to reprocess data. A full refresh can cause existing source files to be ingested again as part of rebuilding the table.
3. Where is data validation defined?
Your second important file is:

sql/02_silver.sql

Here is the actual validation logic from your uploaded project.
Your Silver-layer validation code

CREATE OR REFRESH STREAMING TABLE orders_silver (
    CONSTRAINT valid_order_id
      EXPECT (TRY_CAST(order_id AS INT) IS NOT NULL)
      ON VIOLATION DROP ROW,

    CONSTRAINT valid_customer
      EXPECT (customer_id IS NOT NULL
              AND TRIM(customer_id) <> '')
      ON VIOLATION DROP ROW,

    CONSTRAINT valid_amount
      EXPECT (amount > 100)
      ON VIOLATION DROP ROW,

    CONSTRAINT valid_order_date
      EXPECT (order_date IS NOT NULL)
      ON VIOLATION DROP ROW
)
COMMENT 'Validated orders above 100 dollars'
AS
SELECT
    order_id, customer_id, order_date,
    country, amount,
    source_file, ingestion_timestamp
FROM STREAM(orders_bronze);
  



The keyword EXPECT defines a data quality expectation.

Your pipeline has four expectations:
Expectation	Validation rule
valid_order_id	Order ID must be convertible to an integer
valid_customer	Customer ID cannot be null or empty
valid_amount	Amount must be greater than 100
valid_order_date	Order date must be valid and non-null

Each rule has:
ON VIOLATION DROP ROW


This means that when a record fails a rule, it is excluded from the Silver table.

The record remains in Bronze, but the Silver table will not contain it.
Example: What happens to individual records?
Interactive example — Bronze to Silver
Illustrative records to demonstrate how the rules work, not rows retrieved from your workspace.
All Bronze (5)Silver (2)Dropped (3)

Order ID	Amount	Result
1001	$250	Pass Passes all four rules
1002	$75	Dropped Amount is not greater than 100
ABC	$400	Dropped Order ID is not an integer
1004	$300	Dropped Customer ID is empty
1005	$500	Pass Passes all four rules

Rows displayed: 5
Reset

Notice that the same record can fail multiple expectations. For example, a record with an invalid order ID and a low amount fails two rules but is still only one dropped row.
4. Where does Databricks maintain data validation results?
This is the most important part of your second question.

Databricks maintains expectation metrics in the Lakeflow pipeline event log.

The event log is separate from your business data tables.
Pipeline data versus validation information
Business data

orders_bronze — ingested records

orders_silver — accepted records

daily_sales_gold — aggregated results


Pipeline event log

Expectation names, passed/failed counts, pipeline update details and operational events


Option A: View data quality metrics in the Databricks UI
This is the easiest method for a beginner.
1. Log in to your Databricks workspace.
2. Open Jobs & Pipelines.
3. Select dlt_orders_pipeline (the name may have a deployment prefix).
4. Open a completed pipeline update.
5. Select the orders_silver streaming table in the pipeline graph.
6. Look for the data quality or expectation metrics associated with the update.
You should be able to inspect metrics for the four expectations, subject to the metrics available for that run.
Option B: Query the pipeline event log using SQL
Your pipeline uses the main.dlt_orders_demo schema.

In supported Unity Catalog pipelines, you can expose the event log as a named table using the pipeline configuration:
event_log:
  name: pipeline_event_log


This is a change to the pipeline definition, not a command to run in the SQL editor.

In your project, add it to resources/dlt_pipeline.yml under dlt_orders_pipeline, at the same indentation level as catalog, schema, and serverless.

Then redeploy the bundle and run a new pipeline update. The published event log can be queried as:
SELECT *
FROM main.dlt_orders_demo.pipeline_event_log
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC;


To inspect expectation metrics:
SELECT
    timestamp,
    origin.update_id,
    details:flow_progress.data_quality.expectations
        AS expectation_metrics
FROM main.dlt_orders_demo.pipeline_event_log
WHERE event_type = 'flow_progress'
  AND details:flow_progress.data_quality.expectations IS NOT NULL
ORDER BY timestamp DESC;


This provides data quality metrics recorded for pipeline updates. The event log includes many other operational events, so the filter is important.

Note: The exact event-log publishing support and configuration depend on your Databricks workspace and pipeline version. If the table is not available, inspect the event log in the pipeline UI or check the workspace's event-log configuration.
What validation metrics look like
For example, an expectation might produce a record conceptually like:
{
  "name": "valid_amount",
  "passed_records": 7,
  "failed_records": 1
}


This is an illustrative example, not an actual event retrieved from your workspace.

The important fields are:
- name: Which expectation was evaluated.
- passed_records: Number of records satisfying the expectation.
- failed_records: Number of records failing the expectation.
Metrics are typically recorded for individual pipeline flows and updates, not as a permanent per-record validation history.
5. Does the event log contain the actual invalid records?
No — not automatically. This is a critical distinction.

Your current project uses:
ON VIOLATION DROP ROW


This means:
- The failed record is dropped from Silver.
- The original record remains in Bronze.
- The expectation metrics can report failures.
- The pipeline does not automatically create a separate table containing each failed record and its failure reason.
For example:
Order ID	Bronze	Silver	Event log
101	Present	Present	Counted as passing
102	Present	Absent if invalid	Counted as failing
103	Present	Present	Counted as passing

If you want to store each invalid record with a reason, you need to build a separate quarantine or rejected-records table.

This is a common data engineering design in production systems.
6. How to maintain invalid records in a separate table
For learning purposes, I recommend extending your project with a third table:

orders_rejected

The idea is to retain the original Bronze record and attach a reason explaining why it failed validation.
orders_bronze
All ingested records
Apply the same validation rules

orders_silver
Valid records


orders_rejected
Invalid records and reasons




Example: Create a rejected-records table
Create a new file in your project:

sql/05_rejected.sql

CREATE OR REFRESH STREAMING TABLE orders_rejected
COMMENT 'Orders rejected during validation'
AS
SELECT
    order_id,
    customer_id,
    order_date,
    country,
    amount,
    source_file,
    ingestion_timestamp,

    CONCAT_WS(', ',
        CASE
            WHEN TRY_CAST(order_id AS INT) IS NULL
            THEN 'Invalid Order ID'
        END,
        CASE
            WHEN customer_id IS NULL
              OR TRIM(customer_id) = ''
            THEN 'Invalid Customer'
        END,
        CASE
            WHEN amount IS NULL OR amount <= 100
            THEN 'Invalid Amount'
        END,
        CASE
            WHEN order_date IS NULL
            THEN 'Invalid Order Date'
        END
    ) AS rejection_reason

FROM STREAM(orders_bronze)

WHERE
    TRY_CAST(order_id AS INT) IS NULL
    OR customer_id IS NULL
    OR TRIM(customer_id) = ''
    OR amount IS NULL
    OR amount <= 100
    OR order_date IS NULL;



This example follows the same rules as your Silver table, including SQL null behavior for the amount check. An invalid record can have multiple rejection reasons.

To deploy this new table, also add the file to the libraries section of resources/dlt_pipeline.yml:
- file:
    path: ../sql/05_rejected.sql


Then redeploy and run the pipeline:
databricks bundle deploy -t dev --profile DEV

databricks bundle run dlt_orders_pipeline -t dev --profile DEV


After the update succeeds, query:
SELECT
    order_id,
    customer_id,
    amount,
    rejection_reason,
    source_file
FROM main.dlt_orders_demo.orders_rejected;


This gives you an actual table of rejected records for investigation and remediation.

One important design consideration: keep the rejection rules synchronized with the Silver expectations. Otherwise, a record could be dropped from Silver without being captured in the rejected table.
7. How to check the data in your existing pipeline
You can immediately run these queries without changing the project.

Check all Bronze records:
SELECT *
FROM main.dlt_orders_demo.orders_bronze;


Check valid Silver records:
SELECT *
FROM main.dlt_orders_demo.orders_silver;


Identify records that failed your Silver validation rules:
SELECT *
FROM main.dlt_orders_demo.orders_bronze
WHERE TRY_CAST(order_id AS INT) IS NULL
   OR customer_id IS NULL
   OR TRIM(customer_id) = ''
   OR amount IS NULL
   OR amount <= 100
   OR order_date IS NULL;


This last query identifies invalid Bronze records using your current rules. It does not require a new table, although it does not provide a persisted rejection history.
8. Important differences to remember
Feature	Auto Loader	Data quality expectations
Purpose	Ingest new files incrementally	Validate records
Defined in	01_bronze.sql	02_silver.sql
Main syntax	STREAM read_files()	CONSTRAINT ... EXPECT
Maintains state	Managed streaming checkpoint	Pipeline event log metrics
Tracks	Ingestion progress	Passing/failing records
Rejects invalid records	Not based on your business rules	Yes, with DROP ROW
Stores rejected rows	No	Not automatically

Three key concepts
1. Auto Loader is not a data validation engine. Its job is to discover and ingest files incrementally.
2. The Silver expectations validate data quality. Your project drops invalid rows from Silver and records expectation metrics in the pipeline event log.
3. A quarantine table is needed for detailed rejected-record storage. It preserves the actual failed records and their reasons.
For your next hands-on exercise, I can  walk you through the Databricks event log step by step so you can see how Databricks tracks both file ingestion and data quality in your actual workspace.

# Deploy your Databricks DLT Medallion Demo using Windows CLI

I reviewed your uploaded ZIP file, `databricks-dlt-medallion-demo-main.zip`. It contains a deployable Databricks Lakeflow Spark Declarative Pipeline (formerly Delta Live Tables / DLT) using Bronze, Silver, and Gold layers.

You can deploy it directly from your Windows machine using your existing `DEV` Databricks CLI profile.

Your deployment architecture

CSV files in Unity Catalog Volume

orders_001.csv → orders_002.csv

Bronze — orders_bronze

Auto Loader streaming ingestion

Silver — orders_silver

Data quality rules and invalid row filtering

Gold — daily_sales_gold

Aggregated daily sales by country

## Step 1 — Extract your ZIP file

Extract the uploaded ZIP file into a Windows folder, for example:

```
C:\Databricks\databricks-dlt-medallion-demo-main
```

Open PowerShell and navigate to the extracted folder:

```
cd C:\Databricks\databricks-dlt-medallion-demo-main
```

Verify the files:

```
dir
```

You should see:

```
databricks.yml
resources\
sql\
data\
docs\
README.md
```

## Step 2 — Verify your Databricks connection

Your workspace URL:

```
https://dbc-77b923d9-96ec.cloud.databricks.com
```

Run:

```
databricks auth profiles
```

Verify your `DEV` profile:

```
databricks current-user me --profile DEV
```

You should see your Databricks user details.

## Step 3 — Create the schema and volume

In your Databricks workspace, open the SQL Editor and run:

```
CREATE SCHEMA IF NOT EXISTS main.dlt_orders_demo;

CREATE VOLUME IF NOT EXISTS main.dlt_orders_demo.landing;
```

Your project expects these Unity Catalog resources:

| Resource | Name              |
| -------- | ----------------- |
| Catalog  | `main`            |
| Schema   | `dlt_orders_demo` |
| Volume   | `landing`         |
| Folder   | `orders`          |

The catalog `main` must exist, and you must have permission to create these resources.

## Step 4 — Upload your first CSV file

In PowerShell, run:

```
databricks fs mkdirs dbfs:/Volumes/main/dlt_orders_demo/landing/orders --profile DEV
```

Then upload your first CSV:

```
databricks fs cp .\data\orders_001.csv dbfs:/Volumes/main/dlt_orders_demo/landing/orders/orders_001.csv --profile DEV
```

Verify the uploaded file:

```
databricks fs ls dbfs:/Volumes/main/dlt_orders_demo/landing/orders/ --profile DEV
```

You should see `orders_001.csv`.

Do not upload `orders_002.csv` yet. We will use it later to test incremental ingestion.

## Step 5 — Validate your Databricks bundle

Your project already contains the required deployment configuration:

- `databricks.yml` — bundle configuration
- `resources/dlt_pipeline.yml` — pipeline definition
- `sql/01_bronze.sql` — Bronze streaming table
- `sql/02_silver.sql` — Silver quality rules
- `sql/03_gold.sql` — Gold materialized view

From the project root, run:

```
databricks bundle validate -t dev --profile DEV
```

If successful, this validates your deployment configuration.

## Step 6 — Deploy your pipeline

Execute:

```
databricks bundle deploy -t dev --profile DEV
```

This deploys the pipeline configuration and SQL source files into your Databricks workspace.

Your pipeline will be named:

```
dlt_orders_pipeline
```

The uploaded project is configured for serverless pipeline compute. Your workspace must support that capability.

## Step 7 — Run the pipeline

Execute:

```
databricks bundle run dlt_orders_pipeline -t dev --profile DEV
```

This triggers a pipeline update.

Open Databricks → Jobs & Pipelines, find the deployed pipeline, and check its run status.

When the update succeeds, the pipeline should have created:

```
main.dlt_orders_demo.orders_bronze
main.dlt_orders_demo.orders_silver
main.dlt_orders_demo.daily_sales_gold
```

## Step 8 — Validate Bronze, Silver, and Gold

Open Databricks SQL Editor and run:

```
SELECT COUNT(*) AS bronze_count
FROM main.dlt_orders_demo.orders_bronze;

SELECT COUNT(*) AS silver_count
FROM main.dlt_orders_demo.orders_silver;

SELECT *
FROM main.dlt_orders_demo.daily_sales_gold
ORDER BY order_date, country;
```

Based on your project's sample CSV, the expected initial results are:

Bronze rows

# 8

Silver rows

# 5

The Gold table should contain:

| Date       | Country | Orders | Sales |
| ---------- | ------- | ------ | ----- |
| 2026-10-28 | USA     | 2      | 750   |
| 2026-10-29 | India   | 1      | 300   |
| 2026-10-29 | USA     | 2      | 275   |

## Step 9 — Test incremental ingestion

Upload the second CSV:

```
databricks fs cp .\data\orders_002.csv dbfs:/Volumes/main/dlt_orders_demo/landing/orders/orders_002.csv --profile DEV
```

Run the pipeline again:

```
databricks bundle run dlt_orders_pipeline -t dev --profile DEV
```

Check the row counts:

```
SELECT COUNT(*) FROM main.dlt_orders_demo.orders_bronze;
SELECT COUNT(*) FROM main.dlt_orders_demo.orders_silver;
```

Expected results after both files:

| Layer  | Initial | After incremental load |
| ------ | ------- | ---------------------- |
| Bronze | 8       | 12                     |
| Silver | 5       | 8                      |

This demonstrates that Auto Loader processes newly arrived files incrementally.

## Step 10 — Useful deployment commands

| Purpose                 | PowerShell command                                               |
| ----------------------- | ---------------------------------------------------------------- |
| Validate                | `databricks bundle validate -t dev --profile DEV`                |
| Deploy                  | `databricks bundle deploy -t dev --profile DEV`                  |
| Run                     | `databricks bundle run dlt_orders_pipeline -t dev --profile DEV` |
| View deployment details | `databricks bundle summary -t dev --profile DEV`                 |

Important: Your ZIP is already configured to deploy into `main.dlt_orders_demo`. Before deploying, verify that your Databricks workspace has the `main` catalog, Unity Catalog volumes, and serverless Lakeflow pipeline support. If any of these are unavailable, we need to adjust the configuration.

Start with Steps 1–3, then validate the bundle before deployment.

I can also  prepare a single Windows PowerShell deployment script so you can deploy the entire project with fewer manual commands.

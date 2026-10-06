# PySpark Assignment - Source Files

This folder contains PySpark solutions for various data processing tasks on Databricks.

## Files

| File | Description |
| --- | --- |
| `question_01.py` | Customer & Product Analysis — find customers who bought only iphone13, upgraded from iphone13 to iphone14, and bought all product models |
| `question_02.py` | Credit Card Data & Partitioning — create DataFrame, manage partitions (repartition/coalesce), UDF for masking card numbers |
| `question_03.py` | User Activity Log Analysis — custom schema, column renaming, action counts per user in last 7 days, CSV file write, managed table creation |
| `question_04.py` | JSON Processing & Flattening — read JSON with custom schema, flatten nested data, explode/explode_outer/posexplode, camelCase to snake_case, date columns, partitioned table write |
| `question_05.py` | Employee Data Analysis — multiple DataFrames with joins, avg salary, bonus calculation, inner/left/right joins, country name replacement, lowercase columns, parquet/CSV tables |
| `data/employee.json` | Sample JSON data file used by `question_04.py` |

## Key Concepts Covered

- **DataFrame creation** with custom schemas (`StructType`, `StructField`)
- **Joins** — inner, left, right joins with multiple DataFrames
- **Aggregations** — `groupBy`, `agg`, `avg`, `round`, `countDistinct`, `collect_set`
- **Partitioning** — `repartition()`, `coalesce()`, `spark_partition_id()`
- **UDFs** — user-defined functions for card number masking
- **JSON handling** — reading nested JSON, `explode()`, `explode_outer()`, `posexplode()`
- **Column operations** — `withColumnRenamed`, `withColumn`, camelCase to snake_case conversion
- **Date functions** — `to_timestamp`, `to_date`, `current_date`, `year`, `month`, `dayofmonth`
- **File I/O** — writing CSV, Parquet, and JSON formats to UC Volumes
- **Table management** — `saveAsTable`, `CREATE DATABASE`, partitioned tables

## Requirements

- Databricks workspace with **Unity Catalog** enabled
- **Serverless** or **Standard** compute (Spark Connect compatible)
- UC Volume for file storage (e.g., `workspace.default.employee_data`)

## Notes

- All code is compatible with **Spark Connect** (serverless compute)
- RDD APIs are **not used** — replaced with DataFrame/SQL equivalents
- DBFS paths (`/tmp/`) are replaced with **UC Volume paths** (`/Volumes/...`)
- Managed tables use **Delta format** (required by Unity Catalog)

## Author

avinash4tripathi
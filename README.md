# PySpark Assignment

This repository contains a set of PySpark exercises covering DataFrame operations, schema creation, transformations, joins, UDFs, partitioning, and writing to various file formats and managed tables.

## Project Structure

```
src/
├── README.md
├── question_01.py
├── question_02.py
├── question_03.py
├── questio_04.py
├── question_05.py
└── data/
```

## Questions Overview

### Question 01 — Customer Purchase Analysis

- Creates `purchase_data_df` and `product_data_df` with custom schemas
- Finds customers who bought only **iphone13**
- Finds customers who upgraded from **iphone13** to **iphone14**
- Finds customers who bought all product models

### Question 02 — Credit Card Data Processing

- Creates `credit_card_df` with a custom schema
- Prints and manipulates partition counts (`repartition`, `coalesce`)
- Defines a **UDF** to mask credit card numbers (all digits except last 4)
- Produces a final DataFrame with `card_number` and `masked_card_number`

### Question 03 — User Activity Log Analysis

- Creates an activity DataFrame with a custom schema
- Renames columns dynamically using a mapping dictionary
- Calculates actions per user in the last 7 days
- Converts timestamp to date format
- Writes the DataFrame as CSV with various write options
- Saves as a managed table (`user.login_details`)

### Question 04 — Employee JSON Data Processing

- Reads a JSON file using a dynamic function
- Flattens nested struct and array columns
- Compares record counts before and after flattening
- Demonstrates `explode`, `explode_outer`, and `posexplode`
- Filters records by `id`
- Converts column names from camelCase to snake_case
- Adds `load_date`, `year`, `month`, `day` columns
- Writes to a partitioned managed table (`employee.employee_details`)

### Question 05 — Employee, Department & Country Joins

- Creates `employee_df`, `department_df`, and `country_df` with custom schemas
- Joins employee data with country data using the `State` column
- Calculates average salary by department
- Finds employees whose name starts with **'m'**
- Adds a `bonus` column (salary * 2)
- Reorders columns dynamically
- Demonstrates inner, left, and right joins
- Replaces `State` with `country_name` and converts all column names to lowercase
- Adds a `load_date` column with the current date
- Writes data to external Parquet and CSV formats and managed tables

## Requirements

- Apache Spark with PySpark
- Databricks workspace (for Volume and managed table operations)

## How to Run

Run each `question_XX.py` file in a Databricks notebook or a Spark-enabled environment. Some questions reference data stored in Databricks Volumes (`/Volumes/workspace/default/employee_data/`).

# Question 5.1 create all 3 data frames as employee_df, department_df, country_df with custom schema defined in dynamic way

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)

spark = SparkSession.builder.appName ("EmployeeAssignment").getOrCreate()
#Employee Custom Schema
employee_schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("employee_name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("State", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("Age", IntegerType(), True)
])

employee_data = [
    (11, "james", "D101", "ny", 9000, 34),
    (12, "michel", "D101", "ny", 8900, 32),
    (13, "robert", "D102", "ca", 7900, 29),
    (14, "scott", "D103", "ca", 8000, 36),
    (15, "jen", "D102", "ny", 9500, 38),
    (16, "jeff", "D103", "uk", 9100, 35),
    (17, "maria", "D101", "ny", 7900, 40)
]


employee_df = spark.createDataFrame(employee_data,employee_schema)

#Department Custom Schema
department_schema = StructType([
    StructField("dept_id", StringType(), True),
    StructField("dept_name", StringType(), True)
])

department_data = [
    ("D101", "sales"),
    ("D102", "finance"),
    ("D103", "marketing"),
    ("D104", "hr"),
    ("D105", "support")
]


department_df = spark.createDataFrame(department_data,department_schema)
#Country Custom Schema
country_schema = StructType([
    StructField("country_code", StringType(), True),
    StructField("country_name", StringType(), True)
])
country_data = [
    ("ny", "newyork"),
    ("ca", "California"),
    ("uk", "Russia")
]
country_df = spark.createDataFrame(
    country_data,
    country_schema
)
display(employee_df)

display(department_df)

display(country_df)

## Question 2. create a new column called "country" in employee_df by joining employee_df with country_df using State column as key

employee_df = employee_df.join(country_df, employee_df.State == country_df.country_code)
display(employee_df)

#Question 5.2
from pyspark.sql.functions import avg, round

avg_salary_df = (
    employee_df.groupBy("department")
    .agg(round(avg("salary")).alias("avg_salary"))
)
display(avg_salary_df)

#Question 5.3  Find the employee’s name and department name whose name starts with ‘m’.
from pyspark.sql.functions import col
employee_df = employee_df.withColumn(
    "bonus",
    col("salary") * 2
)
display(employee_df)

#Question 5.4
result_df = employee_df.join(
    department_df,
    employee_df.department == department_df.dept_id,
    "inner"
).filter(
    col("employee_name").startswith("m")
).select(
    "employee_name",
    "dept_name"
)

display(result_df)
#Question 5.5 Create another new column in  employee_df as a bonus by multiplying employee salary *2
from pyspark.sql.functions import col

employee_df = employee_df.withColumn(
    "bonus",
    col("salary") * 2
)

display(employee_df)

#Question 5.6  Reorder the column names of employee_df columns as (employee_id,employee_name,salary,State,Age,department)

employee_df = employee_df.select(
    "employee_id",
    "employee_name",
    "salary",
    "State",
    "Age",
    "department",
    "bonus"
)
employee_df.show()

#Question 5.6  Give the result of an inner join, left join, and right join when joining employee_df with department_df in a dynamic way

def join_dataframes(employee_df, department_df, join_type):
    return employee_df.join(
        department_df,
        employee_df["department"] == department_df["dept_id"],
        join_type
    )
inner_join_df = join_dataframes(
    employee_df,
    department_df,
    "inner"
)

inner_join_df.show()

left_join_df = join_dataframes(
    employee_df,
    department_df,
    "left"
)

left_join_df.show()

right_join_df = join_dataframes(
    employee_df,
    department_df,
    "right"
)

right_join_df.show()

#question 5.7 Derive a new data frame with country_name instead of State in employee_df

from pyspark.sql.functions import col

employee_country_df = employee_df.join(
    country_df,
    employee_df["State"] == country_df["country_code"],
    "left"
).select(
    employee_df["employee_id"],
    employee_df["employee_name"],
    employee_df["department"],
    country_df["country_name"].alias("State"),
    employee_df["salary"],
    employee_df["Age"],
    employee_df["bonus"]
)

employee_country_df.show()

#question 5.8 convert all the column names into lowercase from the result of question 7in a dynamic way, add the load_date column with the current date

from pyspark.sql.functions import current_date

for column_name in employee_country_df.columns:
    employee_country_df = employee_country_df.withColumnRenamed(
        column_name,
        column_name.lower()
    )

# Add current date
employee_country_df = employee_country_df.withColumn(
    "load_date",
    current_date()
)
display(employee_country_df)

# Check schema
employee_country_df.printSchema()

#question 5.9  create 2 external tables with parquet, CSV format with the same name database name, and 2 different table names as CSV and parquet format.
#Create database
spark.sql("""
CREATE DATABASE IF NOT EXISTS employee
""")
#Create External Parquet Table
employee_country_df.write.format("parquet").mode("overwrite").save("/tmp/employee_parquet")
spark.sql("""
CREATE TABLE IF NOT EXISTS employee.employee_parquet
USING PARQUET
LOCATION '/tmp/employee_parquet'
""")
employee_country_df.write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save("/tmp/employee_csv")
#Create External CSV Table
spark.sql("""
CREATE TABLE IF NOT EXISTS employee.employee_csv
USING CSV
OPTIONS (
    header = 'true',
    path = '/tmp/employee_csv'
)
""")
spark.sql("""
SELECT *
FROM employee.employee_parquet
""").show()
spark.sql("""
SELECT *
FROM employee.employee_csv
""").show()
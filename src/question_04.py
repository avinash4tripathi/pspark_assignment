from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, explode_outer, posexplode
import re
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType
)

#Question 4.1  Read JSON file provided in the attachment using the dynamic function

spark = SparkSession.builder.appName("EmployeeData") .getOrCreate()


def read_json(spark, file_path):
    return spark.read.json(file_path)


employee_df = read_json(
    spark,
    "/Volumes/workspace/default/employee_data/employee.json"
)


employee_df.show(truncate=False)

employee_df.printSchema()


# 1. Read JSON

json_df = read_json(spark, "/Volumes/workspace/default/employee_data/employee.json")

# question 4.2 latten the data frame which is a custom schem

employee_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField(
        "properties",
        StructType([
            StructField("name", StringType(), True),
            StructField("storeSize", StringType(), True)
        ]),
        True
    ),
    StructField(
        "employees",
        ArrayType(StructType([
            StructField("empId", IntegerType(), True),
            StructField("empName", StringType(), True)
        ])),
        True
    )
])


def read_json(spark, file_path, schema):
    return (
        spark.read
        .schema(schema)
        .json(file_path)
    )


employee_df = read_json(
    spark,
    "/Volumes/workspace/default/employee_data/employee.json",
    employee_schema
)


flattened_df = employee_df.select(
    "id",
    "properties.name",
    "properties.storeSize",
    explode("employees").alias("employee")
)
flattened_df = flattened_df.select(
    "id",
    "name",
    "storeSize",
    "employee.empId",
    "employee.empName"
)


display(flattened_df)

flattened_df.printSchema()


# 4.3  find out the record count when flattened and when it's not flattened(find out the difference why you are getting more count) 
##Count before and after flattening

original_count = json_df.count()

print(
    "Original record count:",
    original_count
)

flattened_count = flattened_df.count()

print(
    "Flattened record count:",
    flattened_count
)

print(
    "Difference:",
    flattened_count - original_count
)

#questiob4.4 Differentiate the difference using explode, explode outer, posexplode functions

# EXPLODE()
explode_df = employee_df.select(
    "id",
    "properties.name",
    "properties.storeSize",
    explode("employees").alias("employee")
)

explode_df.show(truncate=False)

print(
    "Explode Record Count:",
    explode_df.count()
)

# EXPLODE OUTER()

explode_outer_df = employee_df.select(
    "id",
    "properties.name",
    "properties.storeSize",
    explode_outer("employees").alias("employee")
)

explode_outer_df.show(truncate=False)

print(
    "Explode Outer Record Count:",
    explode_outer_df.count()
)


# POSEXPLODE()

posexplode_df = employee_df.select(
    "id",
    "properties.name",
    "properties.storeSize",
    posexplode("employees").alias("position", "employee")
)
posexplode_df.show(truncate=False)

print(
    "PosExplode Record Count:",
    posexplode_df.count()
)


#Question4.5 Filter the id which is equal to 0001

filtered_df = flattened_df.filter(
    F.col("id") == 1001
)


# Question 4.6 convert the column names from camel case to snake case

def camel_to_snake(name):

    return re.sub(
        r'(?<!^)(?=[A-Z])',
        '_',
        name
    ).lower()


for column_name in filtered_df.columns:

    filtered_df = filtered_df.withColumnRenamed(
        column_name,
        camel_to_snake(column_name)
    )


#Question 4.7  add a column load_date with current date

filtered_df = filtered_df.withColumn(
    "load_date",
    F.current_date()
)


# Question 4.8  create 3 new columns as year, month, and day from the load_date column
filtered_df = (
    filtered_df
    .withColumn("year", F.year("load_date"))
    .withColumn("month", F.month("load_date"))
    .withColumn("day", F.dayofmonth("load_date"))
)


#Question:4.9 write data frame to a table with the Database name as employee and table name as employee_details with overwrite mode, format as JSON and partition based on (year, month, day) using replacing where condition on year, month, day
spark.sql("""
CREATE DATABASE IF NOT EXISTS employee
""")


(
    filtered_df.write
    .mode("overwrite")
    .partitionBy(
        "year",
        "month",
        "day"
    )
    .saveAsTable(
        "employee.employee_details"
    )
)
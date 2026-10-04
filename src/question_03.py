#Questio 3.1  Create a Data Frame with custom schema creation by using Struct Type and Struct Field
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType,StructField,IntegerType,StringType
)
data = [
    (1, 101, 'login', '2023-09-05 08:30:00'),
    (2, 102, 'click', '2023-09-06 12:45:00'),
    (3, 101, 'click', '2023-09-07 14:15:00'),
    (4, 103, 'login', '2023-09-08 09:00:00'),
    (5, 102, 'logout', '2023-09-09 17:30:00'),
    (6, 101, 'click', '2023-09-10 11:20:00'),
    (7, 103, 'click', '2023-09-11 10:15:00'),
    (8, 102, 'click', '2023-09-12 13:10:00')
]

schema = StructType([
    StructField("log id", IntegerType(), True),
    StructField("user$id", IntegerType(), True),
    StructField("action", StringType(), True),
    StructField("timestamp", StringType(), True)
])

activity_df = spark.createDataFrame(data,schema)

display(activity_df)

#Questio 3.2 Column names should be log_id, user_id, user_activity, time_stamp using dynamic function 

column_mapping = {
    "log id": "log_id",
    "user$id": "user_id",
    "action": "user_activity",
    "timestamp": "time_stamp"
}
def rename_columns(df, column_mapping):

    for old_column, new_column in column_mapping.items():
        df = df.withColumnRenamed(
            old_column,
            new_column
        )

    return df
activity_df = rename_columns(
    activity_df,
    column_mapping
)
display(activity_df)
activity_df.printSchema()

#Question 3.3  Write a query to calculate the number of actions performed by each user in the last 7 days

activity_df = activity_df.withColumn(
    "time_stamp",
    F.to_timestamp(
        "time_stamp",
        "yyyy-MM-dd HH:mm:ss"
    )
)
display(activity_df)
max_timestamp = activity_df.select(
    F.max("time_stamp")
).first()[0]

display(max_timestamp)

last_7_days_df = activity_df.filter(
    F.col("time_stamp") >=
    F.lit(max_timestamp) - F.expr("INTERVAL 7 DAYS")
)
actions_by_user_df = (
    last_7_days_df
    .groupBy("user_id")
    .agg(
        F.count("*").alias("action_count")
    )
    .orderBy("user_id")
)
display(actions_by_user_df)

#question 3.4 Convert the time stamp column to the login_date column with YYYY-MM-DD format with date type as its data type

activity_df = activity_df.withColumn(
    "login_date",
    F.to_date("time_stamp")
)
display(activity_df)
activity_df.printSchema()

#question 3.5  Write the data frame as a CSV file with different write options except (merge condition)

output_path = "/Volumes/workspace/default/employee_data/login_details_csv"

(
    activity_df.write
    .mode("overwrite")
    .format("csv")
    .option("header", True)
    .option("delimiter", ",")
    .option("quote", '"')
    .option("escape", '"')
    .option("nullValue", "NULL")
    .save(output_path)
)

#Question3.6:   Write it as a managed table with the Database name as user and table name as login_details with overwrite mode.
spark.sql("""CREATE DATABASE IF NOT EXISTS user""")

(
    activity_df.write
    .mode("overwrite")
    .saveAsTable("user.login_details")
)
display(spark.sql)
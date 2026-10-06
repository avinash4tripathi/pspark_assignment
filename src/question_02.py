from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

#Question 2.1 Create a Dataframe as credit_card_df with different read methods

data = [
     ("1234567891234567",),    
     ("5678912345671234",),
     ("9123456712345678",),
    ("1234567812341122",),
    ("1234567812341342",)
]
schema = StructType([
    StructField("card_number", StringType(), True)
])
credit_card_df = spark.createDataFrame(
    data,
    schema
)
display(credit_card_df)

#Question 2.2 Print number of partitions

def partition_count(df):
    return df.withColumn("_pid", F.spark_partition_id()).select("_pid").distinct().count()

original_partition_count = partition_count(credit_card_df)
print("Original partition count:", original_partition_count)

#Question 2.3 Increase partition size to 5

credit_card_df = credit_card_df.repartition(5)
print("Partition count after repartition:", partition_count(credit_card_df))

# Question 2.4 Decrease partition size back to original

credit_card_df = credit_card_df.coalesce(original_partition_count)
print("Partition count after coalesce:", partition_count(credit_card_df))

#question 2.5 Create UDF to mask all digits except last 4

def mask_card_number(card_number):
    if card_number is None:
        return None
    return (
        "*" * (len(card_number) - 4)
        + card_number[-4:]
    )
mask_card_udf = F.udf(
    mask_card_number,
    StringType()
)

# questio 2.6 Final output with 2 columns card_number,masked_card_number

result_df = credit_card_df.withColumn(
    "masked_card_number",
    mask_card_udf(F.col("card_number"))
)
result_df = result_df.select(
    "card_number",
    "masked_card_number"
)
display(result_df)

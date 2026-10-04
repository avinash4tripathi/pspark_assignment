#Create DataFrame as purchase_data_df,  product_data_df with custom schema with the below data
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

purchase_data = [
        (1, "iphone13"),
        (1, "dell i5 core"),
        (2, "iphone13"),
        (2, "dell i5 core"),
        (3, "iphone13"),
        (3, "dell i5 core"),
        (1, "dell i3 core"),
        (1, "hp i5 core"),
        (1, "iphone14"),
        (3, "iphone14"),
        (4, "iphone13")
]

product_data = [
        ("iphone13",),
        ("dell i5 core",),
        ("dell i3 core",),
        ("hp i5 core",),
        ("iphone14",)
]


#Custom schema for purchase data
purchase_schema = StructType([
        StructField("customer", IntegerType(), True),
        StructField("product_model", StringType(), True)
 ])

# Custom schema for product data
product_schema = StructType([
        StructField("product_model", StringType(), True)
])
product_data_df = spark.createDataFrame(product_data,product_schema)

purchase_data_df = spark.createDataFrame(purchase_data,purchase_schema)


display(purchase_data_df)
display(product_data_df)

#Question1.1 Find the customers who have bought only iphone13?
only_iphone13 = (purchase_data_df.groupBy("customer")
    .agg(F.collect_set("product_model").alias("products"))
    .filter((F.size("products") == 1) & (F.array_contains("products", "iphone13")))
    .select("customer")
)
display(only_iphone13)

#Question 1.2 Find customers who upgraded from product iphone13 to product iphone14 ?
iphone_buyers = purchase_data_df.filter(F.col("product_model").isin("iphone13", "iphone14"))

upgraded = (
    iphone_buyers.groupBy("customer")
    .agg(F.collect_set("product_model").alias("iphone_models"))
    .filter(F.array_contains("iphone_models", "iphone13") & F.array_contains("iphone_models", "iphone14"))
    .select("customer")
)
display(upgraded)

#Question 1.3 Find customers who have bought all models in the new Product Data?
total_products = product_data_df.distinct().count()

customers_all_models = (
    purchase_data_df.groupBy("customer")
    .agg(F.countDistinct("product_model").alias("distinct_models"))
    .filter(F.col("distinct_models") == total_products)
    .select("customer")
)
display(customers_all_models)

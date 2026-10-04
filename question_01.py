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


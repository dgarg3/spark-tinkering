import pyspark
import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
conf = SparkConf().setAppName("custapp")\
    .setMaster("local") \
    .set('spark.sql.files.maxPartitionBytes','1')
spark = SparkSession.builder.config(conf=conf).getOrCreate()
data_file = "../in_data/cust.json"
cust_schema = StructType(fields = [
    StructField('customers',ArrayType(StructType([
        StructField('firstName',StringType()),
        StructField('lastName',StringType()),
        StructField('contact',ArrayType(StructType(
            [
        StructField('type',StringType()),
        StructField('value',StringType()),
            ]
        )))
    ])))
])
df = spark.read.option("multiline","true")\
     .schema(cust_schema) \
     .json(data_file)

# df = df.select(explode("customers").alias("customersexplode")).select("customersexplode.*")
# df.select("firstName","lastName",explode("contact")).show()
df.printSchema()
df.show(truncate = False)
df_exploded = df.select(explode("customers").alias("customersexplode")).select("customersexplode.*")
df_exploded = df_exploded.withColumn("df_exploded1",explode("contact"))
df_exploded.printSchema()
df_exploded = df_exploded.select(
    "firstName",
    "lastName",
    "df_exploded1.type",
    "df_exploded1.value",
)
df_exploded.show(truncate = False)


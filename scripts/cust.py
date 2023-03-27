import pyspark
import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
conf = SparkConf().setAppName("custapp")\
    .setMaster("local") \
    .set('spark.sql.files.maxPartitionBytes','1')
spark = SparkSession.builder.config(conf=conf).getOrCreate()
data_file = "../in_data/cust.json"
df = spark.read.option("multiline","true")\
     .option("inferSchema","true") \
     .json(data_file)
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



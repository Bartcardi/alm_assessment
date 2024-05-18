from pyspark.sql.types import (
    DateType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from utils import spark

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

bronze_schema_source1 = StructType(
    [
        StructField("", StringType(), False),
        StructField("DataSource", StringType(), True),
        StructField("ClientGroup", StringType(), True),
        StructField("ClientNumber", StringType(), True),
        StructField("ClientAmount", StringType(), True),
        StructField("Currency", StringType(), True),
        StructField("NumberOfEmployees", StringType(), True),
        StructField("Location", StringType(), True),
        StructField("ClientSince", StringType(), True),
        StructField("EligibleForDiscount", StringType(), False),
        StructField("SnapshotDate", StringType(), True),
    ]
)

df = spark.read.csv(
    path="data/Source Files/Source1.csv", header=True, schema=bronze_schema_source1
)

spark.sql("DROP TABLE IF EXISTS bronze.Source1")
df.write.format("delta").saveAsTable("bronze.Source1")

df = spark.read.parquet("data/Source Files/Source2")

spark.sql("DROP TABLE IF EXISTS bronze.Source2")
df.write.format("delta").saveAsTable("bronze.Source2")

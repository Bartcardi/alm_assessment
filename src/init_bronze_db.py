from pyspark.sql.types import (  # For defining schema
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from utils import spark  # Assuming this module provides a configured SparkSession

# Create the bronze database if it doesn't exist
spark.sql(
    "CREATE DATABASE IF NOT EXISTS bronze"
)  # This is often used in data lake architectures for raw data storage

# Define the schema for Source1 data
# Note: All columns are StringType since we want to preserve the raw values
bronze_schema_source1 = StructType(
    [
        StructField("", StringType(), True),  # Unnamed column, must have a value
        StructField("DataSource", StringType(), True),
        StructField("ClientGroup", StringType(), True),
        StructField("ClientNumber", StringType(), True),
        StructField("ClientAmount", StringType(), True),
        StructField("Currency", StringType(), True),
        StructField("NumberOfEmployees", StringType(), True),
        StructField("Location", StringType(), True),
        StructField("ClientSince", StringType(), True),
        StructField("EligibleForDiscount", StringType(), True),
        StructField("SnapshotDate", StringType(), True),
    ]
)

# Read the Source1.csv file using the defined schema
df = spark.read.csv(
    path="data/Source Files/Source1.csv", header=True, schema=bronze_schema_source1
)

# Drop the existing bronze.Source1 table if it exists to avoid conflicts
spark.sql("DROP TABLE IF EXISTS bronze.Source1")

# Write the data as a Delta table in the 'bronze' database
df.write.format("delta").saveAsTable("bronze.Source1")

# Read the Source2 data directly as Parquet (schema is inferred automatically)
df = spark.read.parquet("data/Source Files/Source2")

# Drop the existing bronze.Source2 table if it exists
spark.sql("DROP TABLE IF EXISTS bronze.Source2")

# Write the data as a Delta table in the 'bronze' database
df.write.format("delta").saveAsTable("bronze.Source2")

# Define the schema for lookup data exchange rates
bronze_schema_exchange_rates = StructType(
    [
        StructField("", StringType(), True),  # Unnamed column, must have a value
        StructField("Currency", StringType(), True),
        StructField("ExchangeRate", DoubleType(), True),
        StructField("SnapshotDate", DateType(), True),
    ]
)

# Read the Exchange_Rates.csv file using the defined schema
df = spark.read.csv(
    path="data/Lookup Files/Exchange_Rates.csv",
    header=True,
    schema=bronze_schema_exchange_rates,
)

# Drop the existing bronze.Exchange_Rates table if it exists to avoid conflicts
spark.sql("DROP TABLE IF EXISTS bronze.Exchange_Rates")

# Write the data as a Delta table in the 'bronze' database
df.write.format("delta").saveAsTable("bronze.Exchange_Rates")

# Define the schema for lookup data client secured
bronze_schema_client_secured_ind = StructType(
    [
        StructField("ClientNumber", IntegerType(), True),
        StructField("ClientSecuredInd", StringType(), True),
    ]
)

# Read the Client_Secured_Ind.csv file using the defined schema
df = spark.read.csv(
    path="data/Lookup Files/Client_Secured_Ind.csv",
    header=True,
    sep=";",
    schema=bronze_schema_client_secured_ind,
).drop_duplicates()

# Drop the existing bronze.Exchange_Rates table if it exists to avoid conflicts
spark.sql("DROP TABLE IF EXISTS bronze.Client_Secured_Ind")

# Write the data as a Delta table in the 'bronze' database
df.write.format("delta").saveAsTable("bronze.Client_Secured_Ind")

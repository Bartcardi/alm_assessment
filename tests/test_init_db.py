from conftest import spark_session  # Import the shared SparkSession fixture
from pyspark.sql.types import (  # For defining schema
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def test_initialize_source1_bronze(spark_session):
    """
    Tests that all columns in the Source1 table within the 'bronze' database are of StringType().
    """
    spark_session.sql("USE bronze")  # Switch to the 'bronze' database

    # Read the "Source1" table into a DataFrame
    df_s1 = spark_session.read.table("Source1")

    # Get the data types of all columns, convert them to strings, and remove duplicates
    types_s1 = list(set([str(column.dataType) for column in df_s1.schema]))

    # Assert that there is only one unique data type (StringType) among all columns
    assert len(types_s1) == 1 and types_s1[0] == "StringType()"


def test_initialize_source2_bronze(spark_session):
    """
    Tests that all columns in the Source2 table within the 'bronze' database are of StringType().
    """
    spark_session.sql("USE bronze")  # Switch to the 'bronze' database

    # Read the "Source2" table into a DataFrame
    df_s2 = spark_session.read.table("Source2")

    # Get the data types of all columns, convert them to strings, and remove duplicates
    types_s2 = list(set([str(column.dataType) for column in df_s2.schema]))

    # Assert that there is only one unique data type (StringType) among all columns
    assert len(types_s2) == 1 and types_s2[0] == "StringType()"


def test_initialize_exchange_rates_bronze(spark_session):
    """
    Tests that columns in the Exchange_Rates table within the 'bronze' database match the expected schema.
    """
    spark_session.sql("USE bronze")  # Switch to the 'bronze' database

    # Read the "Exchange_Rates" table into a DataFrame
    exchange_rates = spark_session.read.table("Exchange_Rates")

    # Define the expected schema
    bronze_schema_exchange_rates = StructType(
        [
            StructField("", StringType(), True),  # Unnamed column, must have a value
            StructField("Currency", StringType(), True),
            StructField("ExchangeRate", DoubleType(), True),
            StructField("SnapshotDate", DateType(), True),
        ]
    )

    # Assert that the schema of the DataFrame matches the expected schema
    assert exchange_rates.schema == bronze_schema_exchange_rates


def test_initialize_client_secured_ind_bronze(spark_session):
    """
    Tests that columns in the Client_Secured_Ind table within the 'bronze' database match the expected schema.
    """
    spark_session.sql("USE bronze")  # Switch to the 'bronze' database

    # Read the "Client_Secured_Ind" table into a DataFrame
    client_secured = spark_session.read.table("Client_Secured_Ind")

    # Define the expected schema
    bronze_schema_client_secured_ind = StructType(
        [
            StructField("ClientNumber", IntegerType(), True),
            StructField("ClientSecuredInd", StringType(), True),
        ]
    )

    # Assert that the schema of the DataFrame matches the expected schema
    assert client_secured.schema == bronze_schema_client_secured_ind

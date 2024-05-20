from conftest import spark_session  # Import the shared SparkSession fixture


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

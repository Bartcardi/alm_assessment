import os
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# Set location of "data warehouse" to parent folder.
tests_location = Path(os.getcwd())
root_folder = tests_location.parent
warehouse_location = root_folder / "spark-warehouse"


@pytest.fixture(scope="session")
def spark_session(request):
    """Fixture for creating a SparkSession."""
    spark = (
        SparkSession.builder.master("local[2]")  # Adjust based on your system resources
        # Installing the Delta Lake package.
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0")
        # Setting Ivy (dependency manager) options. Useful for Spark, especially on standalone installations.
        .config("spark.driver.extraJavaOptions", "-Divy.cache.dir=/tmp -Divy.home=/tmp")
        # Enabling the Delta Lake extension for Spark.
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        # Configuring Delta Lake as the default catalog. This is where Spark will look for tables by default.
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Set the default location for managed Hive tables and databases.
        .config("spark.sql.warehouse.dir", warehouse_location)
        # Enable Hive support in SparkSession.
        .enableHiveSupport()
        .appName("Pytest Spark Session")
        .getOrCreate()
    )

    request.addfinalizer(lambda: spark.stop())  # Stop SparkSession after tests
    return spark

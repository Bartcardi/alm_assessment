from pyspark.sql import SparkSession

warehouse_location = "spark-warehouse"

# Creating a SparkSession
spark = (
    SparkSession.builder
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
    # Setting the master URL to local with 4 cores. This means we're running Spark in standalone mode on the current machine.
    .master("local[4]")
    # This either gets an existing SparkSession or creates a new one if none exists.
    .getOrCreate()
)

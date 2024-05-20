from pyspark.sql.types import (
    DateType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from utils import spark

spark.sql("USE bronze")

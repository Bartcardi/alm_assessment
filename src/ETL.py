from pyspark.sql import DataFrame
from pyspark.sql.functions import col, length, lit, regexp_extract, substring, when
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    NullType,
    StringType,
    StructField,
    StructType,
)

from utils import spark

date_pattern = r"^\d{4}-\d{2}-\d{2}$"  # Regex for yyyy-MM-dd


def check_date_format_regex(df, column_name):
    """
    Uses a regular expression to check if a string column conforms to the 'yyyy-MM-dd' format.

    Args:
        df: The input Spark DataFrame.
        column_name: The name of the column to check.

    Returns:
        A Spark DataFrame with an added column for date format validity.
    """
    return df.withColumn(
        f"is_valid_{column_name}",
        (regexp_extract(col(column_name), date_pattern, 0) == col(column_name)),
    )


def correct_triple_digit_days_dates(df, column_name):
    """
    Corrects dates with triple digits days by removing the last digit.

    Args:
        df: The input Spark DataFrame.
        column_name: The name of the column to check.
    """
    return df.withColumn(
        column_name,
        when(col(f"is_valid_{column_name}"), col(column_name)).otherwise(
            substring(col(column_name), 1, 10)
        ),
    ).drop(f"is_valid_{column_name}")


def map_source1(df: DataFrame):
    df = (
        df.withColumnRenamed("DataSource", "SourceSystem")
        .withColumn("ClientNumber", col("ClientNumber").cast(IntegerType()))
        .withColumn("OriginalAmount", col("ClientAmount").cast(DoubleType()))
        .drop("ClientAmount")
        .withColumnRenamed("Currency", "OriginalCurrency")
        .withColumn("NumberOfEmployees", col("NumberOfEmployees").cast(IntegerType()))
        .withColumn("OnboardingDate", col("ClientSince").cast(DateType()))
        .drop("ClientSince")
        .withColumn("LoanCheck", lit(None).cast(StringType()))
        .withColumnRenamed("Location", "Country")
        .withColumn("DiscountCheck", substring(col("EligibleForDiscount"), 1, 1))
        .drop("EligibleForDiscount")
        .withColumn("SnapshotDate", col("SnapshotDate").cast(DateType()))
    ).select(
        "SourceSystem",
        "ClientGroup",
        "ClientNumber",
        "OriginalAmount",
        "OriginalCurrency",
        "NumberOfEmployees",
        "OnboardingDate",
        "LoanCheck",
        "Country",
        "DiscountCheck",
        "SnapshotDate",
    )
    return df


def map_source2(df: DataFrame):
    df = (
        df.withColumn("ClientNumber", col("ClientNumber").cast(IntegerType()))
        .withColumn("OriginalAmount", col("ClientAmount").cast(DoubleType()))
        .drop("ClientAmount")
        .withColumnRenamed("Currency", "OriginalCurrency")
        .withColumn("NumberOfEmployees", col("NumberOfEmployees").cast(IntegerType()))
        .withColumn("OnboardingDate", col("ClientSince").cast(DateType()))
        .drop("ClientSince")
        .withColumn("LoanCheck", substring(col("HasLoan"), 1, 1))
        .withColumn("Country", lit(None).cast(StringType()))
        .withColumn("DiscountCheck", lit(None).cast(StringType()))
        .withColumn("SnapshotDate", col("SnapshotDate").cast(DateType()))
    ).select(
        "SourceSystem",
        "ClientGroup",
        "ClientNumber",
        "OriginalAmount",
        "OriginalCurrency",
        "NumberOfEmployees",
        "OnboardingDate",
        "LoanCheck",
        "Country",
        "DiscountCheck",
        "SnapshotDate",
    )
    return df


def main():
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    spark.sql("USE bronze")

    df_s1 = spark.read.table("Source1")
    df_s1 = check_date_format_regex(df_s1, "ClientSince")
    df_s1 = correct_triple_digit_days_dates(df_s1, "ClientSince")
    df_s1 = map_source1(df_s1)

    df_s2 = spark.read.table("Source2")
    df_s2 = check_date_format_regex(df_s2, "ClientSince")
    df_s2 = correct_triple_digit_days_dates(df_s2, "ClientSince")
    df_s2 = map_source2(df_s2)

    df_final = df_s1.union(df_s2)

    df_final.show()


if __name__ == "__main__":
    main()

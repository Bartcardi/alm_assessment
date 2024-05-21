from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col,
    lag,
    lit,
    max,
    regexp_extract,
    round,
    substring,
    sum,
    when,
)
from pyspark.sql.types import DateType, DoubleType, IntegerType

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
        .withColumn("LoanCheck", lit(""))
        # .withColumn("LoanCheck", lit(None).cast(StringType()))
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
        .withColumn("Country", lit(""))
        # .withColumn("Country", lit(None).cast(StringType()))
        .withColumn("DiscountCheck", lit(""))
        # .withColumn("DiscountCheck", lit(None).cast(StringType()))
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


def convert_to_euros(df, amount_col, currency_col, exchange_rates_df):
    """
    Converts a column of amounts in various currencies to Euros using a lookup table of exchange rates.

    Args:
        df: The input Spark DataFrame containing the amount and currency columns.
        amount_col: The name of the column containing the amounts to be converted.
        currency_col: The name of the column containing the currency codes.
        exchange_rates_df: A Spark DataFrame with columns 'currency' and 'rate_to_eur'.

    Returns:
        A new Spark DataFrame with the converted amount in Euros.
    """

    # Join the input DataFrame with the exchange rate lookup table
    joined_df = df.join(
        exchange_rates_df,
        on=[
            exchange_rates_df["Currency"] == df[currency_col],
            exchange_rates_df["SnapShotDate"] == df["SnapShotDate"],
        ],
        how="left",  # Left join to keep all rows from the original DataFrame
    )

    # Perform the conversion
    converted_df = joined_df.withColumn(
        "AmountEUR",
        when(
            col("ExchangeRate").isNotNull(),
            round(col(amount_col) * col("ExchangeRate"), 2),
        ).otherwise(
            col(amount_col)
        ),  # Handle missing exchange rates
    )

    # Return the DataFrame with the new 'AmountEUR' column
    return converted_df.select(df["*"], "AmountEUR")


def is_client_secured(df, client_id_col, string_col, client_secured_df):
    client_secured_df = client_secured_df.withColumn(
        string_col,
        when(col(string_col) == "Y", True)
        .when(col(string_col) == "N", False)
        .otherwise(None),  # Handle values other than 'Y' or 'N' as null
    )

    joined_df = df.join(
        client_secured_df,
        on=client_secured_df[client_id_col] == df[client_id_col],
        how="left",  # Left join to keep all rows from the original DataFrame
    )

    joined_df = joined_df.withColumnRenamed("ClientSecuredInd", "ClientSecuredIND")

    return joined_df.select(df["*"], "ClientSecuredIND")


def determine_enterprize_size(df):
    df = df.withColumn(
        "EnterprizeSize",
        when(col("NumberOfEmployees") < 100, "S")
        .when(
            (col("NumberOfEmployees") >= 100) & (col("NumberOfEmployees") < 1000),
            "M",
        )
        .otherwise("L"),
    )
    return df


def calculate_secured_amount(df):
    # create window for running totals
    cumulative_window = (
        Window.partitionBy("ClientGroup", "ClientSecuredIND")
        .orderBy(
            "ClientNumber"
            # in this case we will use rangeBetween for the sum
        )
        .rangeBetween(
            # In this case we need to use Window.unboundedPreceding to catch all earlier rows
            Window.unboundedPreceding,
            0,
        )
    )
    normal_window = Window.partitionBy("ClientGroup").orderBy("ClientNumber")

    df_cumulative = df.withColumn(
        "remaining_amount",
        when(
            col("ClientSecuredIND"), 500000 - sum("AmountEUR").over(cumulative_window)
        ),
    )

    df_allocated = df_cumulative.withColumn(
        "SecuredAmount",
        when(
            (lag("remaining_amount", 1).over(normal_window).isNull())
            & (
                col("remaining_amount") >= 0
            ),  # Check if previous row does not exist and current row is positive
            col("AmountEUR"),
        )
        .when(
            (lag("remaining_amount", 1).over(normal_window).isNull())
            & (
                col("remaining_amount") < 0
            ),  # Check if previous row does not exist and current row is negative
            lit(500000),
        )
        .when(
            (lag("remaining_amount", 1).over(normal_window).isNotNull())
            & (
                col("remaining_amount") >= 0
            ),  # Check if previous row exists and current row is positive
            col("AmountEUR"),
        )
        .when(
            (lag("remaining_amount", 1).over(normal_window).isNotNull())
            & (col("remaining_amount") < 0)
            & (
                lag("remaining_amount", 1).over(normal_window) > 0
            ),  # Check if previous row does not exist and current row is positive
            lag("remaining_amount", 1).over(normal_window),
        )
        .otherwise(lit(0)),
    ).drop("remaining_amount")
    return df_allocated


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

    print(f"Rows df_s1: {df_s1.count()}")
    print(f"Rows df_s2: {df_s2.count()}")

    df_final = df_s1.union(df_s2)

    print(f"Rows df_final: {df_final.count()}")

    df_exchange_rates = spark.read.table("Exchange_Rates")

    df_final = convert_to_euros(
        df_final, "OriginalAmount", "OriginalCurrency", df_exchange_rates
    )
    print(f"Rows df_final after exchange rates: {df_final.count()}")

    df_client_secured = spark.read.table("Client_Secured_Ind")

    df_final = is_client_secured(
        df_final, "ClientNumber", "ClientSecuredInd", df_client_secured
    )
    print(f"Rows df_final after is secured: {df_final.count()}")

    df_final = determine_enterprize_size(df_final)

    print(f"Rows df_final after enterprize size: {df_final.count()}")

    df_final = calculate_secured_amount(df_final)

    df_final.show()

    # Drop the existing silver.Final table if it exists to avoid conflicts
    spark.sql("DROP TABLE IF EXISTS silver.Final")

    # Write the data as a Delta table in the 'silver' database
    df_final.write.format("delta").saveAsTable("silver.Final")
    df_final.write.csv("data/Output Files/final.csv", header="true")


if __name__ == "__main__":
    main()

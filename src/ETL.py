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
    """
    Maps and transforms columns from Source1 data to a standardized schema.

    This function performs the following transformations:
    * Renames columns to match a standard schema.
    * Casts relevant columns to their correct data types (Integer, Double, Date).
    * Extracts the first character of 'EligibleForDiscount' into 'DiscountCheck'.
    * Creates a new empty column 'LoanCheck'.
    * Selects and reorders the columns to match the final schema.

    Args:
        df: A Spark DataFrame representing data from Source1.

    Returns:
        A transformed Spark DataFrame with a standardized schema.
    """

    return (
        df.withColumnRenamed("DataSource", "SourceSystem")  # Rename column
        .withColumn(
            "ClientNumber", col("ClientNumber").cast(IntegerType())
        )  # Cast to Integer
        .withColumn(
            "OriginalAmount", col("ClientAmount").cast(DoubleType())
        )  # Cast to Double
        .drop("ClientAmount")  # Drop original column after conversion
        .withColumnRenamed("Currency", "OriginalCurrency")  # Rename column
        .withColumn(
            "NumberOfEmployees", col("NumberOfEmployees").cast(IntegerType())
        )  # Cast to Integer
        .withColumn(
            "OnboardingDate", col("ClientSince").cast(DateType())
        )  # Cast to Date
        .drop("ClientSince")  # Drop original column after conversion
        .withColumn("LoanCheck", lit(""))  # Create an empty 'LoanCheck' column
        .withColumnRenamed("Location", "Country")  # Rename column
        .withColumn(
            "DiscountCheck", substring(col("EligibleForDiscount"), 1, 1)
        )  # Extract first character
        .drop("EligibleForDiscount")  # Drop original column after extraction
        .withColumn(
            "SnapshotDate", col("SnapshotDate").cast(DateType())
        )  # Cast to Date
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
    )  # Select and reorder columns


def map_source2(df: DataFrame):
    """
    Maps and transforms columns from Source2 data to a standardized schema.

    This function performs similar transformations as map_source1, but with the following differences:
    * Extracts the first character of 'HasLoan' into 'LoanCheck'.
    * Creates empty columns for 'Country' and 'DiscountCheck'.
    * The original column names are already compatible with the standard schema.

    Args:
        df: A Spark DataFrame representing data from Source2.

    Returns:
        A transformed Spark DataFrame with a standardized schema.
    """

    return (
        df.withColumn(
            "ClientNumber", col("ClientNumber").cast(IntegerType())
        )  # Cast to Integer
        .withColumn(
            "OriginalAmount", col("ClientAmount").cast(DoubleType())
        )  # Cast to Double
        .drop("ClientAmount")  # Drop original column after conversion
        .withColumnRenamed("Currency", "OriginalCurrency")  # Rename column
        .withColumn(
            "NumberOfEmployees", col("NumberOfEmployees").cast(IntegerType())
        )  # Cast to Integer
        .withColumn(
            "OnboardingDate", col("ClientSince").cast(DateType())
        )  # Cast to Date
        .drop("ClientSince")  # Drop original column after conversion
        .withColumn(
            "LoanCheck", substring(col("HasLoan"), 1, 1)
        )  # Extract first character from 'HasLoan'
        .withColumn("Country", lit(""))  # Create an empty 'Country' column
        .withColumn("DiscountCheck", lit(""))  # Create an empty 'DiscountCheck' column
        .withColumn(
            "SnapshotDate", col("SnapshotDate").cast(DateType())
        )  # Cast to Date
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
    )  # Select and reorder columns


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
    """
    Determines whether clients are secured based on a lookup table.

    This function joins a client DataFrame with a client secured indicator lookup table, and returns the
    client DataFrame with an additional column "ClientSecuredIND" indicating whether the client is secured (True/False)
    or not found in the lookup table (None).

    Args:
        df: A Spark DataFrame containing client data, including the client_id_col.
        client_id_col: The name of the column in both DataFrames used for joining (e.g., "ClientNumber").
        string_col: The name of the column in the client_secured_df containing the secured status indicator (e.g., "ClientSecuredInd").
        client_secured_df: A Spark DataFrame with client_id_col and string_col to indicate if a client is secured ('Y'/'N').

    Returns:
        A Spark DataFrame containing the original client data and a new boolean column "ClientSecuredIND"
        representing the client's secured status.
    """

    # Convert 'Y' and 'N' values in the lookup table to True/False for easier joining and subsequent analysis.
    client_secured_df = client_secured_df.withColumn(
        string_col,
        when(col(string_col) == "Y", True)
        .when(col(string_col) == "N", False)
        .otherwise(None),  # Handle values other than 'Y' or 'N' as null
    )

    # Join the client DataFrame with the lookup table based on the common client ID column.
    # A left join ensures that all clients from the original DataFrame are kept, even if not found in the lookup.
    joined_df = df.join(
        client_secured_df,
        on=client_secured_df[client_id_col] == df[client_id_col],
        how="left",
    )

    # Rename the joined column to "ClientSecuredIND" for consistency and clarity.
    joined_df = joined_df.withColumnRenamed("ClientSecuredInd", "ClientSecuredIND")

    # Select and return only the original client columns along with the new "ClientSecuredIND" column.
    return joined_df.select(df["*"], "ClientSecuredIND")


def determine_enterprize_size(df):
    """
    Categorizes businesses into size classes ('S', 'M', 'L') based on their number of employees.

    Args:
        df: A Spark DataFrame with a column named 'NumberOfEmployees'.

    Returns:
        The input DataFrame with a new column 'EnterprizeSize' representing the size classification.
    """
    return df.withColumn(
        "EnterprizeSize",
        when(col("NumberOfEmployees") < 100, "S")
        .when(
            (col("NumberOfEmployees") >= 100) & (col("NumberOfEmployees") < 1000),
            "M",
        )
        .otherwise("L"),
    )


def calculate_secured_amount(df):
    """
    Calculates the 'SecuredAmount' for each client, based on a cumulative limit of 500,000 per client group,
    and considering whether the client is secured ('ClientSecuredIND' = True).

    Args:
        df: A Spark DataFrame with columns 'ClientGroup', 'ClientSecuredIND', 'AmountEUR', and 'ClientNumber'.

    Returns:
        The input DataFrame with an additional column 'SecuredAmount' representing the calculated amount.
    """
    # Create window for calculating running totals of 'AmountEUR' per client group and secured status
    cumulative_window = (
        Window.partitionBy("ClientGroup", "ClientSecuredIND")
        .orderBy("ClientNumber")  # Order within each partition
        .rangeBetween(Window.unboundedPreceding, 0)  # Range for cumulative sum
    )
    normal_window = Window.partitionBy("ClientGroup", "ClientSecuredIND").orderBy(
        "ClientNumber"
    )

    # Calculate the remaining secured amount for each row within the group
    df_cumulative = df.withColumn(
        "remaining_amount",
        when(
            col("ClientSecuredIND"), 500000 - sum("AmountEUR").over(cumulative_window)
        ),
    )

    # Calculate the 'SecuredAmount' for each row based on various conditions
    df_allocated = df_cumulative.withColumn(
        "SecuredAmount",
        when(
            (lag("remaining_amount", 1).over(normal_window).isNull())
            & (
                col("remaining_amount") >= 0
            ),  # First row in group, enough remaining amount
            col("AmountEUR"),
        )
        .when(
            (lag("remaining_amount", 1).over(normal_window).isNull())
            & (
                col("remaining_amount") < 0
            ),  # First row in group, not enough remaining amount
            lit(500000),
        )
        .when(
            (lag("remaining_amount", 1).over(normal_window).isNotNull())
            & (
                col("remaining_amount") >= 0
            ),  # Not the first row, enough remaining amount
            col("AmountEUR"),
        )
        .when(
            (lag("remaining_amount", 1).over(normal_window).isNotNull())
            & (col("remaining_amount") < 0)
            & (
                lag("remaining_amount", 1).over(normal_window) > 0
            ),  # Not the first row, not enough remaining, but previous had enough
            lag("remaining_amount", 1).over(normal_window),
        )
        .otherwise(lit(0)),  # Default value for other cases
    ).drop(
        "remaining_amount"
    )  # Remove the temporary 'remaining_amount' column
    return df_allocated


def main():
    """
    Main data processing pipeline:
    1. Loads data from 'bronze' database.
    2. Performs data cleaning and validation.
    3. Transforms and enriches data.
    4. Saves the final result to the 'silver' database and a CSV file.
    """

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

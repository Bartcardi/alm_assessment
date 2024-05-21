import pytest
from conftest import spark_session
from pyspark.sql import SparkSession

from ETL import convert_to_euros


@pytest.mark.parametrize(
    "original_amounts, columns, exchange_rates, expected_eur_amounts",
    [
        (
            [
                (100.0, "USD", "2023-02-06"),
                (200.0, "GBP", "2023-02-07"),
                (300.0, "EUR", "2023-02-08"),
            ],  # Test data
            [
                "OriginalAmount",
                "OriginalCurrency",
                "SnapShotDate",
            ],  # Column names in test data
            [
                ("USD", 0.95, "2023-02-06"),
                ("GBP", 1.18, "2023-02-06"),
                ("GBP", 1.15, "2023-02-07"),
            ],  # Exchange rates
            [95.0, 230.0, 300.0],  # Expected results
        ),
        (
            [(500.0, "JPY", "2024-01-01"), (1000.0, "CHF", "2023-02-10")],
            [
                "OriginalAmount",
                "OriginalCurrency",
                "SnapShotDate",
            ],
            [
                ("JPY", 0.0068, "2024-01-01"),
                ("JPY", 0.0088, "2023-01-01"),
                ("CHF", 0.98, "2023-02-10"),
            ],
            [3.4, 980.0],
        ),
        (
            [(250.0, "SEK", "2024-05-21")],  # Test with a missing exchange rate
            [
                "OriginalAmount",
                "OriginalCurrency",
                "SnapShotDate",
            ],
            [
                ("USD", 0.95, "2024-05-21"),
                ("GBP", 1.15, "2024-05-21"),
            ],  # SEK is missing
            [250.0],  # Amount should remain unchanged
        ),
    ],
)
def test_convert_to_euros(
    spark_session: SparkSession,
    original_amounts,
    columns,
    exchange_rates,
    expected_eur_amounts,
):
    """
    Tests the convert_to_euros function with various input data and exchange rates.
    """
    # Create the test DataFrames
    df = spark_session.createDataFrame(original_amounts, columns)
    exchange_rates_df = spark_session.createDataFrame(
        exchange_rates, ["Currency", "ExchangeRate", "SnapShotDate"]
    )

    # Call the function under test
    result_df = convert_to_euros(
        df, "OriginalAmount", "OriginalCurrency", exchange_rates_df
    )

    # Verify the results
    result_list = (
        result_df.select("AmountEUR").rdd.flatMap(lambda x: x).collect()
    )  # Turn the result into a python list
    assert result_list == expected_eur_amounts
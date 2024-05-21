import pytest
from conftest import spark_session
from pyspark.sql import SparkSession

from ETL import (
    calculate_secured_amount,
    convert_to_euros,
    determine_enterprize_size,
    is_client_secured,
)


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


@pytest.mark.parametrize(
    "df_data, client_id_col, string_col, client_secured_data, expected_output",
    [
        # Test Case 1: All clients found in lookup, 'Y'/'N' values
        (
            [("client1",), ("client2",), ("client3",)],
            "ClientNumber",
            "ClientSecuredInd",
            [("client1", "Y"), ("client2", "N"), ("client3", "Y")],
            [("client1", True), ("client2", False), ("client3", True)],
        ),
        # Test Case 2: Some clients not found in lookup
        (
            [("client1",), ("client4",)],
            "ClientNumber",
            "ClientSecuredInd",
            [("client1", "Y")],
            [("client1", True), ("client4", None)],
        ),
        # Test Case 3: Non-'Y'/'N' values in lookup
        (
            [("client1",), ("client2",)],
            "ClientNumber",
            "ClientSecuredInd",
            [("client1", "Y"), ("client2", "Maybe")],
            [
                ("client1", True),
                ("client2", None),
            ],  # Expected client2 to be None since it's not 'Y' or 'N'
        ),
    ],
)
def test_is_client_secured(
    spark_session: SparkSession,
    df_data,
    client_id_col,
    string_col,
    client_secured_data,
    expected_output,
):
    """Test the is_client_secured function with various scenarios."""

    # Create test DataFrames
    df = spark_session.createDataFrame(df_data, [client_id_col])
    client_secured_df = spark_session.createDataFrame(
        client_secured_data, [client_id_col, string_col]
    )

    # Call the function under test
    result_df = is_client_secured(df, client_id_col, string_col, client_secured_df)

    # Verify the result DataFrame
    expected_df = spark_session.createDataFrame(
        expected_output, [client_id_col, "ClientSecuredIND"]
    )
    assert result_df.collect() == expected_df.collect()

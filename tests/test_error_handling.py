import pytest
from conftest import spark_session
from pyspark.sql import SparkSession

from ETL import check_date_format_regex


@pytest.mark.parametrize(
    "date_str, expected_validity",
    [
        ("2023-12-31", True),  # Valid date
        # ("2024-02-29", True),  # Leap year
        ("2024-01-15", True),  # Valid date
        ("1999-09-09", True),  # Valid date
        ("invalid-date", False),
        ("2023/12/31", False),  # Wrong delimiter
        ("2010-05-111", False),  # Typo?
        # ("2023-13-01", False),  # Invalid month
        # ("2023-02-30", False),  # Invalid day for February (non-leap year)
        # (None, False),  # Null value
    ],
)
def test_check_date_format_regex(
    spark_session: SparkSession, date_str: str, expected_validity: bool
):
    """
    Tests the check_date_format_regex function with various input date strings.
    """

    # Create a test DataFrame with a single column 'date_str'
    df = spark_session.createDataFrame([(date_str,)], ["date_str"])

    # Call the function under test
    result_df = check_date_format_regex(df, "date_str")

    # Assert the expected validity
    assert result_df.first()[f"is_valid_date_str"] == expected_validity

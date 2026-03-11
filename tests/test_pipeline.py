import pytest
from unittest.mock import patch
from pyspark.sql import SparkSession
from src.pipeline import BankingDataPipeline

# 1. Create a local Spark session specifically for testing purposes (in-memory)
@pytest.fixture(scope="session")
def spark():
    """Provides a local Spark session to be reused across all tests."""
    return SparkSession.builder \
        .appName("pytest-local-spark") \
        .master("local[1]") \
        .getOrCreate()

# 2. Mock the Azure initialization to prevent external network/cloud calls
@patch('src.pipeline.BaseETLPipeline._initialize_spark')
def test_transform_logic(mock_init_spark, spark):
    """
    Tests the core business logic of the pipeline independently of the infrastructure.
    Verifies typo correction, invalid account filtering, and loan aggregation.
    """
    # ARRANGE: Set up the environment and mock data
    
    # Force the pipeline to use our local Spark session instead of the Azure-configured one
    mock_init_spark.return_value = spark
    
    # Initialize the pipeline class (safe now, as Azure initialization is mocked)
    pipeline = BankingDataPipeline()
    
    # Create mock transaction data containing edge cases
    trans_data = [
        (1, 100, "PRJIEM"),  # Edge case 1: Typo that needs fixing
        (2, 200, "PRIJEM"),  # Normal valid record
        (3, 999, "VYDAJ")    # Edge case 2: Invalid account_id (999 does not exist in accounts)
    ]
    df_trans = spark.createDataFrame(trans_data, ["trans_id", "account_id", "type"])

    # Create mock account data
    account_data = [(100, 10), (200, 20)] 
    df_account = spark.createDataFrame(account_data, ["account_id", "district_id"])

    # Create mock loan data
    loan_data = [(1, 100, 5000.0), (2, 200, 10000.0)]
    df_loan = spark.createDataFrame(loan_data, ["loan_id", "account_id", "amount"])

    # Bundle the DataFrames into the expected input format
    raw_data = {"trans": df_trans, "account": df_account, "loan": df_loan}

    # ACT: Execute the actual business logic
    result = pipeline.transform(raw_data)
    
    df_cleaned_trans = result["cleaned_trans"]
    df_avg_loan = result["avg_loan"]

    # ASSERT: Validate the output against expected business rules
    
    # Assertion 1: Was the typo 'PRJIEM' successfully corrected to 'PRIJEM'?
    types = [row["type"] for row in df_cleaned_trans.collect()]
    assert "PRJIEM" not in types, "The typo 'PRJIEM' was not fixed in the output."
    assert types.count("PRIJEM") == 2, "There should be exactly two 'PRIJEM' transactions."
    
    # Assertion 2: Were transactions without a valid account filtered out?
    # trans_id 3 (with account 999) should be dropped during the inner join
    assert df_cleaned_trans.count() == 2, "Transactions with invalid accounts were not filtered out."

    # Assertion 3: Is the average loan amount per district calculated correctly?
    avg_loans = {row["district_id"]: row["average_loan_amount"] for row in df_avg_loan.collect()}
    assert avg_loans[10] == 5000.0, "Average loan calculation for district 10 is incorrect."
    assert avg_loans[20] == 10000.0, "Average loan calculation for district 20 is incorrect."
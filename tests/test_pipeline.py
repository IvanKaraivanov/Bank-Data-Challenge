import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Create a local Spark session specifically for testing purposes
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("TestPipeline").getOrCreate()

def test_fix_prjiem_typo(spark):
    """Test if the specific typo 'PRJIEM' is correctly replaced with 'PRIJEM'."""
    # Arrange: Create mock data with the typo
    trans_data = [("1", "PRJIEM", 100), ("2", "VALID", 200)]
    df_trans = spark.createDataFrame(trans_data, ["account_id", "type", "amount"])
    
    # Act: Apply the transformation logic for the typo
    df_cleaned = df_trans.withColumn("type", when(col("type") == "PRJIEM", "PRIJEM").otherwise(col("type")))
    
    # Assert: Validate the results
    types = [row["type"] for row in df_cleaned.collect()]
    assert "PRJIEM" not in types
    assert "PRIJEM" in types
    assert "VALID" in types # Ensure other values are untouched

def test_filter_invalid_accounts(spark):
    """Test if transactions without a matching valid account are filtered out."""
    # Arrange: Create mock transactions (account 3 is missing from accounts table)
    trans_data = [("1", "PRIJEM", 100), ("3", "PRIJEM", 50)]
    df_trans = spark.createDataFrame(trans_data, ["account_id", "type", "amount"])
    
    acc_data = [("1",)]
    df_account = spark.createDataFrame(acc_data, ["account_id"])
    
    # Act: Apply the inner join logic
    df_result = df_trans.join(df_account, "account_id", "inner")
    
    # Assert: Validate that only account "1" remains
    results = df_result.collect()
    assert len(results) == 1
    assert results[0]["account_id"] == "1"
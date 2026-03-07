import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Create a local Spark session specifically for testing purposes
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("TestPipeline").getOrCreate()

def test_clean_transactions(spark):
    # 1. Create mock transaction data (including the 'PRJIEM' typo)
    trans_data = [("1", "PRJIEM", 100), ("2", "VALID", 200), ("3", "PRJIEM", 50)]
    df_trans = spark.createDataFrame(trans_data, ["account_id", "type", "amount"])
    
    # 2. Create mock valid accounts (notice account "3" is intentionally missing)
    acc_data = [("1",), ("2",)]
    df_account = spark.createDataFrame(acc_data, ["account_id"])

    # 3. Apply the data transformation logic
    df_cleaned = df_trans.withColumn("type", when(col("type") == "PRJIEM", "PRIJEM").otherwise(col("type")))
    df_result = df_cleaned.join(df_account, "account_id", "inner")
    
    # 4. Validate the results
    results = df_result.collect()
    
    # Assert that only 2 transactions remain (because account 3 is invalid/missing)
    assert len(results) == 2
    
    # Assert that the typo 'PRJIEM' was successfully corrected to 'PRIJEM'
    types = [row["type"] for row in results]
    assert "PRJIEM" not in types
    assert "PRIJEM" in types
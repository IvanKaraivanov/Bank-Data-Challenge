import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg

def run_pipeline():
    # Fetch credentials from environment variables
    storage_account_name = "stnaviquedata2026"
    client_id = os.getenv("AZURE_CLIENT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")
    tenant_id = os.getenv("AZURE_TENANT_ID")
    endpoint = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"

    # 1. Initialize the Spark session with configurations BUILT-IN
    spark = SparkSession.builder \
        .appName("NaviqueDataPipeline") \
        .config(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth") \
        .config(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") \
        .config(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", client_id) \
        .config(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", client_secret) \
        .config(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", endpoint) \
        .getOrCreate()
    
    # 2. Dynamic Environment Routing
    env = os.getenv("ENV", "TEST")
    print(f"--- Running pipeline in {env} environment ---")
    
    if env == "PROD":
        input_container = "prod-data"
        output_container = "prod-output"
    else:
        input_container = "test-data"
        output_container = "test-output"
    
    # 3. Define the Azure Data Lake Gen2 base paths
    base_input_path = f"abfss://{input_container}@{storage_account_name}.dfs.core.windows.net/"
    base_output_path = f"abfss://{output_container}@{storage_account_name}.dfs.core.windows.net/"

    print(f"1. Reading input files from {input_container}...")
    df_trans = spark.read.csv(base_input_path + "trans.csv", sep=";", header=True, inferSchema=True)
    df_account = spark.read.csv(base_input_path + "account.csv", sep=";", header=True, inferSchema=True)
    df_loan = spark.read.csv(base_input_path + "loan.csv", sep=";", header=True, inferSchema=True)

    print("2. Cleaning transactions data...")
    # Fix the typo in the 'type' column: PRJIEM -> PRIJEM
    df_trans_cleaned = df_trans.withColumn("type", 
        when(col("type") == "PRJIEM", "PRIJEM").otherwise(col("type")))

    # Filter out transactions that do not have a valid account (Inner Join)
    df_trans_valid = df_trans_cleaned.join(df_account.select("account_id"), "account_id", "inner")

    print(f"3. Saving cleaned transactions to {output_container}/cleaned_transactions.parquet ...")
    df_trans_valid.write.mode("overwrite").parquet(base_output_path + "cleaned_transactions.parquet")

    print("4. Calculating average loan amount per district...")
    # Join loans with accounts to extract the district_id, then calculate the average amount
    df_avg_loan = df_loan.join(df_account, "account_id", "inner") \
                         .groupBy("district_id") \
                         .agg(avg("amount").alias("average_loan_amount"))

    print(f"5. Saving aggregated data to {output_container}/avg_loan_per_district.parquet ...")
    df_avg_loan.write.mode("overwrite").parquet(base_output_path + "avg_loan_per_district.parquet")
    
    print("Pipeline execution completed successfully!")

if __name__ == "__main__":
    run_pipeline()
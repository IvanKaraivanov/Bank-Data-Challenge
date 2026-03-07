from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg

def run_pipeline():
    # 1. Initialize the Spark session
    spark = SparkSession.builder.appName("NaviqueDataPipeline").getOrCreate()

    # IMPORTANT: Ensure this matches the Storage Account created via Terraform
    storage_account_name = "stnaviquedata2026"
    container_name = "banking-data"
    
    # Define the Azure Data Lake Gen2 base path
    base_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"
    output_path = base_path + "output/"

    print("1. Reading input files...")
    # Read the CSV files (semicolon separated as per requirements)
    df_trans = spark.read.csv(base_path + "trans.csv", sep=";", header=True, inferSchema=True)
    df_account = spark.read.csv(base_path + "account.csv", sep=";", header=True, inferSchema=True)
    df_loan = spark.read.csv(base_path + "loan.csv", sep=";", header=True, inferSchema=True)

    print("2. Cleaning transactions data...")
    # Fix the typo in the 'type' column: PRJIEM -> PRIJEM
    df_trans_cleaned = df_trans.withColumn("type", 
        when(col("type") == "PRJIEM", "PRIJEM").otherwise(col("type")))

    # Filter out transactions that do not have a valid account (Inner Join)
    df_trans_valid = df_trans_cleaned.join(df_account.select("account_id"), "account_id", "inner")

    print("3. Saving cleaned transactions to Parquet...")
    # Save the cleaned transactions data in Parquet format
    df_trans_valid.write.mode("overwrite").parquet(output_path + "cleaned_transactions.parquet")

    print("4. Calculating average loan amount per district...")
    # Join loans with accounts to extract the district_id, then calculate the average amount
    df_avg_loan = df_loan.join(df_account, "account_id", "inner") \
                         .groupBy("district_id") \
                         .agg(avg("amount").alias("average_loan_amount"))

    # Save the aggregated data in Parquet format
    df_avg_loan.write.mode("overwrite").parquet(output_path + "avg_loan_per_district.parquet")
    
    print("Pipeline execution completed successfully!")

if __name__ == "__main__":
    run_pipeline()
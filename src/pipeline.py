import os
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg

# ==========================================
# 1. THE CORE FRAMEWORK (Reusable Base Class)
# ==========================================
class BaseETLPipeline(ABC):
    """
    Abstract Base Class representing a generic ETL pipeline.
    Handles environment configuration, Spark initialization, and OAuth2 auth.
    """
    def __init__(self, pipeline_name="BaseETLPipeline"):
        self.env = os.getenv("ENV", "TEST")
        self.storage_account_name = "stnaviquedata2026"
        
        print(f"--- Initializing {pipeline_name} in {self.env} environment ---")
        
        # Determine base paths dynamically
        self.input_container = "prod-data" if self.env == "PROD" else "test-data"
        self.output_container = "prod-output" if self.env == "PROD" else "test-output"
        
        self.base_input_path = f"abfss://{self.input_container}@{self.storage_account_name}.dfs.core.windows.net/"
        self.base_output_path = f"abfss://{self.output_container}@{self.storage_account_name}.dfs.core.windows.net/"
        
        # Initialize Spark Session centrally
        self.spark = self._initialize_spark(pipeline_name)

    def _initialize_spark(self, app_name):
        """Sets up Spark with Azure ADLS Gen2 OAuth2 credentials."""
        client_id = os.getenv("AZURE_CLIENT_ID")
        client_secret = os.getenv("AZURE_CLIENT_SECRET")
        tenant_id = os.getenv("AZURE_TENANT_ID")
        endpoint = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"

        return SparkSession.builder \
            .appName(app_name) \
            .config(f"fs.azure.account.auth.type.{self.storage_account_name}.dfs.core.windows.net", "OAuth") \
            .config(f"fs.azure.account.oauth.provider.type.{self.storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") \
            .config(f"fs.azure.account.oauth2.client.id.{self.storage_account_name}.dfs.core.windows.net", client_id) \
            .config(f"fs.azure.account.oauth2.client.secret.{self.storage_account_name}.dfs.core.windows.net", client_secret) \
            .config(f"fs.azure.account.oauth2.client.endpoint.{self.storage_account_name}.dfs.core.windows.net", endpoint) \
            .getOrCreate()

    @abstractmethod
    def extract(self):
        """To be implemented by subclasses"""
        pass

    @abstractmethod
    def transform(self, raw_data):
        """To be implemented by subclasses"""
        pass

    @abstractmethod
    def load(self, transformed_data):
        """To be implemented by subclasses"""
        pass

    def run(self):
        """Template method that orchestrates the ETL execution."""
        try:
            raw_data = self.extract()
            transformed_data = self.transform(raw_data)
            self.load(transformed_data)
            print(f"[{self.__class__.__name__}] Execution completed successfully!")
        except Exception as e:
            print(f"[{self.__class__.__name__}] Failed with error: {str(e)}")
            raise
        finally:
            self.spark.stop()


# ==========================================
# 2. THE SPECIFIC IMPLEMENTATION
# ==========================================
class BankingDataPipeline(BaseETLPipeline):
    """
    Specific pipeline implementation for processing banking transactions and loans.
    Inherits infrastructure setup from BaseETLPipeline.
    """
    def __init__(self):
        # Call the parent class constructor to set up Spark and paths
        super().__init__(pipeline_name="NaviqueBankingPipeline")

    def extract(self):
        print("1. Extracting data from Data Lake...")
        df_trans = self.spark.read.csv(self.base_input_path + "trans.csv", sep=";", header=True, inferSchema=True)
        df_account = self.spark.read.csv(self.base_input_path + "account.csv", sep=";", header=True, inferSchema=True)
        df_loan = self.spark.read.csv(self.base_input_path + "loan.csv", sep=";", header=True, inferSchema=True)
        
        # Returning a dictionary makes it easy to pass multiple DataFrames
        return {"trans": df_trans, "account": df_account, "loan": df_loan}

    def transform(self, raw_data):
        print("2. Transforming and cleaning data...")
        df_trans = raw_data["trans"]
        df_account = raw_data["account"]
        df_loan = raw_data["loan"]

        # Clean typo
        df_trans_cleaned = df_trans.withColumn("type", 
            when(col("type") == "PRJIEM", "PRIJEM").otherwise(col("type")))

        # Filter invalid accounts
        df_trans_valid = df_trans_cleaned.join(df_account.select("account_id"), "account_id", "inner")

        # Aggregate loans
        df_avg_loan = df_loan.join(df_account, "account_id", "inner") \
                             .groupBy("district_id") \
                             .agg(avg("amount").alias("average_loan_amount"))
                             
        return {"cleaned_trans": df_trans_valid, "avg_loan": df_avg_loan}

    def load(self, transformed_data):
        print("3. Loading results to Parquet...")
        transformed_data["cleaned_trans"].write.mode("overwrite").parquet(self.base_output_path + "cleaned_transactions.parquet")
        transformed_data["avg_loan"].write.mode("overwrite").parquet(self.base_output_path + "avg_loan_per_district.parquet")


# ==========================================
# 3. EXECUTION
# ==========================================
if __name__ == "__main__":
    # The orchestration is extremely clean now
    pipeline = BankingDataPipeline()
    pipeline.run()
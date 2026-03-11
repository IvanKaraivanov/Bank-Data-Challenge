High-Level Architecture
The solution is divided into logical layers to ensure scalability, security, and separation of concerns:

Infrastructure (Terraform): Provisions the foundational resources including Azure Data Lake Storage Gen2 (ADLS), Azure Container Registry (ACR), and Azure Key Vault.

Application Logic (PySpark): An Object-Oriented PySpark application that extracts raw banking CSVs from ADLS, applies business transformations (filtering, joining, aggregations), and loads the final dataset as partitioned Parquet files back to the Data Lake.

Containerization & Compute (Docker + ACI): The application is packaged into a Docker image and executed serverless-ly via Azure Container Instances (ACI) to optimize compute costs.

Security & IAM: Authentication between Spark and ADLS is handled via an Azure Service Principal using OAuth2 Client Credentials. Secrets are stored in Azure Key Vault and injected dynamically as secure environment variables, ensuring zero hardcoded credentials in the codebase.

Orchestration (Azure DevOps): A CI/CD pipeline (azure-pipelines.yml) automates unit testing, Docker image building, ACR pushing, and ACI deployment upon every commit.

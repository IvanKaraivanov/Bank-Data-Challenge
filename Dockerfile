# 1. Use a base image that already has Java and Spark installed
FROM bitnami/spark:3.5.0

# 2. Switch to the root user to install necessary Python libraries
USER root

# 3. Set the working directory inside the container
WORKDIR /app

# 4. Copy the source code and tests from the local machine to the container
COPY src/ /app/src/
COPY tests/ /app/tests/

# 5. Install pytest for running automated tests in the CI/CD pipeline
RUN pip install pytest

# 6. Revert to the default non-root user for security best practices
USER 1001

# 7. Define the default command to execute when the container starts.
# IMPORTANT: We add the --packages argument to download Azure Data Lake drivers. 
# Without this, Spark cannot read from the abfss:// protocol in Azure.
ENTRYPOINT [ "spark-submit", "--packages", "org.apache.hadoop:hadoop-azure:3.3.4", "/app/src/pipeline.py" ]
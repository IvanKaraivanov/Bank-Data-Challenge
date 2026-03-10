# 1. Use a base image that already has Java and Spark installed
FROM bitnamilegacy/spark:3.5.1

# 2. Switch to the root user to install necessary Python libraries
USER root

# 3. Set the working directory inside the container
WORKDIR /app

# 4. Install pytest for running automated tests in the CI/CD pipeline
# 4. Install dependencies FIRST for better Docker layer caching
RUN pip install pytest

# 5. Copy the source code and tests from the local machine to the container
COPY src/ /app/src/
COPY tests/ /app/tests/

# 6. Revert to the default non-root user for security best practices
# USER 1001
# Force the container to run as root to satisfy Java's UnixLoginModule native OS checks
USER root

# 7. Define the default command to execute when the container starts.
# IMPORTANT: We add the --packages argument to download Azure Data Lake drivers. 
# Without this, Spark cannot read from the abfss:// protocol in Azure.
ENTRYPOINT [ "spark-submit", "--packages", "org.apache.hadoop:hadoop-azure:3.3.4", "/app/src/pipeline.py" ]
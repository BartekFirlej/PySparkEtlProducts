# Use the Bitnami Spark image
FROM bitnami/spark:latest

USER root

# Install Python packages
RUN pip install pyodbc pandas

RUN pip install pyspark cassandra-driver boto3

# Create a user and set it as the default user for the container
RUN useradd -ms /bin/bash sparkuser
USER sparkuser

# Copy the Spark job script and configuration to the container
COPY --chown=sparkuser:sparkuser main.py /opt/spark/work-dir/
COPY --chown=sparkuser:sparkuser config/mssql_config.py /opt/spark/work-dir/config/
COPY --chown=sparkuser:sparkuser config/cassandra_config.py /opt/spark/work-dir/config/

# Set the working directory
WORKDIR /opt/spark/work-dir/

# Set environment variables for Ivy and Spark
ENV IVY_HOME /home/sparkuser/.ivy2
ENV SPARK_HOME /opt/bitnami/spark

# Run the Spark job
ENTRYPOINT ["spark-submit", "--packages", "com.microsoft.azure:spark-mssql-connector_2.12:1.2.0", "main.py"]

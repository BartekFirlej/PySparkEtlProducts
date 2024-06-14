import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum
import logging
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from config.cassandra_config import CASSANDRA_HOST, CASSANDRA_KEYSPACE, CASSANDRA_PORT
from config.mssql_config import MSSQL_SERVER, MSSQL_DATABASE, MSSQL_PASSWORD, MSSQL_USERNAME

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info("Starting Products ETL job...")

# Configure SparkSession
spark = SparkSession.builder \
    .appName("ProductsEtl") \
    .config("spark.jars.packages", "com.microsoft.azure:spark-mssql-connector_2.12:1.2.0") \
    .getOrCreate()

logger.info("SparkSession initialized.")

# Database connection properties
jdbc_url = f"jdbc:sqlserver://{MSSQL_SERVER};databaseName={MSSQL_DATABASE};"
jdbc_properties = {
    "user": MSSQL_USERNAME,
    "password": MSSQL_PASSWORD,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Define the SQL query without outer parentheses
query = """
SELECT 
    P.ProductID, 
    P.ProductName, 
    P.ProductTypeID, 
    PT.ProductTypeName, 
    OD.Price, 
    OD.Quantity 
FROM 
    OrderDetails OD 
INNER JOIN 
    Products P ON OD.ProductID = P.ProductID 
INNER JOIN 
    ProductTypes PT ON P.ProductTypeID = PT.ProductTypeID
"""

logger.info(f"Executing query: {query}")

# Fetch data using Spark JDBC
df_spark = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", f"({query}) AS subquery") \
    .options(**jdbc_properties) \
    .load()

logger.info("Data fetched from SQL Server.")

# Show fetched data for debugging
logger.info("Showing the first few rows of the fetched data:")
df_spark.show(5)

# Group by ProductID and ProductName, and sum Quantity * Price
df_groupedProducts = df_spark.groupBy("ProductID", "ProductName").agg(
    _sum(col("Quantity") * col("Price")).alias("SumaCen")
)

df_groupedProductTypes = df_spark.groupBy("ProductTypeID", "ProductTypeName").agg(
    _sum(col("Quantity") * col("Price")).alias("SumaCen")
)
logger.info("Data grouped by ProductID and ProductName, and summed Quantity * Price.")

# Show grouped data for debugging
logger.info("Showing the first few rows of the grouped data:")
df_groupedProducts.show(5)
df_groupedProductTypes.show(5)


# Rename columns to Polish names
df_finalProduct = df_groupedProducts.select(
    col("ProductID").alias("id_produktu"),
    col("ProductName").alias("nazwa_produktu"),
    col("SumaCen").alias("suma_cen")
)
df_finalProductTypes = df_groupedProductTypes.select(
    col("ProductTypeID").alias("id_typu_produktu"),
    col("ProductTypeName").alias("nazwa_typu_produktu"),
    col("SumaCen").alias("suma_cen")
)

logger.info("Columns renamed to Polish names.")

# Show final data for debugging
logger.info("Showing the first few rows of the final data:")
df_finalProduct.show(5)
df_finalProductTypes.show(5)

# Function to write data to Cassandra
def write_product_to_cassandra(df, keyspace, table):
    auth_provider = PlainTextAuthProvider(username=os.getenv('CASSANDRA_USERNAME', ''), password=os.getenv('CASSANDRA_PASSWORD', ''))
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT, auth_provider=auth_provider)
    session = cluster.connect(keyspace)

    truncate_statement = session.prepare(f"""
    TRUNCATE {table};
    """)

    insert_statement = session.prepare(f"""
    INSERT INTO {table} (product_id, product_name , product_sum)
    VALUES (?, ?, ?)
    """)

    session.execute(truncate_statement)

    for row in df.collect():
        session.execute(insert_statement, (row.id_produktu, row.nazwa_produktu, row.suma_cen))

    session.shutdown()
    cluster.shutdown()

def write_product_type_to_cassandra(df, keyspace, table):
    auth_provider = PlainTextAuthProvider(username=os.getenv('CASSANDRA_USERNAME', ''), password=os.getenv('CASSANDRA_PASSWORD', ''))
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT, auth_provider=auth_provider)
    session = cluster.connect(keyspace)
    logger.info("DELETING OLD DATA.")
    truncate_statement = session.prepare(f"""
    TRUNCATE {table};
    """)

    insert_statement = session.prepare(f"""
    INSERT INTO {table} (product_type_id, product_type_name , product_type_sum)
    VALUES (?, ?, ?)
    """)

    session.execute(truncate_statement)

    for row in df.collect():
        session.execute(insert_statement, (row.id_typu_produktu, row.nazwa_typu_produktu, row.suma_cen))


    session.shutdown()
    cluster.shutdown()

logger.info("Writing data to Cassandra...")
write_product_to_cassandra(df_finalProduct, CASSANDRA_KEYSPACE, "product_buy_sum")
write_product_type_to_cassandra(df_finalProductTypes, CASSANDRA_KEYSPACE, "product_type_buy_sum")
logger.info("Data written to Cassandra successfully.")

# Stop the Spark session
spark.stop()
logger.info("Spark session stopped. ETL job finished.")

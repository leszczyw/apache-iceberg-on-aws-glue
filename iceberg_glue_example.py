from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def main():
    # Initialize a SparkSession with Iceberg and AWS Glue Catalog support
    spark = SparkSession.builder \
        .appName("IcebergAWSGlueExample") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.warehouse", "s3://your-glue-iceberg-bucket/warehouse/") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()

    # Sample schema definition
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])

    # Sample data
    data = [("1", "Alice", 30), ("2", "Bob", 25), ("3", "Charlie", 35)]

    # Create DataFrame
    df = spark.createDataFrame(data, schema=schema)

    # Define Iceberg table name in Glue Catalog
    table_name = "glue_catalog.db_example.iceberg_table"

    # Create database if not exists (Glue database)
    spark.sql("CREATE DATABASE IF NOT EXISTS db_example")

    # Write data to Iceberg table in Glue Catalog
    df.write.format("iceberg").mode("overwrite").saveAsTable(table_name)

    # Read data back from Iceberg table
    iceberg_df = spark.read.format("iceberg").table(table_name)

    # Show the schema and contents
    iceberg_df.printSchema()
    iceberg_df.show()

    spark.stop()

if __name__ == "__main__":
    main()

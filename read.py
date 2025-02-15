from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import when
import os


def setup_environment():
    os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11"


def create_spark_session(spark_config, protocol):
    builder = SparkSession.builder

    if protocol == "s3a":
        default = {
            "spark.hadoop.fs.s3a.access.key": os.getenv("AWS_ACCESS_KEY_ID", ""),
            "spark.hadoop.fs.s3a.secret.key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
            "spark.sql.catalog.ManagedIcebergCatalog.s3.endpoint": "https://s3.amazonaws.com",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        }
        for key, value in default.items():
            builder = builder.config(key, value)

    for key, value in spark_config.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def main():
    conf = {
        "spark.app.name": "iceberg_lab",
        "spark.jars.packages": "com.amazonaws:aws-java-sdk-bundle:1.12.661,org.apache.hadoop:hadoop-aws:3.3.4,software.amazon.awssdk:bundle:2.29.38,com.github.ben-manes.caffeine:caffeine:3.1.8,org.apache.commons:commons-configuration2:2.11.0,software.amazon.s3tables:s3-tables-catalog-for-iceberg:0.1.3,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.6.1",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.ManagedIcebergCatalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.ManagedIcebergCatalog.catalog-impl": "software.amazon.s3tables.iceberg.S3TablesCatalog",
        "spark.sql.catalog.ManagedIcebergCatalog.warehouse": "XX",
        "spark.sql.catalog.ManagedIcebergCatalog.client.region": "us-east-1",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    }

    setup_environment()
    spark = create_spark_session(spark_config=conf, protocol="s3a")
    spark.sparkContext.setLogLevel("WARN")

    print("Initial data:")
    spark.sql("SELECT * FROM ManagedIcebergCatalog.default.test_table ORDER BY id ASC").show(truncate=False)


main()

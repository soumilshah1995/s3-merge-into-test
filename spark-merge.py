from pyspark.sql import SparkSession
import os
import sys
import time


def setup_environment():
    os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11"


def create_spark_session():
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

    builder = SparkSession.builder
    for key, value in conf.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def merge_job(spark, site_id, num_epochs):
    for epoch in range(1, num_epochs + 1):
        current_time = int(time.time())
        merge_stmt = f"""
        MERGE INTO
            ManagedIcebergCatalog.default.test_table t
        USING 
            (SELECT {site_id} as id, 'site_{site_id}' as site_id, 'Update_{epoch}_{current_time}' as message) s
        ON 
            t.id = s.id AND t.site_id = s.site_id
        WHEN MATCHED THEN 
            UPDATE SET t.message = s.message
        WHEN NOT MATCHED THEN 
            INSERT (id, site_id, message) VALUES (s.id, s.site_id, s.message)
        """
        spark.sql(merge_stmt)
        print(f"Site {site_id}: Merged record for epoch {epoch}")


def main(site_id, num_epochs):
    setup_environment()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    merge_job(spark, site_id, num_epochs)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python writer.py <site_id> <number_of_epochs>")
        sys.exit(1)
    site_id = int(sys.argv[1])
    num_epochs = int(sys.argv[2])
    main(site_id, num_epochs)

#!/bin/bash

SPARK_SUBMIT="spark-submit"
SCRIPT_PATH="$(pwd)/spark-merge.py"
NUM_EPOCHS=5
SITES=10

PACKAGES="--packages com.amazonaws:aws-java-sdk-bundle:1.12.661,org.apache.hadoop:hadoop-aws:3.3.4,software.amazon.awssdk:bundle:2.29.38,com.github.ben-manes.caffeine:caffeine:3.1.8,org.apache.commons:commons-configuration2:2.11.0,software.amazon.s3tables:s3-tables-catalog-for-iceberg:0.1.3,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.6.1"

SPARK_CONF="--master local[*] \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.ManagedIcebergCatalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.ManagedIcebergCatalog.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog \
    --conf spark.sql.catalog.ManagedIcebergCatalog.warehouse=aXX \
    --conf spark.sql.catalog.ManagedIcebergCatalog.client.region=us-east-1"

# Launch separate Spark jobs with different site IDs
for i in $(seq 1 $SITES); do
  $SPARK_SUBMIT $SPARK_CONF $PACKAGES $SCRIPT_PATH $i $NUM_EPOCHS &
done

wait

echo "All jobs completed"

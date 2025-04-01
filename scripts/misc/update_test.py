import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import lit
from awsglue.dynamicframe import DynamicFrame
import boto3

s3 = boto3.client('s3')
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)

# Initialize the Spark session
spark = SparkSession.builder \
    .appName("Delta Lake Upsert Data Aggregations") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .getOrCreate()

# Define S3 bucket paths
output_path = "s3://aws-glue-assets-992382490096-us-east-1/glue-logs/cleaned_filepaths_delta/"

# Bucket and path configurations
bucket_name = 'mbta-tsp-signal'
csv_path_prefix = 'csv'
output_bucket = 'aws-glue-assets-992382490096-us-east-1'
output_key = 'glue-logs/file_paths.parquet'

# Function to list all CSV files recursively
def list_csv_files(bucket, prefix):
    result = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.csv'):
                result.append(f's3://{bucket}/{obj["Key"]}')
    return result

# List CSV files
csv_file_paths = list_csv_files(bucket_name, csv_path_prefix)

# Create a DataFrame from the list of CSV file paths
df = spark.createDataFrame([(path,) for path in csv_file_paths], ["file_path"])
# Add 'silver' column with default value 0
df = df.withColumn("silver", lit(0))

delta_table_exists = DeltaTable.isDeltaTable(spark, output_path)

# If the table exists, read it and append new data
if delta_table_exists:
    # Load the Delta table
    delta_table = DeltaTable.forPath(spark, output_path)
    dfUpdates = delta_table.toDF()
    df = df.join(dfUpdates, on="file_path", how="left_anti")
    
    # Perform UPSERT (MERGE)
    delta_table.alias("target").merge(
        df.alias("source"),
        "target.file_path = source.file_path"
    ).whenNotMatchedInsertAll().execute()
else:
    # If the Delta table does not exist, create one by writing out the current aggregation
    df.write.format("delta").mode("overwrite").save(output_path)

# Stop the Spark session
spark.stop()
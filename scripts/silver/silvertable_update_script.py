import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import lit, col
from awsglue.dynamicframe import DynamicFrame
import boto3
import pandas as pd
import os
from datetime import datetime
import shutil
from tqdm import tqdm
from datetime import datetime, timedelta

s3 = boto3.client('s3')
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)

target_date = datetime.now().strftime('%Y_%m_%d')

# Initialize the Spark session
spark = SparkSession.builder \
    .appName("Delta Lake Upsert Data Aggregations") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .getOrCreate()

# Define S3 bucket paths
bronzetable = "s3://aws-glue-assets-992382490096-us-east-1/glue-logs/cleaned_filepaths_delta/"

delta_table = DeltaTable.forPath(spark, bronzetable)
spark_df = delta_table.toDF().filter("silver = 0").filter(
    (col("file_path").contains(target_date)) & (~col("file_path").contains("_detail"))
)

# Define the patterns you're looking for
patterns = [
"ARL0127","ARL0156","ARL0219","ARL0562","ARL2620","CAM0636",
"CAM4873","MAL0001","MAL0002","MAL0003","MAL0004","MAL0005",
"MAL0006","MAL0007","MALD0001","MALD0002","MALD0003","MALD0004",
"MALD0005","MALD0006","MALD0007","SOM0315","SOM0603","SOM0827",
"ARL004","ARL001","ARL002","ARL003","ARL005","CAM015",
"CAM018","MAL001","MAL002","MAL003","MAL004","MAL006",
"MAL007","MAL009","MAL001","MAL002","MAL003","MAL004",
"MAL006","MAL007","MAL009","SOM001","SOM002","SOM006"
]

# Function to handle pagination and filter files
def list_and_filter_files(bucket_name, prefix, target_date):
    paginator = s3.get_paginator('list_objects_v2')
    filtered_file_paths = []
    
    # Iterate over paginated results
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                file_path = obj['Key']
                
                # Filter for files containing the target date, matching patterns, and NOT containing "_detail"
                if (target_date in file_path) and \
                   any(pattern in file_path for pattern in patterns) and \
                   ("_detail" not in file_path):
                    filtered_file_paths.append(f"s3://{bucket_name}/{file_path}")
    
    return filtered_file_paths

# Specify the S3 bucket and prefix (if any)
bucket_name = 'mbta-tsp-signal'
prefix = 'csv/'

# Get the filtered file paths for the target date
file_paths = list_and_filter_files(bucket_name, prefix, target_date)

def find_common_items(my_list, spark_df, column_name):
    list_df = spark.createDataFrame([(item,) for item in my_list], ["item"])
    common_df = list_df.join(spark_df, list_df.item == col(column_name), "inner")
    common_items = [row.item for row in common_df.select("item").distinct().collect()]
    return common_items

common_items = find_common_items(file_paths, spark_df, "file_path")

def download_files(file_paths, local_dir):
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
    for file_path in tqdm(file_paths, desc="Downloading files"):  # Add tqdm here
        bucket_name = file_path.split('/')[2]
        key = '/'.join(file_path.split('/')[3:])
        local_filename = os.path.join(local_dir, os.path.basename(file_path))
        
        response = s3.get_object(Bucket=bucket_name, Key=key)
        with open(local_filename, 'wb') as file:
            file.write(response['Body'].read())
            
def process_files(file_paths):
    local_directory = '/tmp/downloaded_files/'
    download_files(file_paths, local_directory)

    df_list = []
    for root, dirs, files in os.walk(local_directory):
        for file in tqdm(files, desc="Processing files"):
            file_path = os.path.join(local_directory, os.path.basename(file))
            try:
                df = pd.read_csv(file_path, header=None, names=['time', 'event', 'param'])
                df['intersection_id'] = file.split('_')[0]
                df['date'] = datetime.strptime(f"{file.split('_')[3]}_{file.split('_')[4]}_{file.split('_')[5]}", '%Y_%m_%d')
                df['time'] = pd.to_datetime(df['time']) 
            except Exception as e:
                print(f"Skipping file {file_path}: Could not read as CSV. Error: {e}")
                continue
            except pd.errors.EmptyDataError:
                print(f"Skipping empty file: {file_path}")
                continue
            df_list.append(df)
    for filename in os.listdir(local_directory):
        file_path = os.path.join(local_directory, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))

    return pd.concat(df_list, ignore_index=True)
    
# --- Process in Chunks ---
chunk_size = 100
silvertable = "s3://aws-glue-assets-992382490096-us-east-1/glue-logs/silver_table/"
delta_table_exists = DeltaTable.isDeltaTable(spark, silvertable)

bronze_delta_table = DeltaTable.forPath(spark, bronzetable) 
silver_delta_table = DeltaTable.forPath(spark, silvertable)

for i in range(0, len(common_items), chunk_size):
    chunk = common_items[i:i + chunk_size]
    print(f"Processing chunk {i // chunk_size + 1} of {len(common_items) // chunk_size + 1}")
    
    # Process the chunk
    combined_df = process_files(chunk)
    combined_df = spark.createDataFrame(combined_df)

    # Perform UPSERT (MERGE)
    silver_delta_table.alias("target").merge(
        combined_df.alias("source"),
        "target.intersection_id = source.intersection_id AND target.time = source.time AND target.event = source.event"
        ).whenNotMatchedInsertAll().execute()
    
    test_df = spark.createDataFrame([(path,) for path in chunk], ["file_path"])
    test_df = test_df.withColumn("silver", lit(1))
        
    bronze_delta_table.alias("target").merge(
        test_df.alias("source"),
        "target.file_path = source.file_path"
        ).whenMatchedUpdateAll().execute()

#Stop the Spark session
spark.stop()
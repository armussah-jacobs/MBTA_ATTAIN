import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *

# --- Boilerplate ---
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- Kinesis stream config ---
kinesis_stream_name = "mbtaAvl"
region = "us-east-1"
checkpoint_path = "s3://aws-glue-assets-992382490096-us-east-1/medallion/bronze/avl/_checkpoints/"
delta_output_path = "s3://aws-glue-assets-992382490096-us-east-1/medallion/bronze/avl/"


# --- Read from Kinesis ---
raw_df = spark.readStream \
    .format("kinesis") \
    .option("streamName", kinesis_stream_name) \
    .option("startingPosition", "LATEST") \
    .option("region", region) \
    .load()

# --- Parse JSON from Kinesis records ---
json_df = raw_df.selectExpr("CAST(data AS STRING) as json") \
    .select(from_json(col("json"), """
        timestamp STRING,
        vehicle_id STRING,
        lat DOUBLE,
        lon DOUBLE,
        bearing INT,
        status STRING,
        stop_id STRING,
        stop_sequence INT,
        occupancy STRING,
        route_id STRING,
        trip_id STRING,
        shape_id STRING,
        direction_id INT,
        partition_date STRING
    """).alias("record")) \
    .select("record.*")

# --- Add partition column as DATE type ---
final_df = json_df.withColumn("partition_date", to_date("partition_date"))

# --- Write to Delta ---
query = final_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .partitionBy("partition_date") \
    .option("checkpointLocation", checkpoint_path) \
    .start(delta_output_path)

query.awaitTermination()

job.commit()


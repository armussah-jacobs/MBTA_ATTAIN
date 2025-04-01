import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Resolve required Glue args
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Start Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df = spark.read.format("delta").load("s3://aws-glue-assets-992382490096-us-east-1/medallion/bronze/avl")
df.show(5)
'''
# Delta table registration for Athena (only needs to run once)
spark.sql("""
CREATE TABLE IF NOT EXISTS mbtaglue.avl
USING DELTA
LOCATION 's3://aws-glue-assets-992382490096-us-east-1/medallion/bronze/avl/'
""")

print("✅ Delta table 'mbtaglue.avl' has been registered.")
'''
job.commit()

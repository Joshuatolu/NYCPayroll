import sys
import boto3
import json
import psycopg2
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Initialize boto3 client
s3_client = boto3.client('s3')

# S3 configuration
bucket_name = 'xxxx'
s3_file_path = 'env_file/config.json'
local_file_path = '/tmp/config.json'  # Use /tmp for AWS Glue local file storage

# Download configuration file from S3
try:
    print(f"Downloading {s3_file_path} from bucket {bucket_name}...")
    s3_client.download_file(bucket_name, s3_file_path, local_file_path)
    print("Config file downloaded successfully.")
except Exception as e:
    print(f"Error downloading file from S3: {str(e)}")
    raise

# Load RDS connection details
try:
    with open(local_file_path) as config_file:
        config = json.load(config_file)

    rds_user = config['rds_user']
    rds_password = config['rds_password']
    rds_host = config['rds_host']
    rds_port = config['rds_port']
    rds_db_name = config['rds_db_name']
    rds_schema = config.get('rds_schema')

    print("RDS connection details loaded.")
except Exception as e:
    print(f"Error reading config file: {str(e)}")
    raise

# JDBC URL for PostgreSQL
jdbc_url = f'jdbc:postgresql://{rds_host}:{rds_port}/{rds_db_name}'

# Execute the stored procedure
try:
    print("Connecting to RDS...")
    conn = psycopg2.connect(
        host=rds_host,
        port=rds_port,
        user=rds_user,
        password=rds_password,
        database=rds_db_name
    )

    print("Connected to RDS. Executing stored procedure...")
    cur = conn.cursor()
    cur.execute("CALL stg.GenerateAggregateTables();")
    conn.commit()
    print("Stored procedure executed successfully.")

except Exception as e:
    print(f"Error executing stored procedure: {str(e)}")
    raise
finally:
    if 'cur' in locals():
        cur.close()
    if 'conn' in locals():
        conn.close()
        print("RDS connection closed.")

job.commit()

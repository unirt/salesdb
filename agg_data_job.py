import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3, json

# OUTPUT_DIR = "s3://<put-your-s3-bucket-name>/outputs/customer_dim"
DB_NAME = "salesdb"

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

cust_dyf = glueContext.create_dynamic_frame.from_catalog(database=DB_NAME, table_name="custdb_customer")
cust_site_dyf = glueContext.create_dynamic_frame.from_catalog(database=DB_NAME, table_name="custsitedb_customer_site")

customer_dim_dyf = Join.apply(cust_dyf, cust_site_dyf, 'cust_id', 'cust_id') \
                    .drop_fields(['cust_id'])

glueContext.write_dynamic_frame.from_options(frame=customer_dim_dyf, connection_type="s3", connection_options={"path": OUTPUT_DIR}, format="parquet")

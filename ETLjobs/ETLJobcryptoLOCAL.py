import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, lower, trim, regexp_replace, when, lit
from pyspark.sql import SparkSession
# ## @params: [JOB_NAME]
import os
from datetime import datetime
os.environ['AWS_DEFAULT_REGION'] = 'eu-north-1'

try:
    # Si som a AWS, això funcionarà
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
except:
    # Si som al Docker (Local), inventem un nom pel Job
    args = {'JOB_NAME': 'local_test_job'}

spark = SparkSession.builder.getOrCreate()
glueContext = GlueContext(spark.sparkContext)
spark.sparkContext.setLogLevel("ERROR")

# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session
# job = Job(glueContext)
# job.init(args['JOB_NAME'], args)

# 1. Llegir del glue catalog
##Només funciona a aws consultar al glue data catalog
# datasource = glueContext.create_dynamic_frame.from_catalog(database = "glue-crawler-schema-database", table_name = "crypto")
# df = datasource.toDF()

df = spark.read.parquet("s3://amzn-s3-tfgdl/bronze/crypto/year=2026/month=03/day=31/crypto_2026-03-31_18-38.parquet")

# 2. Neteja de text (treure URLs i caràcters estranys)
columns_to_drop = ["image", "circulating_supply", "atl_date", "atl_change_percentage", "ath_change_percentage", "fully_diluted_valuation", "last_updated", "total_supply", "roi_currency", "roi_percentage", "roi_times","ath_date"]

df_clean = df.drop(*columns_to_drop)
# 3. Omplir nuls
df_final = df_clean.withColumn("current_price", 
    when(col("current_price").isNull(), col("high_24h"))
    .otherwise(col("current_price")))
data = datetime.now()

df_final = df_final.withColumn("year", lit(data.strftime('%Y'))) 

df_final = df_final.withColumn("month", lit(data.strftime('%m')))
            
df_final = df_final.withColumn("day", lit(data.strftime('%d')))
#Abans de guardar passar de DF a DynamicFrame
dynamic_silver = DynamicFrame.fromDF(df_final, glueContext, "dynamic_silver")


glueContext.write_dynamic_frame.from_options(
    frame = dynamic_silver,
    connection_type = "s3",
    connection_options = {"path": "s3://amzn-s3-tfgdl/silver/crypto-silver/",
    "partitionKeys": ["year", "month", "day"]},
    format = "parquet"
)

# job.commit()
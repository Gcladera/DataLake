import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, when, lit

# 1. INICIALITZACIÓ DEL JOB (Obligatori a AWS)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#EXTRACT
datasource = glueContext.create_dynamic_frame.from_catalog(
    database = "glue-crawler-schema-database", 
    table_name = "crypto"
)
df = datasource.toDF()
print("Llista de columnes:", df.columns)
#TRANSFORM
columns_to_drop = [
    "image", "circulating_supply", "atl_date", "atl_change_percentage", 
    "ath_change_percentage", "fully_diluted_valuation", "last_updated", 
    "total_supply", "roi_currency", "roi_percentage", "roi_times", "ath_date"
]
df_clean = df.drop(*columns_to_drop)

#TRANSFORM
df_final = df_clean.withColumn("current_price", 
    when(col("current_price").isNull(), col("high_24h")).otherwise(col("current_price"))
)

#TRANSFORM
data_proces = datetime.now()
df_final = df_final.coalesce(1).withColumn("year", lit(data_proces.strftime('%Y'))) \
                   .withColumn("month", lit(data_proces.strftime('%m'))) \
                   .withColumn("day", lit(data_proces.strftime('%d'))) \
#LOAD
dynamic_silver = DynamicFrame.fromDF(df_final, glueContext, "dynamic_silver")

glueContext.write_dynamic_frame.from_options(
    frame = dynamic_silver,
    connection_type = "s3",
    connection_options = {
        "path": "s3://amzn-s3-tfgdl/silver/crypto-silver/",
        "partitionKeys": ["year", "month", "day"]
    },
    format = "parquet"
)

job.commit()
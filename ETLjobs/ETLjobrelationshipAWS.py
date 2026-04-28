import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, explode, when, lit, first, from_json
from pyspark.sql.types import ArrayType, StringType
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df = glueContext.create_dynamic_frame.from_catalog(
    database="glue-crawler-schema-database",
    table_name="social_media_relationships"
).toDF()

follows_schema = ArrayType(ArrayType(StringType()))
df_correct = df.withColumn("follows_array", from_json(col("follows"), follows_schema))

df_exploded = df_correct.select(
    "author_did",
    explode(col("follows_array")).alias("follow_entry") #explode divideix la llista de relacions i crea una fila per relació.
)

authors = df.select(
    col("author_did").alias("user_id:ID"),
)
followed_users = df_exploded.select(
    col("follow_entry")[0].alias("user_id:ID"),
)
nodes = authors.union(followed_users).distinct().withColumn(":LABEL", lit("User")).coalesce(1)

df_rels = df_exploded.select(
    when(col("follow_entry")[1] == "0", col("author_did")).otherwise(col("follow_entry")[0]).alias(":START_ID"),
    when(col("follow_entry")[1] == "0", col("follow_entry")[0]).otherwise(col("author_did")).alias(":END_ID"),
    lit("FOLLOWS").alias(":TYPE")
).dropDuplicates().coalesce(1)

nodes.write.mode("overwrite").option("header", "true").csv("s3://amzn-s3-tfgdl/silver/posts/neo4j/nodes/")
df_rels.write.mode("overwrite").option("header", "true").csv("s3://amzn-s3-tfgdl/silver/posts/neo4j/relationships/")

job.commit()
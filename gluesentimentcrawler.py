#No se puede ejecutar en local, necesito docker o ejecutar directamente en la nube.

import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Inicialització del context de Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Llegim els JSON de la carpeta bronze
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://el-teu-bucket/bronze/"]},
    format="json"
)

def analyze_sentiment(text):
    if not text: return "NEUTRAL"
    comprehend = boto3.client('comprehend', region_name='us-east-1') # Canvia la teva regió
    try:
        res = comprehend.detect_sentiment(Text=text, LanguageCode='en')
        return res['Sentiment']
    except:
        return "ERROR"

# Registrem la funció perquè Spark la pugui fer servir en paral·lel
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
sentiment_udf = udf(analyze_sentiment, StringType())

# Passem a DataFrame de Spark per manipular-lo millor
df = datasource.toDF()

# Afegim la columna de sentiment cridant a Comprehend
df_with_sentiment = df.withColumn("sentiment", sentiment_udf(df["text_column_name"]))

# Tornem a format Glue i guardem en Parquet (Capa Silver)
glueContext.write_dynamic_frame.from_options(
    frame = DynamicFrame.fromDF(df_with_sentiment, glueContext, "df_final"),
    connection_type = "s3",
    connection_options = {"path": "s3://el-teu-bucket/silver/"},
    format = "parquet"
)
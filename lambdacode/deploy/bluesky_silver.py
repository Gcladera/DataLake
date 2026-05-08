import boto3
import pandas as pd
import io
import logging
import hashlib
import json
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def read_parquet_from_buffer(bucket, prefix):

    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    if 'Contents' not in response or not response['Contents']:
        logger.warning("No files found in the specified S3 prefix.")
        return None

    arxius_ordenats = sorted(response['Contents'], key=lambda obj: obj['LastModified'], reverse=True)
    latest_file_key = arxius_ordenats[0]['Key']

    buffer = io.BytesIO()
    s3_client.download_fileobj(Bucket=bucket, Key=latest_file_key, Fileobj=buffer)
    buffer.seek(0)

    df = pd.read_parquet(buffer)
    logger.info(f"Read {len(df)} rows from S3 file: {latest_file_key}")
    return df


def process_posts_data(df):
    processed_data = []

    for _, row in df.iterrows():
        entry = {
            'id': hashlib.md5(row['cid'].encode()).hexdigest(),
            'cid': row['cid'],
            'author_handle': row['author_handle'],
            'author_did': row['author_did'],
            'text': row['text'],
            'created_at': pd.to_datetime(row['created_at']),
            'indexed_at': pd.to_datetime(row['indexed_at']),
            'like_count': row['like_count'],
            'reply_count': row['reply_count'],
            'repost_count': row['repost_count'],
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        processed_data.append(entry)
    logger.info(f"Processed {len(processed_data)} rows of data.")
    return processed_data

def transform_data_to_parquet(data):
    df = pd.DataFrame(data)
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow", coerce_timestamps='us', allow_truncated_timestamps=True)
    buffer.seek(0)
    return df, buffer

def upload_to_s3(buffer, bucket, key):
    s3 = boto3.client('s3')
    s3.upload_fileobj(
        Fileobj=buffer,
        Bucket=bucket,
        Key=key
    )

def lambda_handler(event, context):
    try:
        bucket = 'amzn-s3-tfgdl'
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
        year = datetime.now().strftime('%Y')
        month = datetime.now().strftime('%m')
        day = datetime.now().strftime('%d')

        prefix = f"bronze/posts-bronze/post-content-bronze/year={year}/month={month}/day={day}/"

        df_posts = read_parquet_from_buffer(bucket, prefix)

        if df_posts is None or df_posts.empty:
            logger.warning("No data to process. Exiting.")
            return

        logger.info(f"Processing {len(df_posts)} rows of data.")
        processed_posts = process_posts_data(df_posts)

        if not processed_posts:
            logger.warning("Processed data is empty. No data to upload.")
            return

        key_posts = f'silver/posts-silver/post-content-silver/year={year}/month={month}/day={day}/posts_{timestamp}.parquet'

        logger.info("Silver data transformation completed successfully")

        _, buffer_processed_posts = transform_data_to_parquet(processed_posts)

        logger.info(f"Uploading {len(processed_posts)} rows to S3: {key_posts}")
        upload_to_s3(
            buffer=buffer_processed_posts,
            bucket='amzn-s3-tfgdl',
            key=key_posts
        )
        log_data = {
                    "message": "Processant compte de Bluesky",
                    "status": "success",
                    "custom_metric": len(processed_posts)
                }

        logger.info(json.dumps(log_data))

    except Exception as e:
        logger.error(f"Error in lambda_handler: {e}")
        raise

lambda_handler(None, None)
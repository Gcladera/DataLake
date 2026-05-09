import logging
import os
import io
import boto3
from datetime import datetime
import pandas as pd
import hashlib
import json
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def hash_row(row):
    row_str = '|'.join(str(x) for x in row.values)
    return hashlib.md5(row_str.encode()).hexdigest()

def convert_df_to_parquet(df):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow", coerce_timestamps='us', allow_truncated_timestamps=True)
    buffer.seek(0)
    return buffer

def get_latest_s3_object(bucket, prefix):

    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    if 'Contents' not in response or not response['Contents']:
        logger.warning("No files found in the specified S3 prefix.")
        return None

    arxius_ordenats = sorted(response['Contents'], key=lambda obj: obj['LastModified'], reverse=True)
    latest_file_key = arxius_ordenats[0]['Key']
    logger.info(f"Latest S3 file selected: {latest_file_key}")
    return latest_file_key

def read_parquet_from_s3(bucket, key):
    """
    Reads a parquet file from S3 and returns a pandas DataFrame.
    """
    s3 = boto3.client('s3')
    
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_parquet(io.BytesIO(response['Body'].read()))
        return df
    except ClientError as e:
        logger.error(f"Error reading parquet file from S3: {e}")
        raise

def transform_market_ranking_to_silver(df):
    """
    Transforms market ranking data from bronze to silver.
    Applies validation and data transformations.
    """
    try:
        essential_cols = ['id', 'name', 'symbol', 'row_id', 'timestamp']
        missing_cols = [col for col in essential_cols if col not in df.columns]
        if missing_cols:
            logger.warning(f"Missing columns in market ranking data: {missing_cols}")
        
        df = df.dropna(subset=['id', 'name', 'symbol'])
        
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        logger.info(f"Transformed {len(df)} market ranking records to silver")
        return df
    except Exception as e:
        logger.error(f"Error transforming market ranking data: {e}")
        raise

def transform_sentiment_to_silver(df):
    """
    Transforms sentiment data from bronze to silver.
    Applies validation and data transformations.
    """
    try:
        essential_cols = ['coin_id', 'sentiment_up_percentage', 'sentiment_down_percentage', 'timestamp']
        missing_cols = [col for col in essential_cols if col not in df.columns]
        if missing_cols:
            logger.warning(f"Missing columns in sentiment data: {missing_cols}")
        
        df = df.dropna(subset=['coin_id'])
        
        # Convert sentiment percentages to numeric and fill nulls with 0
        df['sentiment_up_percentage'] = pd.to_numeric(df['sentiment_up_percentage'], errors='coerce').fillna(0)
        df['sentiment_down_percentage'] = pd.to_numeric(df['sentiment_down_percentage'], errors='coerce').fillna(0)
        df['watchlist_count'] = pd.to_numeric(df['watchlist_count'], errors='coerce').fillna(0)
        
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        logger.info(f"Transformed {len(df)} sentiment records to silver")
        return df
    except Exception as e:
        logger.error(f"Error transforming sentiment data: {e}")
        raise

def transform_trending_to_silver(df):
    """
    Transforms trending data from bronze to silver.
    Applies validation and data transformations.
    """
    try:
    
        df = df.dropna(subset=['category_id', 'name'])
        
        numeric_cols = ['coins_count', 'market_cap_usd', 'volume_24h_usd', 'change_24h_usd', 'change_1h_usd']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        logger.info(f"Transformed {len(df)} trending records to silver")
        return df
    except Exception as e:
        logger.error(f"Error transforming trending data: {e}")
        raise

def lambda_handler(event, context):
    try:
        s3 = boto3.client('s3')
        bucket = 'amzn-s3-tfgdl'
        processed_datasets = []
        
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
        year = datetime.now().strftime('%Y')
        month = datetime.now().strftime('%m')
        day = datetime.now().strftime('%d')
        final_path = f'/year={year}/month={month}/day={day}/'

        #Process market ranking data
        try:
            market_ranking_key = get_latest_s3_object(bucket, 'bronze/crypto-bronze/market-ranking'+final_path)
            if market_ranking_key:
                logger.info(f"Processing market ranking from: {market_ranking_key}")
                market_ranking_df = read_parquet_from_s3(bucket, market_ranking_key)
                market_ranking_silver = transform_market_ranking_to_silver(market_ranking_df)
                market_ranking_parquet = convert_df_to_parquet(market_ranking_silver)
                
                s3.upload_fileobj(
                    Bucket=bucket,
                    Key=f'silver/crypto-silver/market-ranking-silver/year={year}/month={month}/day={day}/market-ranking_{timestamp}.parquet',
                    Fileobj=market_ranking_parquet
                )
                logger.info("Market ranking data uploaded to silver")
                processed_datasets.append('market_ranking')
        except Exception as e:
            logger.error(f"Error processing market ranking data: {e}")
        
        # Process sentiment data
        try:
            sentiment_key = get_latest_s3_object(bucket, 'bronze/crypto-bronze/sentiment'+final_path)
            if sentiment_key:
                logger.info(f"Processing sentiment from: {sentiment_key}")
                sentiment_df = read_parquet_from_s3(bucket, sentiment_key)
                sentiment_silver = transform_sentiment_to_silver(sentiment_df)
                sentiment_parquet = convert_df_to_parquet(sentiment_silver)
                
                s3.upload_fileobj(
                    Bucket=bucket,
                    Key=f'silver/crypto-silver/sentiment-silver/year={year}/month={month}/day={day}/sentiment_{timestamp}.parquet',
                    Fileobj=sentiment_parquet
                )
                logger.info("Sentiment data uploaded to silver")
                processed_datasets.append('sentiment')
        except Exception as e:
            logger.error(f"Error processing sentiment data: {e}")
        
        # Process trending data
        try:
            trending_key = get_latest_s3_object(bucket, 'bronze/crypto-bronze/trending'+final_path)
            if trending_key:
                logger.info(f"Processing trending from: {trending_key}")
                trending_df = read_parquet_from_s3(bucket, trending_key)
                trending_silver = transform_trending_to_silver(trending_df)
                trending_parquet = convert_df_to_parquet(trending_silver)
                
                s3.upload_fileobj(
                    Bucket=bucket,
                    Key=f'silver/crypto-silver/trending-silver/year={year}/month={month}/day={day}/trending_{timestamp}.parquet',
                    Fileobj=trending_parquet
                )
                logger.info("Trending data uploaded to silver")
                processed_datasets.append('trending')
        except Exception as e:
            logger.error(f"Error processing trending data: {e}")
        
        log_data = {
            "message": "Processant datos de bronze a silver",
            "status": "success",
            "timestamp": timestamp
        }
        
        logger.info(json.dumps(log_data))
        return {
            'statusCode': 200,
            'body': json.dumps(log_data)
        }

    except Exception as e:
        logger.error(f"Error in lambda_handler: {e}")
        raise
lambda_handler(None, None)
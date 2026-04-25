import logging
import os
import io
from coingecko_sdk import Coingecko
import boto3
from datetime import datetime
import time
import pandas as pd
from dotenv import load_dotenv
import hashlib
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)
load_dotenv(dotenv_path="/mnt/windows/Users/user/Documents/PERSONAL/Grau/Educacion/UAB/Any4/TFG/Codi/.env")

def hash_row(row):
    row_str = '|'.join(str(x) for x in row.values)
    return hashlib.md5(row_str.encode()).hexdigest()

def get_market_data(client):
    try:
        coins = client.coins.markets.get(vs_currency='eur', order='market_cap_desc', per_page=20, page=1)
    except Exception as e:
        logger.error(f"Error in get_market_data: {e}")
        raise
    df = pd.DataFrame([vars(item) for item in coins])

    df['roi_currency'] = df['roi'].apply(lambda x: x.currency if x else None)
    df['roi_percentage'] = df['roi'].apply(lambda x: x.percentage if x else None)
    df['roi_times'] = df['roi'].apply(lambda x: x.times if x else None)
    df = df.drop(columns=['roi'])
    df['row_id'] = df.apply(hash_row, axis=1)    
    df['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    return df
    

def get_trending_data(client):
    try:
        trending = client.search.trending.get()    
    except Exception as e:
        logger.error(f"Error getting trending data: {e}")
        raise

    categories_list = []
    for cat in trending.categories:
        categories_list.append({
            'category_id': cat.id,
            'name': cat.name,
            'coins_count': cat.coins_count,
            'market_cap_usd': cat.data.market_cap,
            'volume_24h_usd': cat.data.total_volume,
            'change_24h_usd': cat.data.market_cap_change_percentage_24h.usd,
            'change_1h_usd': cat.market_cap_1h_change,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })

    df_categories = pd.DataFrame(categories_list)
    return df_categories

def get_community_sentiment(client, coin_ids):
    sentiment_list = []
    for cid in coin_ids:
        try:
            coin_data = client.coins.get_id(
                id=cid, 
                localization=False, 
                tickers=False, 
                market_data=False, 
                developer_data=False, 
            )
        except Exception as e:
            logger.warning(f"Error fetching sentiment for {cid}: {e}")
            
        coin_dict = vars(coin_data) if hasattr(coin_data, '__dict__') else coin_data #Converteix un objecte a dict si no ho és

        sentiment_list.append({
            'coin_id': cid,
            'sentiment_up_percentage': coin_dict.get('sentiment_votes_up_percentage'),
            'sentiment_down_percentage': coin_dict.get('sentiment_votes_down_percentage'),
            'watchlist_count': coin_dict.get('watchlist_portfolio_users'),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        
        time.sleep(1.5) #per no s'aturar l'API
        
    
    df_sentiment = pd.DataFrame(sentiment_list) 
    return df_sentiment

def initialise_api_client():
    try:
        client = Coingecko(
            demo_api_key = os.getenv("COINGECKO_API_KEY"),
            environment="demo",)
        return client
    except Exception as e:
        logger.error(f"Error in initialise_api_client: {e}")
        raise

def convert_df_to_parquet(df):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow", coerce_timestamps='us', allow_truncated_timestamps=True)
    buffer.seek(0)
    return buffer
  

def lambda_handler(event, context):
    try:
        client = initialise_api_client()
        top_20_df = get_market_data(client)
        coin_ids = top_20_df['id'].tolist()
        sentiment_data = get_community_sentiment(client, coin_ids)
        trending_data = get_trending_data(client) 
        print(sentiment_data)
        print('-----------------------------')
        print(trending_data)

        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
        year = datetime.now().strftime('%Y')
        month = datetime.now().strftime('%m')
        day = datetime.now().strftime('%d')
        
        
        top_20_parquet = convert_df_to_parquet(top_20_df)
        sentiment_parquet = convert_df_to_parquet(sentiment_data)
        trending_parquet = convert_df_to_parquet(trending_data)

        s3 = boto3.client('s3')

        s3.upload_fileobj(
            Bucket='amzn-s3-tfgdl',
            Key=f'bronze/crypto/market_ranking/year={year}/month={month}/day={day}/crypto_{timestamp}.parquet',
            Fileobj=top_20_parquet
        )

        s3.upload_fileobj(
            Bucket='amzn-s3-tfgdl',
            Key=f'bronze/crypto/sentiment/year={year}/month={month}/day={day}/sentiment_{timestamp}.parquet',
            Fileobj=sentiment_parquet
        )

        s3.upload_fileobj(
                Bucket='amzn-s3-tfgdl',
                Key=f'bronze/crypto/trending/year={year}/month={month}/day={day}/trending_{timestamp}.parquet',
                Fileobj=trending_parquet
                )
        
        log_data = {
            "message": "Processant compte de CoinGecko",
            "status": "success",
            "custom_metric": 42
        }
        
        logger.info(json.dumps(log_data))

    except Exception as e:
        logger.error(f"Error in lambda_handler: {e}")
        raise

lambda_handler(None, None)
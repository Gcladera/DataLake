import os
import io
import boto3
from datetime import datetime, timezone, timedelta
import pandas as pd 
from coingecko_sdk import Coingecko
from atproto import Client
import hashlib
import logging
import json
from dotenv import load_dotenv

logger = logging.getLogger()
logger.setLevel(logging.INFO)

load_dotenv(dotenv_path="/mnt/windows/Users/user/Documents/PERSONAL/Grau/Educacion/UAB/Any4/TFG/Codi/.env")

def get_posts(search_attributes, client):
    try:
        posts = get_latests_posts(client, search_attributes)
        return posts
    except Exception as e:
        logger.error(f"Error in get_posts: {e}")
        raise

def get_relationships(client: Client, actors: list):
    try:
        social_graph = []

        for actor in actors:
            try:
                profile = client.get_profile(actor=actor)
                
                followers_dids = []
                cursor = None
                while True:
                    res = client.get_followers(actor=actor, cursor=cursor)
                    followers_dids.extend([(f.did, '0') for f in res.followers])
                    cursor = res.cursor
                    if not cursor:
                        break

                follows_dids = []
                cursor = None
                while True:
                    res = client.get_follows(actor=actor, cursor=cursor)
                    follows_dids.extend([(f.did, '1') for f in res.follows])
                    cursor = res.cursor
                    if not cursor:
                        break

                f = followers_dids + follows_dids
                social_graph.append( {
                    'id': hashlib.md5(actor.encode()).hexdigest(),
                    'display_name': profile.display_name,
                    'follows': f,
                    'author_did': profile.did,
                    'author_handle': profile.handle,
                    }
                )
            except Exception as e:
                logger.error(f"Error processing actor {actor}: {e}")
                social_graph.append(None)

        return social_graph
    except Exception as e:
        logger.error(f"Error in get_relationships: {e}")
        raise

def get_crypto_data():
    try:
        client = initialise_cryptos_api_client()
        coins = client.coins.markets.get(vs_currency='eur', order='market_cap_desc', per_page=20, page=1)
        coins_list = []
        for i in range(len(coins)):
            if i == len(coins)-1:
             search_attributes = f"{coins[i].name} OR {coins[i].symbol}"
            else:   
                search_attributes = f"{coins[i].name} OR {coins[i].symbol} OR"
            
            coins_list.append(search_attributes)
        coins_list = " ".join(coins_list)
        return coins_list
    except Exception as e:
        logger.error(f"Error in get_crypto_data: {e}")
        raise

def initialise_cryptos_api_client():
    try:
        client = Coingecko(
            demo_api_key = os.getenv("COINGECKO_API_KEY"),
            environment="demo",)
        
        return client
    except Exception as e:
        logger.error(f"Error initializing cryptos API client: {e}")
        raise

def initialise_posts_api_client():
    try:
        client = Client()
        client.login(login="grau.cladera@autonoma.cat", password="mENp8HEbpv9kUid")
        return client
    except Exception as e:
        logger.error(f"Error initializing posts API client: {e}")
        raise

def transform_data_to_parquet(data):
    try:
        df = pd.DataFrame(data)
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow", coerce_timestamps='us', allow_truncated_timestamps=True)
        buffer.seek(0)
        return df, buffer
    except Exception as e:
        logger.error(f"Error in transform_data_to_parquet: {e}")
        raise

def get_latests_posts(client, search_attributes):
    try:
        # antiquity = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat()
        response = client.app.bsky.feed.search_posts(
            params={
                'q': " ".join(search_attributes),
                # 'since': antiquity,
                'sort': 'latest',  
                'limit': 20        
            }
        )
        return response
    except Exception as e:
        logger.error(f"Error in get_latests_posts: {e}")
        raise

def process_posts_data(data):
    try:
        processed_data = []
        for post in data:
            if post[0] == 'posts':
                for postview in post[1]:
                    entry = {
                        'id': hashlib.md5(postview.cid.encode()).hexdigest(),
                        'cid': postview.cid, #content identifier
                        'author_handle': postview.author.handle,
                        'author_did': postview.author.did,
                        'text': postview.record.text,
                        'created_at': pd.to_datetime(postview.record.created_at),
                        'indexed_at': pd.to_datetime(postview.indexed_at),
                        'like_count': postview.like_count,
                        'reply_count': postview.reply_count,
                        'repost_count': postview.repost_count,
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    processed_data.append(entry)
        return processed_data
    except Exception as e:
        logger.error(f"Error in process_posts_data: {e}")
        raise

def lambda_handler(event, context):
    try:
        client = initialise_posts_api_client()
        search_attributes = get_crypto_data()
        data = get_posts(search_attributes=search_attributes, client=client)
        cleaned_data = process_posts_data(data)
        accounts = set([post.author.did for post in data.posts])        

        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
        year = datetime.now().strftime('%Y')
        month = datetime.now().strftime('%m')
        day = datetime.now().strftime('%d')

        _, buffer_posts = transform_data_to_parquet(cleaned_data)
        
        relationships = get_relationships(client=client, actors=accounts)
        _, buffer_relationships = transform_data_to_parquet(relationships)


        s3 = boto3.client('s3')
        s3.upload_fileobj(
            Bucket='amzn-s3-tfgdl',
            Key=f'bronze/posts/post_content/year={year}/month={month}/day={day}/posts_{timestamp}.parquet',
            Fileobj=buffer_posts
            )

        s3.upload_fileobj(
            Bucket='amzn-s3-tfgdl',
            Key=f'bronze/posts/social_media_relationships/year={year}/month={month}/day={day}/relationships_{timestamp}.parquet',
            Fileobj=buffer_relationships
            )
    
        log_data = {
            "message": "Processant compte de Bluesky",
            "status": "success",
            "custom_metric": 42
        }
        
        logger.info(json.dumps(log_data))

    except Exception as e:
        logger.error(f"Error in lambda_handler: {e}")
        raise

lambda_handler(None, None)
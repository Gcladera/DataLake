import os
import io
import boto3
from datetime import datetime
import pandas as pd 
from coingecko_sdk import Coingecko
from atproto import Client
import hashlib
import logging
import json
from dotenv import load_dotenv
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

load_dotenv(dotenv_path="/mnt/windows/Users/user/Documents/PERSONAL/Grau/Educacion/UAB/Any4/TFG/Codi/.env")

def get_bluesky_secret():
    secret_name = "BlueSkyAPICredentials"
    region_name = "eu-north-1"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except Exception as e:
        raise e

    secret_string = get_secret_value_response['SecretString']
    secret = json.loads(secret_string)
    return secret

def get_crypto_secret():
    secret_name = "CryptoAPICredentials"
    region_name = "eu-north-1"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except Exception as e:
        raise e

    secret_string = get_secret_value_response['SecretString']
    secret = json.loads(secret_string)
    return secret

def get_posts(search_attributes, client):
    try:
        posts = get_latests_posts(client, search_attributes)
        return posts
    except Exception as e:
        logger.error(f"Error in get_posts: {e}")
        raise

def get_relationships(client: Client, actors: list):
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
                'follows': json.dumps(f),
                'author_did': profile.did,
                'author_handle': profile.handle,
                }
            )
        except Exception as e:
            logger.error(f"Error processing actor {actor}: {e}")
            social_graph.append(None)

    return social_graph

def initialise_cryptos_api_client():
    try:
        secret = get_crypto_secret()
        client = Coingecko(
            demo_api_key = secret["COINGECKO_API_KEY"],
            environment="demo",)
        
        return client
    except Exception as e:
        logger.error(f"Error initializing cryptos API client: {e}")
        raise

def get_crypto_data(client):
    try:
        coins = client.coins.markets.get(vs_currency='eur', order='market_cap_desc', per_page=20, page=1)
    except Exception as e:
        logger.error(f"Error in get_crypto_data: {e}")
        raise

    coins_list = []
    for i in range(len(coins)):
        coins_list.append(coins[i].name)
    return coins_list
   

def initialise_posts_api_client():
    try:
        secret = get_bluesky_secret()
        client = Client()
        client.login(login=secret['BS_USER'], password=secret['BS_PASSWORD'])
        return client
    except Exception as e:
        logger.error(f"Error initializing posts API client: {e}")
        raise

def transform_data_to_parquet(data):
    df = pd.DataFrame(data)
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow", coerce_timestamps='us', allow_truncated_timestamps=True)
    buffer.seek(0)
    return df, buffer


def get_latests_posts(client, search_attributes):
    total_posts = []
    for coin in search_attributes:
        try:
            response = client.app.bsky.feed.search_posts(
                params={
                    'q': coin,
                    'sort': 'latest',  
                    'limit': 20        
                }
            )
            total_posts.append(response)            
            time.sleep(1)
        except Exception as e:
            logger.warning(f"Error searching posts {coin}: {e}")

    return total_posts

def process_posts_data(data):
    processed_data = []
    for batch in data:
        for post in batch:
            if post[0] == 'posts':
                for postview in post[1]:
                    entry = {
                        'id': hashlib.md5(postview.cid.encode()).hexdigest(),
                        'cid': postview.cid,
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
  

def lambda_handler(event, context):
    try:
        client_posts = initialise_posts_api_client()
        client_crypto = initialise_cryptos_api_client()
        search_attributes = get_crypto_data(client=client_crypto)
        data = get_posts(search_attributes=search_attributes, client=client_posts)
        cleaned_data = process_posts_data(data)
        accounts_count = {}
        for post in cleaned_data:
            did = post['author_did']
            accounts_count[did] = accounts_count.get(did, 0) + 1

        top_accounts = sorted(accounts_count.items(), key=lambda x: x[1], reverse=True)
        accounts = [did for did, _ in top_accounts[:15]]
        
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
        year = datetime.now().strftime('%Y')
        month = datetime.now().strftime('%m')
        day = datetime.now().strftime('%d')

        _, buffer_posts = transform_data_to_parquet(cleaned_data)
        
        relationships = get_relationships(client=client_posts, actors=accounts)
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
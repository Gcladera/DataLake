import os
import io
import boto3
from datetime import datetime, timezone, timedelta
import pandas as pd 
from coingecko_sdk import Coingecko
from atproto import Client
import hashlib

def get_relationships(client: Client, actors: list):
    social_graph = []

    for actor in actors:
        try:
            profile = client.get_profile(actor=actor)
            
            followers_dids = [()]
            cursor = None
            while True:
                res = client.get_followers(actor=actor, cursor=cursor)
                followers_dids.append([(f.did, '0') for f in res.followers])
                
                cursor = res.cursor
                if not cursor:
                    break

            follows_dids = [()]
            cursor = None
            while True:
                res = client.get_follows(actor=actor, cursor=cursor)
                follows_dids.append([(f.did, '1') for f in res.follows])
                
                cursor = res.cursor
                if not cursor:
                    break

            f = followers_dids + follows_dids
            social_graph.append( {
                'id': hashlib.md5(actor.encode()).hexdigest(),
                'display_name': profile.display_name,
                'follows': f
                }
            )
        except Exception as e:
            print(f"Error processant {actor}: {e}")
            social_graph.append(None)

    return social_graph

def initialise_posts_api_client():
    client = Client()
    client.login(login="grau.cladera@autonoma.cat", password="mENp8HEbpv9kUid")
    return client

def transform_data_to_parquet(data):
    df = pd.DataFrame(data)
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow", coerce_timestamps='us', allow_truncated_timestamps=True)
    buffer.seek(0)
    return df, buffer

   
a = get_relationships(client=initialise_posts_api_client(), actors=['bigcoinreport.bsky.social', 'bs-xknows.bsky.social'])
b = transform_data_to_parquet(a)
print(b.shape)

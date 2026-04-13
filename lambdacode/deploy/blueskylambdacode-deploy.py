import os
import io
import boto3
from datetime import datetime, timezone, timedelta
import pandas as pd
import pickle 
from atproto import Client
import emoji
import uuid

def get_data(call_api=False, search_attributes=[]): #para mas querys meter if's y argumentos de T/F
    if call_api:
        client = initialise_api_client()
        posts = get_latests_posts(client, search_attributes)
        # with open('posts.pkl', 'wb') as f:
        #     pickle.dump(posts, f)
    else:
        with open('posts.pkl', 'rb') as f:
            posts = pickle.load(f)
    return posts

def initialise_api_client():
    client = Client()
    client.login(login="grau.cladera@autonoma.cat", password="mENp8HEbpv9kUid")
    return client


def get_latests_posts(client, search_attributes):
    hace_30_min = (datetime.now(timezone.utc) - timedelta(minutes=180)).isoformat()
    response = client.app.bsky.feed.search_posts(
        params={
            'q': search_attributes,
            # 'since': hace_30_min,
            'sort': 'latest',     
            'limit': 20           
        }
    )
    return response

def lambda_handler(event, context):
    search_attributes = "bitcoin OR crypto" #No funciona lo del or con #
    data = get_data(call_api=True, search_attributes=search_attributes)
         
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
    year = datetime.now().strftime('%Y')
    month = datetime.now().strftime('%m')
    day = datetime.now().strftime('%d')
    
    cleaned_data = []
    for post in data:
        if post[0] == 'posts':
            for postview in post[1]:
                entry = {
                    'uri': postview.uri,
                    'cid': postview.cid, #content identifier
                    'author_handle': postview.author.handle,
                    'text': postview.record.text,
                    'created_at': pd.to_datetime(postview.record.created_at),
                    'indexed_at': pd.to_datetime(postview.indexed_at),
                    'like_count': postview.like_count,
                    'reply_count': postview.reply_count,
                    'repost_count': postview.repost_count
                }
                cleaned_data.append(entry)

    df = pd.DataFrame(cleaned_data)
    # df['text'] = df['text'].apply(lambda x: emoji.replace_emoji(str(x), replace='')) #Sino dona errors amb parquet.
    # df['text'] = df['text'].astype(str).str.encode('utf-8', 'ignore').str.decode('utf-8')
    # df = df.fillna("")

    df['id'] = [str(uuid.uuid4()) for _ in range(len(df))]

# Opción mucho más rápida para datasets grandes
    # df.to_parquet(f'data/posts/posts_{timestamp}.parquet')
    # s3 = boto3.client('s3')
    # s3.upload_file(f'data/posts/posts_{timestamp}.parquet', 'amzn-s3-tfgdl', f'posts/{year}/{month}/{day}/posts_{timestamp}.parquet')

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow", coerce_timestamps='us', allow_truncated_timestamps=True)
    buffer.seek(0)

    s3 = boto3.client('s3')
    s3.put_object(
        Bucket='amzn-s3-tfgdl',
        Key=f'bronze/posts/year={year}/month={month}/day={day}/posts_{timestamp}.parquet',
        Body=buffer
        )
lambda_handler("", "")
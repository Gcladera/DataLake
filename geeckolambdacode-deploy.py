import os
import io
from coingecko_sdk import Coingecko
import boto3
from datetime import datetime, timezone
import pandas as pd
import pickle 


def get_data(call_api=False):
    if call_api:
        client = initialise_api_client()
        coins = client.coins.markets.get(vs_currency='eur', order='market_cap_desc', per_page=100, page=1)
        # with open('coins.pkl', 'wb') as f:
        #     pickle.dump(coins, f)
        return coins
    else:
        with open('coins.pkl', 'rb') as f:
            data = pickle.load(f)
        return data


def initialise_api_client():
    client = Coingecko(
        demo_api_key = os.getenv("COINGECKO_API_KEY"),
        environment="demo",)
    
    return client

def lambda_handler(event, context):
    data = get_data(call_api=True)

    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M') #realmente no hace falta el año en este caso.
    year = datetime.now().strftime('%Y')
    month = datetime.now().strftime('%m')
    day = datetime.now().strftime('%d')
    df = pd.DataFrame([vars(item) for item in data])

    #Columna roi da problemas. En la mayoria de monedas es None
    df['roi_currency'] = df['roi'].apply(lambda x: x.currency if x else None)
    df['roi_percentage'] = df['roi'].apply(lambda x: x.percentage if x else None)
    df['roi_times'] = df['roi'].apply(lambda x: x.times if x else None)
    df = df.drop(columns=['roi'])
            
    #df.to_parquet(f'data/crypto/crypto_{timestamp}.parquet')

    #s3 = boto3.client('s3')
    #s3.upload_file(f'data/crypto/crypto_{timestamp}.parquet', 'amzn-s3-tfgdl', f'crypto/{year}/{month}/{day}/crypto_{timestamp}.parquet'
    
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    s3 = boto3.client('s3')
    s3.put_object(
        Bucket='amzn-s3-tfgdl',
        Key=f'bronze/crypto/year={year}/month={month}/day={day}/crypto_{timestamp}.parquet',
        Body=buffer
        )
    

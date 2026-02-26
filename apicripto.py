import os
from dotenv import load_dotenv
from coingecko_sdk import Coingecko

load_dotenv()


client = Coingecko(
    demo_api_key = os.getenv("COINGECKO_API_KEY"),
    environment="demo", # "demo" to initialize the client with Demo API access
)

#All coins
# coins = client.coins.list.get()
# for moneda in coins[:10]:
#     print(moneda.name)

#Coin para un momento dado (historico)
# coins = client.coins.history.get(id='bitcoin', date='2026-01-10')

#Precio actual de la moneda
# value = client.simple.price.get(ids='bitcoin', vs_currencies='eur')
# print(value['bitcoin'].eur)

#Top 100 monedas
#ordenadas por capitalización de mercado =  numero_acciones * valor de las acciones

top_100 = client.coins.markets.get(vs_currency='eur', order='market_cap_desc', per_page=100, page=1)
for coin in top_100:
    print(coin.market_cap_rank, coin.name, coin.symbol.upper(), coin.current_price)

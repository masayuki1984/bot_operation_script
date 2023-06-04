# Bybitのローソク足データを取得するコード
import time
from datetime import datetime, timedelta
import pandas as pd
import ccxt
import yaml
import boto3

def fetch_yaml_config() -> dict:
    config = "/script/config.yml"
    with open(config, "r", encoding="utf-8") as yml:
        config = yaml.safe_load(yml)
    return config


# configファイルから設定値を取得する
config = fetch_yaml_config()
ACCESS_KEY = config['AWS']['ACCESS_KEY']
SECRET_ACCESS_KEY = config['AWS']['SECRET_ACCESS_KEY']
s3 = boto3.resource(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_ACCESS_KEY,
    region_name='ap-northeast-1'
)
bucket = s3.Bucket(config['AWS']['BUCKET'])
SAVE_DIR = config['bybit']['PAXG_USDT']['SAVEDIR']
CRYPT = config['bybit']['PAXG_USDT']['SYMBOL']
TIME_FRAME = config['bybit']['PAXG_USDT']['TIME_FRAME']
FILENAME_PREFIX = config['bybit']['PAXG_USDT']['FILENAME']
UPLOAD_PATH = config['bybit']['PAXG_USDT']['UPLOAD_PATH']

# PAXG_USDTのccxt設定
bybit = ccxt.bybit() 
bybit.loadMarkets() 
symbol = bybit.market(CRYPT)['symbol'] 

# データ取得期間の設定
start_date = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
start_time = bybit.parse8601(start_date + 'T00:00:00Z') 
start_time = int(start_time) - (60*60*9*1000) 

end_time = bybit.parse8601(start_date + 'T23:59:59Z')
end_time = int(end_time) - (60*60*9*1000)

timedict = {'1m':60, '5m':300, '15m':900, 
            '30m':1800, '1h':3600, '4h':14400, '1d':86400, '1w':604800} 

# データ取得を開始する。Bybitは１回の送受信で200本までしかデータを取得できないので繰り返し処理で取得を行う
df = pd.DataFrame()
while start_time <= end_time:  
    data = bybit.fetchOHLCV(symbol, TIME_FRAME, start_time) 
    df_child = pd.DataFrame(data) 
    df = pd.concat([df, df_child]) 
    start_time = start_time + (timedict[TIME_FRAME] * 1000 * 200) 
    
# 作成したデータには列名が入っていないため、列名を入れる
col_names = ['open_time', 'open', 'high', 'low', 'close', 'volume'] 
df.columns = col_names

# 作成したデータを時間表示がUNIX時間のため、分かりやすい時刻表示に変える
df['open_time'] = df['open_time'].apply(lambda x: str(x)[0:10]) 
df['open_time'] = df['open_time'].apply(lambda x:datetime.fromtimestamp(int(x)).strftime('%Y-%m-%d %H:%M'))
df = df[df["open_time"].str.contains(start_date, na=False)]

# ファイルをS3 Bucketにアップロードする
temp_yesterday = (datetime.now() - timedelta(1))
yesterday_year = str(temp_yesterday.year).zfill(4)
yesterday_month = str(temp_yesterday.month).zfill(2)
yesterday_day = str(temp_yesterday.day).zfill(2)
full_filename = FILENAME_PREFIX + "{}.csv".format(yesterday_year + yesterday_month + yesterday_day)
df.to_csv(SAVE_DIR + full_filename, index=False, encoding="utf-8")
bucket.upload_file(SAVE_DIR + full_filename, UPLOAD_PATH + full_filename)
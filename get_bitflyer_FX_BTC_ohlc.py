from datetime import datetime, timedelta
import time
import requests
import boto3
import pandas as pd
import yaml

def fetch_yaml_config() -> dict:
    config = "/script/bitflyer/config.yml"
    with open(config, "r", encoding="utf-8") as yml:
        config = yaml.safe_load(yml)
    return config


config = fetch_yaml_config()
ACCESS_KEY = config["ACCESS_KEY"]
SECRET_ACCESS_KEY = config["SECRET_ACCESS_KEY"]
s3 = boto3.resource('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_ACCESS_KEY, region_name='ap-northeast-1')
bucket = s3.Bucket(config["BUCKET"])
SAVE_DIR = "/tmp/"
upload_path = "bitflyer/FX_BTC/daily_temp/"
target_coin = "FX_BTC_JPY"

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36'}
today_now = datetime.now()
today_year = str(today_now.year).zfill(4)
today_month = str(today_now.month).zfill(2)
today_day = str(today_now.day).zfill(2)
yesterday_now = today_now + timedelta(days=-1)
yesterday_year = str(yesterday_now.year).zfill(4)
yesterday_month = str(yesterday_now.month).zfill(2)
yesterday_day = str(yesterday_now.day).zfill(2)
start_datetime = datetime.strptime("{}-{}-{} 09:00:00".format(today_year, today_month, today_day), "%Y-%m-%d %H:%M:%S")
end_datetime = datetime.strptime("{}-{}-{} 12:00:00".format(yesterday_year, yesterday_month, yesterday_day), "%Y-%m-%d %H:%M:%S")

ohlc_list=[]
while start_datetime > end_datetime:
    unixtime = start_datetime.timestamp() * 1000
    response = requests.get( f"https://lightchart.bitflyer.com/api/ohlc?symbol={target_coin}&period=m&before={unixtime}", headers= headers).json()
    ohlc_list.extend(response)
    start_datetime -= timedelta(minutes=720)
    time.sleep(1)

df_1m = pd.DataFrame(ohlc_list,columns=['timestamp', 'op', 'hi', 'lo', 'cl', 'volume','volume_buy_sum','volume_sell_sum','volume_buy','volume_sell'])
df_1m["timestamp"] = pd.to_datetime(df_1m["timestamp"]/1000,unit='s', utc=True)
df_1m.set_index("timestamp",inplace=True)
df_1m.sort_index(inplace=True)
file_name = "FX_BTC_JPY_ohlcv_{}.pkl".format(yesterday_year + yesterday_month + yesterday_day)
df_1m.to_pickle(SAVE_DIR + file_name, protocol = 4)
bucket.upload_file(SAVE_DIR + file_name, upload_path + file_name)
import sys
import os
import yaml
import boto3
import calendar
import glob
import pandas as pd
import shutil


def fetch_yaml_config() -> dict:
    config = "/script/config.yml"
    with open(config, "r", encoding="utf-8") as yml:
        config = yaml.safe_load(yml)
    return config

def get_daily_download_filename(target_year:int, target_month:int, target_file:str) -> list:
    target_month_last_day = calendar.monthrange(target_year, target_month)[1]
    target_file_list = []
    for day in range(1, target_month_last_day + 1):
        target_file_list.append(target_file.format(str(target_year).zfill(4) + str(target_month).zfill(2) + str(day).zfill(2)))

    return target_file_list


# 環境変数読み込み
config = fetch_yaml_config()
ACCESS_KEY = config['AWS']['ACCESS_KEY']
SECRET_ACCESS_KEY = config['AWS']['SECRET_ACCESS_KEY']
s3 = boto3.resource('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_ACCESS_KEY, region_name='ap-northeast-1')
bucket = s3.Bucket(config['AWS']['BUCKET'])
target_year = sys.argv[1]
target_month = sys.argv[2]
target_year_month = target_year.zfill(4) + target_month.zfill(2)
SAVE_DIR = config['bybit']['PAXG_USDT']['SAVEDIR']
download_path = config['bybit']['PAXG_USDT']['DOWNLOAD_PATH']
target_file = "PAXG_USDT_ohlcv_{}.pkl"

#対象年月のs3ダウンロードファイル取得処理
download_files = get_daily_download_filename(int(target_year), int(target_month), target_file)
os.makedirs(SAVE_DIR + target_year_month, exist_ok=True)

for download_file in download_files:
    bucket.download_file(download_path + download_file, SAVE_DIR + target_year_month + '/' + download_file)

# 日次pickleファイルを読み込んでまとめる処理
read_files = glob.glob(SAVE_DIR + target_year_month + '/*')
aggregate_file_list = []
read_files.sort()
for file in read_files:
    aggregate_file_list.append(pd.read_pickle(file))

aggregated_file = pd.concat(aggregate_file_list)

# 月次Dataframeをpickle形式のファイルにしてs3にアップロードする
aggregated_file.to_pickle(SAVE_DIR + target_year_month + '/' + target_file.format(target_year_month), protocol = 4)
bucket.upload_file(
    SAVE_DIR + target_year_month + '/' + target_file.format(target_year_month),
    "bybit/PAXG_USDT/" + target_file.format(target_year_month)
)

# スクリプトの作業ディレクトリ(/tmp/YYYYMM)の削除
shutil.rmtree(SAVE_DIR + target_year_month + '/')
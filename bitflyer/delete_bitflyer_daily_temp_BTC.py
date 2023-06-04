import sys
import yaml
import boto3
import calendar

def fetch_yaml_config() -> dict:
    config = "/script/bitflyer/config.yml"
    with open(config, "r", encoding="utf-8") as yml:
        config = yaml.safe_load(yml)
    return config

def get_daily_download_filename(target_year:int, target_month:int, target_file:str) -> list:
    target_month_last_day = calendar.monthrange(target_year, target_month)[1]
    target_file_list = []
    for day in range(1, target_month_last_day + 1):
        target_file_list.append(target_file.format(str(target_year).zfill(4) + str(target_month).zfill(2) + str(day).zfill(2)))

    return target_file_list

def delete_objects(bucket, object_keys):
    bucket.delete_objects(Delete={
        'Objects': [{
            'Key': key
        } for key in object_keys]
    })


# 環境変数読み込み
config = fetch_yaml_config()
ACCESS_KEY = config["ACCESS_KEY"]
SECRET_ACCESS_KEY = config["SECRET_ACCESS_KEY"]
s3 = boto3.resource('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_ACCESS_KEY, region_name='ap-northeast-1')
bucket = s3.Bucket(config["BUCKET"])
target_year = sys.argv[1]
target_month = sys.argv[2]
save_path = "bitflyer/BTC/daily_temp/"
target_file = "BTC_JPY_ohlcv_{}.pkl"

#対象年月のs3ダウンロードファイル削除処理
delete_file_names = get_daily_download_filename(int(target_year), int(target_month), target_file)
delete_files = [save_path + file for file in delete_file_names]
delete_objects(bucket, delete_files)
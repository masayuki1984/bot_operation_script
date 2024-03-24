# GMOコインでDAIを積立購入するスクリプト
import requests
import json
import hmac
import hashlib
import time
import math
import calendar
from decimal import Decimal, ROUND_DOWN
import datetime

dt_now = datetime.datetime.now()

def fetch_yaml_config() -> dict:
    config = "/script/config.yml"
    with open(config, "r", encoding="utf-8") as yml:
        config = yaml.safe_load(yml)
    return config

# configファイルから設定値を取得する
config = fetch_yaml_config()
API_KEY = config['gmo_coin']['DAI_JPY']['API_KEY']
API_SECRET = config['gmo_coin']['DAI_JPY']['API_SECRET']
SYMBOL = config['gmo_coin']['DAI_JPY']['SYMBOL']
TARGET_AMOUNT = config['gmo_coin']['DAI_JPY']['TARGET_AMOUNT']
DEVIATION = config['gmo_coin']['DAI_JPY']['DEVIATION']
PRIVATE_ENDPOINT = config['gmo_coin']['DAI_JPY']['PRIVATE_ENDPOINT']

def get_usd_jpy_price() -> float:
    """
    USD/JPYの価格を取得する
    
    Returns
    usd_jpy_price : float
        USD/JPYの価格(bidとaskの平均値)
    """
    endPoint = 'https://forex-api.coin.z.com/public'
    path = '/v1/ticker'
    symbol_list = requests.get(endPoint + path)
    for symbol in symbol_list.json()['data']:
        if symbol['symbol'] == 'USD_JPY':
            usd_jpy_ask = float(symbol['ask'])
            usd_jpy_bid = float(symbol['bid'])

    usd_jpy_price = (usd_jpy_ask + usd_jpy_bid) / 2
    return usd_jpy_price

def get_dai_jpy_price() -> float:
    """
    DAI/JPYのbid(max)価格を取得する
    
    Returns
    buying_board_max_price : float
        DAI/JPYの価格(bidのMAX値)
    """
    endPoint = 'https://api.coin.z.com/public'
    path = '/v1/ticker?symbol=DAI'
    response = requests.get(endPoint + path)
    buying_board_max_price = float(response.json()['data'][0]['bid'])
    return buying_board_max_price

def check_price_deviation(usd_jpy_price: float, buying_board_max_price: float) -> float:
    """
    USD/JPYの価格とDAI/JPYの価格の価格差（乖離幅）をDAI/JPY価格で割った指標を計算する

    Parameters
    ----------
    usd_jpy_price : float
        USD/JPY価格
    buying_board_max_price : float
        DAI/JPY価格（買い板のMAX値）
    
    Returns
    deviation_rate : float
        USD/JPYとDAI/JPYの乖離率
    """
    deviation_rate = (usd_jpy_price - buying_board_max_price) / buying_board_max_price * 100
    return deviation_rate

def get_dai_balance(API_KEY, API_SECRET, SYMBOL, PRIVATE_ENDPOINT) -> int:
    """
    保有しているDAIの量を取得する

    Parameters
    ----------
    API_KEY : str
        APIキー
    API_SECRET: str
        APIシークレット
    SYMBOL: str
        対象の通貨
    PRIVATE_ENDPOINT: str
        PrivateAPIのエンドポイント
    
    Returns
    symbol_amount : int
        対象の通貨の所有数
    """
    timestamp = '{0}000'.format(int(time.mktime(datetime.now().timetuple())))
    method    = 'GET'
    endPoint  = PRIVATE_ENDPOINT
    path      = '/v1/account/assets'

    text = timestamp + method + path
    sign = hmac.new(bytes(API_SECRET.encode('ascii')), bytes(text.encode('ascii')), hashlib.sha256).hexdigest()

    headers = {
        "API-KEY": API_KEY,
        "API-TIMESTAMP": timestamp,
        "API-SIGN": sign
    }

    res = requests.get(endPoint + path, headers=headers)
    if res.json()['status'] != 0:
        # エラー処理
        pass

    for symbol_dict in res.json()['data']:
        if SYMBOL in symbol_dict.values():
            symbol_amount = int(symbol_dict['amount'])

    return symbol_amount

def check_own_dai_is_achieved(symbol_amount: int, TARGET_AMOUNT: int) -> bool:
    """
    保有しているDAIが目標数に達しているかを確認する

    Parameters
    ----------
    symbol_amount: int
        対象の通貨の所有数
    TARGET_AMOUNT: int
        通貨の目標保有数
    
    Returns
    is_achieved : bool
        通貨が目標保有数に達している場合True、達していない場合False
    """
    if symbol_amount >= TARGET_AMOUNT:
        is_achieved = True
    else:
        is_achieved = False

    return is_achieved


def calc_left_days() -> int:
    """
    月の残日数を取得する

    Returns
    left_days : int
        月の残日数
    """
    dt_now = datetime.now()
    dt_year = dt_now.year
    dt_month = dt_now.month
    left_days = calendar.monthrange(dt_year, dt_month)[1]
    return left_days

def get_active_order(symbol):
    pass

def order_cancel(order_id_list):
    pass

def check_own_jpy_is_enough() -> bool:
    pass

def  order(API_KEY, API_SECRET, buying_price, buying_size):
    # レート取得
    endPoint = 'https://api.coin.z.com/public'
    path = '/v1/ticker?symbol=BTC'

    res = requests.get(endPoint + path)
    for i in res.json()["data"]:
        rate = int(i["last"])

    # 一ヶ月の予算を入れる
    monthly_budget = 1
    # その月の日数で割る
    daily_budget = int(math.floor(monthly_budget / calendar.monthrange(dt_now.year, dt_now.month)[1]))

    # 一日あたりの注文数量
    # 最低注文数量の0.0001単位で丸める。
    amount = daily_budget / rate
    order_amount = Decimal(str(amount)).quantize(Decimal('0.0001'), rounding=ROUND_DOWN)

    # 注文
    timestamp = '{0}000'.format(int(time.mktime(dt_now.timetuple())))
    method    = 'POST'
    endPoint  = 'https://api.coin.z.com/private'
    path      = '/v1/order'
    reqBody = {
        "symbol": "BTC",
        "side": "BUY",
        "executionType": "MARKET",
        "size": float(order_amount)
    }

    text = timestamp + method + path + json.dumps(reqBody)
    sign = hmac.new(bytes(API_SECRET.encode('ascii')), bytes(text.encode('ascii')), hashlib.sha256).hexdigest()

    headers = {
        "API-KEY": API_KEY,
        "API-TIMESTAMP": timestamp,
        "API-SIGN": sign
    }

    res = requests.post(endPoint + path, headers=headers, data=json.dumps(reqBody))
    print (json.dumps(res.json(), indent=2))


# USD/JPYの価格取得
usd_jpy_price = get_usd_jpy_price()

# DAI/JPYの価格取得(買い板のMAX値)
buying_board_max_price = get_dai_jpy_price()
    
# USD/JPYとDAI/JPYの価格を比較して設定値以上の乖離があるか確認
order_valid = DEVIATION < check_price_deviation(usd_jpy_price, buying_board_max_price)

# DAI資産残高取得
dai_balance = get_dai_balance(API_KEY, API_SECRET, SYMBOL, PRIVATE_ENDPOINT)

# 所有DAIが目標数に達しているか確認
is_achieved = check_own_dai_is_achieved(dai_balance, TARGET_AMOUNT)

if order_valid and not is_achieved:
    # 当月の残り日数を算出
    left_days = calc_left_days()

    # (月間目標購入数 - 所有DAI数) / (当月の残り日数) 
    buying_size = math.ceil((TARGET_AMOUNT - dai_balance) / left_days)

    # 有効注文がある場合、注文のキャンセルをおこなう
    order_id_list = get_active_order(SYMBOL)
    order_cancel(order_id_list)

    # 通貨を買うためのJPY資産が充分にあるか確認をおこなう
    is_enough_jpy = check_own_jpy_is_enough()

    # JPY資産が足りない場合はDiscordへの通知をおこなう(一度通知をおこなったら24時間は通知をおこなわない)
    if not is_enough_jpy:
        pass

    # 注文を入れる(買い板MAX値に+0.001した値)
    buying_price = buying_board_max_price + 0.001
    order(API_KEY, API_SECRET, buying_price, buying_size)

import requests
import json
import os

from si_prefix import si_format

ticker_file_path = 'files/tickers.json'
ticker_reverse_file_path = 'files/tickers_reverse.json'

if not os.path.isfile(ticker_file_path):
    with open(ticker_file_path, 'w') as f:
        f.write('{}')

if not os.path.isfile(ticker_reverse_file_path):
    with open(ticker_reverse_file_path, 'w') as f:
        f.write('{}')


def set_prefix(number):
    if number < 1000:
        return number
    else:
        return si_format(number, precision=2)


def get_new_ticker_file():
    url_ticker = 'https://s3-us-west-2.amazonaws.com/data.pooltool.io/stats/tickers.json'
    try:
        r = requests.get(url_ticker)
        if r.ok:
            return r.json()
    except requests.exceptions.RequestException as e:
        return 'error'


def handle_wallet_newpool(data):
    with open(ticker_file_path, 'w') as f:
        data = get_new_ticker_file()
        if data != 'error':
            json.dump(data, f)
        with open(ticker_reverse_file_path, 'r') as ticker:
            tickers = json.load(ticker)
        with open(ticker_reverse_file_path, 'w') as reverse_f:
            reverse_dic = {}
            for pool in data:
                reverse_dic[data[pool]['ticker'].upper()] = reverse_dic.get(data[pool]['ticker'].upper(), [])
                reverse_dic[data[pool]['ticker'].upper()].append(pool)

            for ticker in reverse_dic:
                if ticker not in tickers:
                    tickers[ticker] = tickers.get(ticker, [])
                    for pool_id in reverse_dic[ticker]:
                        tickers[ticker].append(pool_id)
                else:
                    for pool_id in reverse_dic[ticker]:
                        if pool_id not in tickers[ticker]:
                            tickers[ticker].append(pool_id)

            json.dump(tickers, reverse_f)

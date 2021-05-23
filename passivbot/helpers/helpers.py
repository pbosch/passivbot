import argparse
import datetime
import json
import os
from time import time

import hjson
from dateutil import parser


def first_capitalized(s: str):
    return s[0].upper() + s[1:].lower()


def format_tick(tick: dict) -> dict:
    return {'trade_id': int(tick['id']),
            'price': float(tick['price']),
            'qty': float(tick['qty']),
            'timestamp': date_to_ts(tick['time']),
            'is_buyer_maker': tick['side'] == 'Sell'}


async def fetch_ticks(cc, symbol: str, from_id: int = None, do_print=True) -> [dict]:
    params = {'symbol': symbol, 'limit': 1000}
    if from_id:
        params['from'] = max(0, from_id)
    try:
        fetched_trades = await cc.v2_public_get_trading_records(params=params)
    except Exception as e:
        print(e)
        return []
    trades = [format_tick(t) for t in fetched_trades['result']]
    if do_print:
        print_(['fetched trades', symbol, trades[0]['trade_id'],
                ts_to_date(trades[0]['timestamp'] / 1000)])
    return trades


def date_to_ts(date: str):
    return parser.parse(date).timestamp() * 1000


def get_keys():
    return ['inverse', 'do_long', 'do_shrt', 'qty_step', 'price_step', 'min_qty', 'min_cost',
            'contract_multiplier', 'ddown_factor', 'qty_pct', 'leverage', 'n_close_orders',
            'grid_spacing', 'pos_margin_grid_coeff', 'volatility_grid_coeff',
            'volatility_qty_coeff', 'min_markup', 'markup_range', 'ema_span', 'ema_spread',
            'stop_loss_liq_diff', 'stop_loss_pos_pct', 'entry_liq_diff_thr']


def sort_dict_keys(d):
    if type(d) == list:
        return [sort_dict_keys(e) for e in d]
    if type(d) != dict:
        return d
    return {key: sort_dict_keys(d[key]) for key in sorted(d)}


def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if type(v) == dict:
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def make_get_filepath(filepath: str) -> str:
    '''
    if not is path, creates dir and subdirs for path, returns path
    '''
    dirpath = os.path.dirname(filepath) if filepath[-1] != '/' else filepath
    if not os.path.isdir(dirpath):
        os.makedirs(dirpath)
    return filepath


def load_key_secret(exchange: str, user: str) -> (str, str):
    try:
        keyfile = json.load(open('api-keys.json'))
        # Checks that the user exists, and it is for the correct exchange
        if user in keyfile and keyfile[user]["exchange"] == exchange:

            # If we need to get the `market` key:
            # market = keyfile[user]["market"]
            # print("The Market Type is " + str(market))

            keyList = [str(keyfile[user]["key"]), str(keyfile[user]["secret"])]

            return keyList
        elif user not in keyfile or keyfile[user]["exchange"] != exchange:
            print("Looks like the keys aren't configured yet, or you entered the wrong username!")
        raise Exception('API KeyFile Missing!')
    except FileNotFoundError:
        print("File Not Found!")
        raise Exception('API KeyFile Missing!')


def print_(args, r=False, n=False):
    line = ts_to_date(time())[:19] + '  '
    str_args = '{} ' * len(args)
    line += str_args.format(*args)
    if n:
        print('\n' + line, end=' ')
    elif r:
        print('\r' + line, end=' ')
    else:
        print(line)
    return line


def ts_to_date(timestamp: float) -> str:
    return str(datetime.datetime.fromtimestamp(timestamp)).replace(' ', 'T')


def filter_orders(actual_orders: [dict], ideal_orders: [dict], keys: [str] = ['symbol', 'side', 'qty', 'price']) -> (
        [dict], [dict]):
    # returns (orders_to_delete, orders_to_create)

    if not actual_orders:
        return [], ideal_orders
    if not ideal_orders:
        return actual_orders, []
    actual_orders = actual_orders.copy()
    orders_to_create = []
    ideal_orders_cropped = [{k: o[k] for k in keys} for o in ideal_orders]
    actual_orders_cropped = [{k: o[k] for k in keys} for o in actual_orders]
    for ioc, io in zip(ideal_orders_cropped, ideal_orders):
        matches = [(aoc, ao) for aoc, ao in zip(actual_orders_cropped, actual_orders) if aoc == ioc]
        if matches:
            actual_orders.remove(matches[0][1])
            actual_orders_cropped.remove(matches[0][0])
        else:
            orders_to_create.append(io)
    return actual_orders, orders_to_create


def flatten(lst: list) -> list:
    return [y for x in lst for y in x]


def add_argparse_args(parser):
    parser.add_argument('--nojit', help='disable numba', action='store_true')
    parser.add_argument('-b', '--backtest_config', type=str, required=False, dest='backtest_config_path',
                        default='configs/backtest/default.hjson', help='backtest config hjson file')
    parser.add_argument('-o', '--optimize_config', type=str, required=False, dest='optimize_config_path',
                        default='configs/optimize/default.hjson', help='optimize config hjson file')
    parser.add_argument('-d', '--download-only', help='download only, do not dump ticks caches', action='store_true')
    parser.add_argument('-s', '--symbol', type=str, required=False, dest='symbol',
                        default='none', help='specify symbol, overriding symbol from backtest config')
    parser.add_argument('-u', '--user', type=str, required=False, dest='user',
                        default='none',
                        help='specify user, a.k.a. account_name, overriding user from backtest config')
    return parser


def get_passivbot_argparser():
    parser = argparse.ArgumentParser(prog='passivbot', description='run passivbot')
    parser.add_argument('user', type=str, help='user/account_name defined in api-keys.json')
    parser.add_argument('symbol', type=str, help='symbol to trade')
    parser.add_argument('live_config_path', type=str, help='live config to use')
    return parser


async def create_binance_bot(config: dict):
    from bots.binance import BinanceBot
    bot = BinanceBot(config)
    await bot._init()
    return bot


async def create_bybit_bot(config: dict):
    from bots.bybit import BybitBot
    bot = BybitBot(config)
    await bot._init()
    return bot


def get_dummy_settings(user: str, exchange: str, symbol: str):
    return {**{k: 1.0 for k in get_keys()},
            **{'user': user, 'exchange': exchange, 'symbol': symbol, 'config_name': '', 'logging_level': 0}}


async def fetch_market_specific_settings(user: str, exchange: str, symbol: str):
    tmp_live_settings = get_dummy_settings(user, exchange, symbol)
    settings_from_exchange = {}
    if exchange == 'binance':
        bot = await create_binance_bot(tmp_live_settings)
        settings_from_exchange['maker_fee'] = 0.00018
        settings_from_exchange['taker_fee'] = 0.00036
        settings_from_exchange['exchange'] = 'binance'
    elif exchange == 'bybit':
        bot = await create_bybit_bot(tmp_live_settings)
        settings_from_exchange['maker_fee'] = -0.00025
        settings_from_exchange['taker_fee'] = 0.00075
        settings_from_exchange['exchange'] = 'bybit'
    else:
        raise Exception(f'unknown exchange {exchange}')
    await bot.session.close()
    if 'inverse' in bot.market_type:
        settings_from_exchange['inverse'] = True
    elif 'linear' in bot.market_type:
        settings_from_exchange['inverse'] = False
    else:
        raise Exception('unknown market type')
    for key in ['max_leverage', 'min_qty', 'min_cost', 'qty_step', 'price_step', 'max_leverage',
                'contract_multiplier']:
        settings_from_exchange[key] = getattr(bot, key)
    return settings_from_exchange


async def prep_config(args) -> dict:
    try:
        bc = hjson.load(open(args.backtest_config_path))
    except Exception as e:
        raise Exception('failed to load backtest config', args.backtest_config_path, e)
    try:
        oc = hjson.load(open(args.optimize_config_path))
    except Exception as e:
        raise Exception('failed to load optimize config', args.optimize_config_path, e)
    config = {**oc, **bc}
    if args.symbol != 'none':
        config['symbol'] = args.symbol
    if args.user != 'none':
        config['user'] = args.user
    end_date = config['end_date'] if config['end_date'] and config['end_date'] != -1 else ts_to_date(time())[:16]
    config['session_name'] = f"{config['start_date'].replace(' ', '').replace(':', '').replace('.', '')}_" \
                             f"{end_date.replace(' ', '').replace(':', '').replace('.', '')}"

    base_dirpath = os.path.join('backtests', config['exchange'], config['symbol'])
    config['caches_dirpath'] = make_get_filepath(os.path.join(base_dirpath, 'caches', ''))
    config['optimize_dirpath'] = make_get_filepath(os.path.join(base_dirpath, 'optimize', ''))
    config['plots_dirpath'] = make_get_filepath(os.path.join(base_dirpath, 'plots', ''))

    if os.path.exists((mss := config['caches_dirpath'] + 'market_specific_settings.json')):
        market_specific_settings = json.load(open(mss))
    else:
        market_specific_settings = await fetch_market_specific_settings(config['user'], config['exchange'],
                                                                        config['symbol'])
        json.dump(market_specific_settings, open(mss, 'w'), indent=4)
    config.update(market_specific_settings)

    # setting absolute min/max ranges
    for key in ['qty_pct', 'ddown_factor', 'ema_span', 'grid_spacing']:
        if key in config['ranges']:
            config['ranges'][key][0] = max(0.0, config['ranges'][key][0])
    for key in ['qty_pct']:
        if key in config['ranges']:
            config['ranges'][key][1] = min(1.0, config['ranges'][key][1])

    if 'leverage' in config['ranges']:
        config['ranges']['leverage'][1] = min(config['ranges']['leverage'][1], config['max_leverage'])
        config['ranges']['leverage'][0] = min(config['ranges']['leverage'][0], config['ranges']['leverage'][1])

    return config


def load_live_config(path: str) -> dict:
    try:
        live_config = json.load(open(args.live_config_path))
        if 'entry_liq_diff_thr' not in live_config:
            live_config['entry_liq_diff_thr'] = live_config['stop_loss_liq_diff']
    except Exception as e:
        raise Exception(f'failed to load live config {path}, {e}')

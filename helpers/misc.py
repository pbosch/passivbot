import datetime
import os
from collections import OrderedDict
from typing import Union, Tuple

import numpy as np
import pandas as pd
from ray.tune import uniform, randint, choice
from ray.tune.sample import Float, Integer

from definitions.candle import empty_candle, empty_candle_list
from definitions.tick import empty_tick_list
from helpers.converters import convert_array_to_tick_list, candles_to_array
from helpers.optimized import prepare_candles
from helpers.print_functions import print_


def sort_dict_keys(d: Union[dict, list]) -> Union[dict, list]:
    """
    Sort dictionaries and keys.
    :param d: The object to sort.
    :return: A sorted list or dictionary.
    """
    if type(d) == list:
        return [sort_dict_keys(e) for e in d]
    if type(d) != dict:
        return d
    return {key: sort_dict_keys(d[key]) for key in sorted(d)}


def ts_to_date(timestamp: float) -> str:
    """
    Converts timestamp to human readable time.
    :param timestamp: The epoch timestamp.
    :return: A human readable time.
    """
    return str(datetime.datetime.fromtimestamp(timestamp)).replace(' ', 'T')


def get_filenames(filepath: str) -> list:
    """
    Get filenames of saved CSV files.
    :param filepath: Path to CSV files.
    :return: List of sorted file names.
    """
    return sorted([f for f in os.listdir(filepath) if f.endswith(".csv")],
                  key=lambda x: int(eval(x[:x.find("_")].replace(".cs", "").replace("v", ""))))


def make_get_filepath(filepath: str) -> str:
    """
    If the filepath is not a path, it creates the directory and sub directories and returns the path.
    :param filepath: The file path to use.
    :return: The actual path.
    """
    dirpath = os.path.dirname(filepath) if filepath[-1] != '/' else filepath
    os.makedirs(dirpath, exist_ok=True)
    return filepath


def get_utc_now_timestamp() -> int:
    """
    Creates a millisecond based timestamp of UTC now.
    :return: Millisecond based timestamp of UTC now.
    """
    return int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)


def create_test_data(filepath: str, tick_interval: float = 0.25):
    """
    Basic function to create test data for the backtester. Reads CSV files, creates ticks out of it, aggregates ticks
    into candles, and saved them in a numpy array. Can later be used in the downloader to create arrays for the
    backtester.
    :param filepath: Path to CSV files.
    :param tick_interval: Tick interval to use, defaults to 0.25 seconds.
    :return:
    """
    files = get_filenames(filepath)
    last_tick_update = 0
    tick_list = empty_tick_list()
    last_candle = empty_candle()
    candles = empty_candle_list()
    for f in files[:10]:
        df = pd.read_csv(os.path.join(filepath, f))
        if last_tick_update == 0:
            last_tick_update = int(df.iloc[0]['timestamp'] - (df.iloc[0]['timestamp'] % (tick_interval * 1000)))
        next_update = int(df.iloc[-1]['timestamp'] - (df.iloc[-1]['timestamp'] % (tick_interval * 1000))) + int(
            tick_interval * 1000)
        tick_list = convert_array_to_tick_list(tick_list, df[['timestamp', 'price', 'qty', 'is_buyer_maker']].values)
        c, tick_list, last_tick_update = prepare_candles(tick_list, last_tick_update, next_update, last_candle,
                                                         tick_interval)
        candles.extend(c)
    data = candles_to_array(candles)
    np.save('test_data.npy', data)


def check_dict(d: dict) -> Tuple[dict, list]:
    """
    Checks a optimization configuration dictionary or sub dictionary and calls appropriate function.
    :param d: Dictionary to check.
    :return: A dictionary with tune types in the same structure as the original dictionary and a list of keys.
    """
    new_d = {}
    keys = []
    for key, value in d.items():
        keys.append(key)
        if type(value) == OrderedDict or type(value) == dict:
            d, k = check_dict(value)
            new_d[key] = d
            keys.extend([key + '_' + x for x in k])
        elif type(value) == list:
            l, k = check_list(value)
            new_d[key] = l
            keys.extend([key + '_' + x for x in k])
        else:
            print_(["Something wrong in checking dictionary"])
    return new_d, keys


def check_list(l: list) -> Tuple[Union[float, int, list, Float, Integer], list]:
    """
    Checks a optimization configuration list or sub list and calls appropriate function or creates variable.
    :param l: List to check.
    :return: A list, integer, float, tune float, or tune integer.
    """
    new_l = []
    keys = []
    if type(l[0]) == float:
        if l[0] == l[1]:
            return l[0], keys
        else:
            return uniform(l[0], l[1]), keys
    elif type(l[0]) == int:
        if l[0] == l[1]:
            return l[0], keys
        else:
            return randint(l[0], l[1] + 1), keys
    elif type(l[0]) == bool:
        if l[0] == l[1]:
            return l[0], keys
        else:
            return choice([l[0], l[1]]), keys
    else:
        for item in l:
            if type(item) == list:
                new_l.append(check_list(item))
            elif type(item) == OrderedDict or type(item) == dict:
                d, k = check_dict(item)
                new_l.append(d)
                keys.extend(k)
            else:
                print_(["Something wrong in checking list"])
    return new_l, keys


def load_dict(start: dict, tune: dict) -> dict:
    new_d = {}
    for key, value in tune.items():
        # print(key, value)
        if key in start:
            if type(value) == OrderedDict or type(value) == dict:
                new_d[key] = load_dict(start[key], value)
            elif type(value) == list:
                new_d[key] = load_list(start[key], value)
            elif type(value) == Float or type(value) == Integer or type(value) == float or type(value) == int or type(
                    value) == bool:
                new_d[key] = start[key]
            else:
                print_(["Something wrong in checking dictionary"])
        else:
            print_([key, 'not in starting config', start])
    return new_d


def load_list(start: list, tune: list):
    new_l = []
    for index in range(len(tune)):
        # print(tune[index], start[index])
        if index < len(start):
            if type(tune[index]) == OrderedDict or type(tune[index]) == dict:
                new_l.append(load_dict(start[index], tune[index]))
            elif type(tune[index]) == list:
                new_l.append(load_list(start[index], tune[index]))
            elif type(tune[index]) == Float or type(tune[index]) == Integer or type(tune[index]) == float or type(
                    tune[index]) == int or type(tune[index]) == bool:
                new_l.append(start[index])
        else:
            print_([index, 'out of start config list range', start])
    return new_l


def get_template_live_config() -> dict:
    """
    Creates a base live config that needs to be filled.
    :return: Live config.
    """
    config = {
        "market_type": "futures",
        "leverage": 0,
        "call_interval": 1.0,
        "tick_interval": 0.25,
        "historic_tick_range": 0.0,
        "historic_fill_range": 0.0,
        "strategy_file": "",
        "strategy_class": "",
        "strategy": {}
    }
    return config

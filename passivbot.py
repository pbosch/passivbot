import argparse
import hjson

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='Passivbot',
                                     description='A bot for grid trading that allows backtesting, optimization, and live trading.')
    parser.add_argument('mode', type=str, help='What mode to use.',
                        choices=['download', 'backtest', 'optimize', 'start'])
    parser.add_argument('-l', '--live_config', type=str, required=False, dest='live_config_path', default=None,
                        help='Path to live config to test.')
    parser.add_argument('-b', '--backtest_config', type=str, required=False, dest='backtest_config_path',
                        default='configs/backtest/default.hjson', help='Backtest config hjson file.')
    parser.add_argument('-o', '--optimize_config', type=str, required=False, dest='optimize_config_path',
                        default='configs/optimize/default.hjson', help='Optimize config hjson file.')
    parser.add_argument('--nojit', help='disable numba', action='store_true')
    parser.add_argument('-d', '--download-only', help='Download data only, do not dump ticks caches.',
                        action='store_true')
    parser.add_argument('-t', '--start', type=str, required=False, dest='starting_configs', default=None,
                        help='Start with given live configs. Single json file or dir with multiple json files.')
    parser.add_argument('-s', '--symbol', type=str, required=False, dest='symbol',
                        default=None, help='Specify symbol, overriding symbol from backtest config.')
    parser.add_argument('-u', '--user', type=str, required=False, dest='user', default=None,
                        help='Specify user, a.k.a. account_name, overriding user from backtest config.')
    parser.add_argument('--start-date', type=str, required=False, dest='start_date', default=None,
                        help='Specify start date, overriding value from backtest config.')
    parser.add_argument('--end-date', type=str, required=False, dest='end_date', default=None,
                        help='Specify end date, overriding value from backtest config.')
    args = parser.parse_args()

    if args.mode == 'download':
        from passivbot.downloader import Downloader

    elif args.mode == 'backtest':
        pass
    elif args.mode == 'optimize':
        pass
    elif args.mode == 'start':
        pass

import sys
from typing import Union, Tuple, List

import numpy as np
import pandas as pd
import psutil
import ray
from ray import tune
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.suggest import ConcurrencyLimiter

from bots.configs import BacktestConfig
from helpers.analyzers import analyze_fills
from helpers.converters import fills_to_frame, statistics_to_frame, orders_to_frame
from helpers.misc import check_dict, load_dict
from helpers.print_functions import print_
from helpers.reporter import LogReporter


class Optimizer:
    """
    The optimizer class that holds all methods and functions to execute an optimization run.
    """

    def __init__(self, config: dict, data: np.ndarray, strategy_module, bot_module,
                 current_best: Union[dict, list] = None):
        self.config = config
        self.data = data
        self.strategy_module = strategy_module
        self.bot_module = bot_module
        self.current_best = current_best

    def check_memory(self) -> int:
        """
        Performs a memory check if the available memory would drop below 10%.
        :return: Whether it would drop below 10% or not.
        """
        memory = int(sys.getsizeof(self.data) * 1.2)
        virtual_memory = psutil.virtual_memory()
        print_([f'Data size in mb {memory / (1000 * 1000):.4f}'])
        if (virtual_memory.available - memory) / virtual_memory.total < 0.1:
            print_(["Available memory would drop below 10%. Please reduce the time span."])
            return 0
        else:
            return memory

    def create_config(self, config: dict) -> Tuple[dict, list]:
        """
        Creates the tune config based on a given config. Iterates through nested structures and creates correct sample
        variables.
        :param config: The config to transform.
        :return: The transformed and tunable config and a list of keys to use as parameters.
        """
        strategy_config, keys = check_dict(config)
        filtered_keys = []
        for k in keys:
            partial_exist = False
            for e in keys:
                if k in e and not e == k:
                    partial_exist = True
                    break
            if not partial_exist:
                filtered_keys.append(k)
        tune_config = {'strategy': strategy_config}
        tune_config['backtest_parameters']['quantity_step'] = config[
            'quantity_step'] if 'quantity_step' in config else 0.0
        tune_config['backtest_parameters']['price_step'] = config['price_step'] if 'price_step' in config else 0.0
        tune_config['backtest_parameters']['minimal_quantity'] = config[
            'minimal_quantity'] if 'minimal_quantity' in config else 0.0
        tune_config['backtest_parameters']['minimal_cost'] = config['minimal_cost'] if 'minimal_cost' in config else 0.0
        tune_config['backtest_parameters']['call_interval'] = config[
            'call_interval'] if 'call_interval' in config else 1.0
        tune_config['backtest_parameters']['historic_tick_range'] = config[
            'historic_tick_range'] if 'historic_tick_range' in config else 0.0
        tune_config['backtest_parameters']['historic_fill_range'] = config[
            'historic_fill_range'] if 'historic_fill_range' in config else 0.0
        tune_config['backtest_parameters']['tick_interval'] = config[
            'tick_interval'] if 'tick_interval' in config else 0.25
        tune_config['backtest_parameters']['statistic_interval'] = config[
            'statistic_interval'] if 'statistic_interval' in config else 3600
        tune_config['backtest_parameters']['leverage'] = config['leverage'] if 'leverage' in config else 1.0
        tune_config['backtest_parameters']['symbol'] = config['symbol'] if 'symbol' in config else ''
        tune_config['backtest_parameters']['maker_fee'] = config['maker_fee'] if 'maker_fee' in config else 0.0
        tune_config['backtest_parameters']['latency_simulation_ms'] = config[
            'taker_fee'] if 'taker_fee' in config else 0.0
        tune_config['backtest_parameters']['latency_simulation_ms'] = config[
            'latency_simulation_ms'] if 'latency_simulation_ms' in config else 100
        tune_config['backtest_parameters']['market_type'] = config[
            'market_type'] if 'market_type' in config else 'futures'
        tune_config['backtest_parameters']['inverse'] = config['inverse'] if 'inverse' in config else False
        tune_config['backtest_parameters']['contract_multiplier'] = config[
            'contract_multiplier'] if 'contract_multiplier' in config else 1.0
        tune_config['backtest_parameters']['strategy_class'] = config['strategy_class']
        tune_config['backtest_parameters']['number_of_days'] = config['number_of_days']
        tune_config['backtest_parameters']['starting_balance'] = config['starting_balance']
        if 'sliding_window_days' in config:
            tune_config['backtest_parameters']['sliding_window_days'] = config['sliding_window_days']
        if 'periodic_gain_n_days' in config:
            tune_config['backtest_parameters']['periodic_gain_n_days'] = config['periodic_gain_n_days']
        return tune_config, filtered_keys

    def clean_start_config(self, config_to_clean: dict, optimizer_config: dict):
        """
        Extracts arguments only available in the optimizer config from a start candidate.
        :param config_to_clean: The start candidate to extract arguments from.
        :param optimizer_config: The optimizer config to take as a template.
        :return: The cleaned start configuration.
        """
        return load_dict(config_to_clean['strategy'], optimizer_config)

    def prepare_starting_candidates(self, tune_config: dict) -> list:
        """
        Prepares a list of current best configurations. Cleans the configuration to make sure it works.
        :return: The list of starting candidates.
        """
        starting_candidates = []
        if self.current_best is not None:
            if type(self.current_best) == list:
                for c in self.current_best:
                    c = self.clean_start_config(c, tune_config)
                    if c not in starting_candidates:
                        starting_candidates.append(c)
            else:
                current_best = self.clean_start_config(self.current_best, tune_config)
                starting_candidates.append(current_best)
        return starting_candidates

    def prepare_searcher(self, starting_candidates: list):
        """
        Imports and creates the searcher, either Hyperopt or Nevergrad and initializes them.
        :param starting_candidates: The list of starting candidates.
        :return: The initialized searcher.
        """
        if self.config['algorithm'] == 'hyperopt':
            initial_points = self.config['initial_points'] if 'initial_points' in self.config else 25
            from ray.tune.suggest.hyperopt import HyperOptSearch
            algo = HyperOptSearch(points_to_evaluate=starting_candidates, n_initial_points=initial_points)
        elif self.config['algorithm'] == 'nevergrad':
            number_particles = self.config['number_particles'] if 'number_particles' in self.config else 10
            phi1 = 1.4962
            phi2 = 1.4962
            omega = 0.7298
            if 'options' in self.config:
                phi1 = self.config['options']['c1']
                phi2 = self.config['options']['c2']
                omega = self.config['options']['w']
            import nevergrad as ng
            from ray.tune.suggest.nevergrad import NevergradSearch
            algo = ng.optimizers.ConfiguredPSO(transform='identity', popsize=number_particles, omega=omega, phip=phi1,
                                               phig=phi2)
            algo = NevergradSearch(optimizer=algo, points_to_evaluate=starting_candidates)
        else:
            print_(["Only hyperopt and nevergrad are supported. Exiting."])
            return None
        return algo

    def prepare_slices(self, data, sliding_window_days: float):
        sliding_window_ms = sliding_window_days * 24 * 60 * 60 * 1000
        span_ms = data[-1][0] - data[0][0]
        sample_size_ms = data[1][0] - data[0][0]
        samples_per_window = sliding_window_ms / sample_size_ms
        n_windows = int(np.round(span_ms / sliding_window_ms)) + 1
        for x in np.linspace(0, len(data) - samples_per_window, n_windows):
            start_i = max(0, int(x))
            end_i = start_i + samples_per_window + 1
            yield data[start_i:end_i]

    def single_sliding_window_run(self, config: dict, data: np.ndarray, do_print=True):
        n_days = config['n_days']

        if config['sliding_window_days'] == 0.0:
            sliding_window_days = n_days
        else:
            sliding_window_days = min(n_days, max([config['backtest_parameters']['periodic_gain_n_days'] * 1.1,
                                                   config['sliding_window_days']]))

        for z, data_slice in enumerate(self.prepare_slices(data, sliding_window_days)):
            if len(data_slice[0]) == 0:
                print('debug b no data')
                continue
            try:
                bot = self.prepare_backtest(config, data_slice)
                bot.init()
                bot.update_balance(config['backtest_parameters']['starting_balance'])
                fills, statistics, accepted_orders, finished = bot.start_websocket()
                fill_frame = fills_to_frame(fills)
                statistic_frame = statistics_to_frame(statistics)
                accepted_order_frame = orders_to_frame(accepted_orders)
            except Exception as e:
                print(e)
                break
            try:
                do_break = self.strategy_module.break_function(fill_frame, statistic_frame, accepted_order_frame)
            except:
                result = analyze_fills(fill_frame, statistic_frame, config, data_slice[0][0], data_slice[-1][0])
                do_break = False

                analysis['score'] = objective_function(analysis, config, metric=metric) * (
                        analysis['n_days'] / config['n_days'])
                analyses.append(analysis)
                objective = np.mean([e['score'] for e in analyses]) * max(1.01, config['reward_multiplier_base']) ** (z + 1)
                analyses[-1]['objective'] = objective
                line = (f'{str(z).rjust(3, " ")} adg {analysis["average_daily_gain"]:.4f}, '
                        f'bkr {analysis["closest_bkr"]:.4f}, '
                        f'eqbal {analysis["lowest_eqbal_ratio"]:.4f} n_days {analysis["n_days"]:.1f}, '
                        f'shrp {analysis["sharpe_ratio"]:.4f} , '
                        f'{config["avg_periodic_gain_key"]} {analysis["average_periodic_gain"]:.4f}, '
                        f'score {analysis["score"]:.4f}, objective {objective:.4f}, '
                        f'hrs stuck ss {str(round(analysis["max_hrs_no_fills_same_side"], 1)).zfill(4)}, ')
                do_break = False
                if (bef := config['break_early_factor']) != 0.0:
                    if analysis['closest_bkr'] < config['minimum_bankruptcy_distance'] * (1 - bef):
                        line += f"broke on min_bkr {analysis['closest_bkr']:.4f}, {config['minimum_bankruptcy_distance']} "
                        do_break = True
                    if analysis['lowest_eqbal_ratio'] < config['minimum_equity_balance_ratio'] * (1 - bef):
                        line += f"broke on min_eqbal_r {analysis['lowest_eqbal_ratio']:.4f} "
                        do_break = True
                    if analysis['sharpe_ratio'] < config['minimum_sharpe_ratio'] * (1 - bef):
                        line += f"broke on shrp_r {analysis['sharpe_ratio']:.4f} {config['minimum_sharpe_ratio']} "
                        do_break = True
                    if analysis['max_hrs_no_fills'] > config['maximum_hrs_no_fills'] * (1 + bef):
                        line += f"broke on max_h_n_fls {analysis['max_hrs_no_fills']:.4f}, {config['maximum_hrs_no_fills']} "
                        do_break = True
                    if analysis['max_hrs_no_fills_same_side'] > config['maximum_hrs_no_fills_same_side'] * (1 + bef):
                        line += f"broke on max_h_n_fls_ss {analysis['max_hrs_no_fills_same_side']:.4f}, {config['maximum_hrs_no_fills_same_side']} "
                        do_break = True
                    if analysis['mean_hrs_between_fills'] > config['maximum_mean_hrs_between_fills'] * (1 + bef):
                        line += f"broke on mean_h_b_fls {analysis['mean_hrs_between_fills']:.4f}, {config['maximum_mean_hrs_between_fills']} "
                        do_break = True
                    if analysis['average_daily_gain'] < config['minimum_slice_adg'] * (1 - bef):
                        line += f"broke on low adg {analysis['average_daily_gain']:.4f} "
                        do_break = True
                    if z > 2 and (mean_adg := np.mean([e['average_daily_gain'] for e in analyses])) < 1.0:
                        line += f"broke on low mean adg {mean_adg:.4f} "
                        do_break = True
                if do_print:
                    print(line)
            if do_break:
                break
        return objective, analyses

    def prepare_backtest(self, config: dict, data: np.ndarray):
        strategy_config = self.strategy_module.convert_dict_to_config(config['strategy'])
        # Create a backtest config
        b_config = BacktestConfig(config['backtest_parameters']['quantity_step'],
                                  config['backtest_parameters']['price_step'],
                                  config['backtest_parameters']['minimal_quantity'],
                                  config['backtest_parameters']['minimal_cost'],
                                  config['backtest_parameters']['call_interval'],
                                  config['backtest_parameters']['historic_tick_range'],
                                  config['backtest_parameters']['historic_fill_range'],
                                  config['backtest_parameters']['tick_interval'],
                                  config['backtest_parameters']['statistic_interval'],
                                  config['backtest_parameters']['leverage'],
                                  config['backtest_parameters']['symbol'],
                                  config['backtest_parameters']['maker_fee'],
                                  config['backtest_parameters']['taker_fee'],
                                  config['backtest_parameters']['latency_simulation_ms'],
                                  config['backtest_parameters']['market_type'],
                                  config['backtest_parameters']['inverse'],
                                  config['backtest_parameters']['contract_multiplier'])

        strategy = getattr(self.strategy_module, config['backtest_parameters']['strategy_class'])(strategy_config)
        bot = self.bot_module.BacktestBot(b_config, strategy, data)
        return bot

    def optimize_wrap(self, config: dict, data: np.ndarray):
        bot = self.prepare_backtest(config, data)
        bot.init()
        bot.update_balance(config['backtest_parameters']['starting_balance'])
        fills, statistics, accepted_orders, finished = bot.start_websocket()
        fill_frame = fills_to_frame(fills)
        statistic_frame = statistics_to_frame(statistics)
        accepted_order_frame = orders_to_frame(accepted_orders)
        self.report([fill_frame], [statistic_frame], [accepted_order_frame], [finished])

    def report(self, fill_frames: List[pd.DataFrame], statistic_frames: List[pd.DataFrame],
               accepted_order_frames: List[pd.DataFrame], finished: List[bool]):
        try:
            # Return a dict of parameters to report, has to include obj
            report_dict = self.strategy_module.report_function(fill_frames, statistic_frames, accepted_order_frames,
                                                               finished)
            if 'obj' in report_dict:
                tune.report(**report_dict)
        except:
            # Use base report function
            tune.report(obj=0.0)

    def run(self):
        if memory := self.check_memory() == 0:
            return None

        tune_config, parameters = self.create_config(self.config["hyperparameters"])

        print('Tuning:')
        print(tune_config)

        if 'iterations' in self.config:
            iterations = self.config['iterations']
        else:
            print_(['Parameter iters should be defined in the configuration. Defaulting to 10.'])
            iterations = 10
        if 'number_cores' in self.config:
            number_cores = self.config['number_cores']
        else:
            print_(['Parameter number_cores should be defined in the configuration. Defaulting to 2.'])
            number_cores = 2

        starting_candidates = self.prepare_starting_candidates(tune_config)
        algo = self.prepare_searcher(starting_candidates)
        if not algo:
            return None

        algo = ConcurrencyLimiter(algo, max_concurrent=number_cores)
        scheduler = AsyncHyperBandScheduler()

        ray.init(num_cpus=number_cores,
                 object_store_memory=memory if memory > 4000000000 else None)  # , logging_level=logging.FATAL, log_to_driver=False)

        backtest_wrap = tune.with_parameters(simple_sliding_window_wrap, data=self.data)
        analysis = tune.run(
            backtest_wrap, metric='obj', mode='max', name='search',
            search_alg=algo, scheduler=scheduler, num_samples=iterations, config=tune_config, verbose=1,
            reuse_actors=True, local_dir=self.config['optimize_dirpath'],
            progress_reporter=LogReporter(
                metric_columns=['min_adg',
                                'avg_adg',
                                'min_bkr',
                                'min_eqbal_r',
                                'min_shrp_r',
                                'avg_shrp_r',
                                config['avg_periodic_gain_key'],
                                'max_h_n_fls',
                                'max_h_n_fls_ss',
                                'max_mean_h_b_fills',
                                'avg_mean_h_b_fills',
                                'n_slc',
                                'obj'],
                parameter_columns=parameters),
            raise_on_failed_trial=False
        )
        ray.shutdown()
        return analysis

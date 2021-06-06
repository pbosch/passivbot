import asyncio
import json
import logging
import os
import signal

import telegram_bot

from bots.base import Bot
from helpers.helpers import add_argparse_args, get_passivbot_argparser, create_binance_bot, create_bybit_bot

logging.getLogger("telegram").setLevel(logging.CRITICAL)


async def start_bot(bot):
    while not bot.stop_websocket:
        try:
            await bot.acquire_interprocess_lock()
            await bot.start_websocket()
        except Exception as e:
            print(
                'Websocket connection has been lost or unable to acquire lock to start, attempting to reinitialize the bot...',
                e)
            await asyncio.sleep(10)


async def _start_telegram(account: dict, bot: Bot):
    telegram = telegram_bot.Telegram(config=account['telegram'],
                                     bot=bot,
                                     loop=asyncio.get_event_loop())
    telegram.log_start()
    return telegram


async def main() -> None:
    args = add_argparse_args(get_passivbot_argparser()).parse_args()
    try:
        accounts = json.load(open('api-keys.json'))
    except Exception as e:
        print(e, 'failed to load api-keys.json file')
        return
    try:
        account = accounts[args.user]
    except Exception as e:
        print('unrecognized account name', args.user, e)
        return
    try:
        config = json.load(open(args.live_config_path))
    except Exception as e:
        print(e, 'failed to load config', args.live_config_path)
        return
    config['user'] = args.user
    config['exchange'] = account['exchange']
    config['symbol'] = args.symbol
    config['live_config_path'] = args.live_config_path

    if account['exchange'] == 'binance':
        bot = await create_binance_bot(config)
    elif account['exchange'] == 'bybit':
        bot = await create_bybit_bot(config)
    else:
        raise Exception('unknown exchange', account['exchange'])
    print('using config')
    print(json.dumps(config, indent=4))

    if 'telegram' in account and account['telegram']['enabled']:
        telegram = await _start_telegram(account=account, bot=bot)
        bot.telegram = telegram
    signal.signal(signal.SIGINT, bot.stop)
    signal.signal(signal.SIGTERM, bot.stop)
    await start_bot(bot)
    await bot.session.close()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception as e:
        print(f'\nThere was an error starting the bot: {e}')
    finally:
        print('\nPassivbot was stopped succesfully')
        os._exit(0)

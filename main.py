import threading
import time

from modules.dbhelper import DBHelper
from modules.telegramhelper import TelegramHelper

from threads.telegram_handler import TelegramHandler
from threads.event_handler import EventHandler
from threads.update_subscribers_on_pooltool import UpdateSubscribers

from modules import common as c
from modules import pooltool_dbhelper


def main():
    db = DBHelper()
    db.setup()

    #ptdb = pooltool_dbhelper.PoolToolDb()
    #reward_addr_json = json.loads(subprocess.check_output(f'echo addr1qx327m9urmgl9fygek5qzdmd9xnxc393ym7fe7t30mr36h3gctxml88r3zt6x3epv3eyylkpv6dz52tp50dere4lnu4spysu54 | ~/wallet/cardano-wallet-shelley-linux64/cardano-address address inspect', shell=True).decode('utf-8'))
    #stake_key_hash = reward_addr_json['stake_key_hash']
    #total_stake = ptdb.get_total_stake('4f0ddf015b0fe42ec43deb55151aa408f423fd8514dfc93e951d3e84')
    #total_stake_rewards = ptdb.get_total_stake_rewards('4f0ddf015b0fe42ec43deb55151aa408f423fd8514dfc93e951d3e84')
    #print( (total_stake/total_stake_rewards) * 100)
    #print(ptdb.get_total_stake_rewards('4f0ddf015b0fe42ec43deb55151aa408f423fd8514dfc93e951d3e84'))
    # c.clean_up_pools_table(db)
    # c.handle_wallet_newpool(db)

    tg = TelegramHelper()

    tg_handler = TelegramHandler(db, tg)
    ev_handler = EventHandler(db, tg)
    sub_updater = UpdateSubscribers(db)

    telegram_handler = threading.Thread(target=tg_handler.run)
    event_handler = threading.Thread(target=ev_handler.run)
    subscribe_updater = threading.Thread(target=sub_updater.run)

    telegram_handler.start()
    event_handler.start()
    subscribe_updater.start()

    periodic_check_timer = c.get_current_time_millis()
    while True:
        if not telegram_handler.is_alive():
            telegram_handler = threading.Thread(target=tg_handler.run)
            telegram_handler.start()

        if not event_handler.is_alive():
            event_handler = threading.Thread(target=ev_handler.run)
            event_handler.start()

        if c.get_current_time_millis() - periodic_check_timer > 3600 * 1000:
            if not subscribe_updater.is_alive():
                subscribe_updater = threading.Thread(target=sub_updater.run)
                subscribe_updater.start()
                periodic_check_timer = c.get_current_time_millis()

        time.sleep(5 * 60)


if __name__ == '__main__':
    main()
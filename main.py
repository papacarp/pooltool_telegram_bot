import threading
import time

from modules.dbhelper import DBHelper
from modules.telegramhelper import TelegramHelper

from threads.telegram_handler import TelegramHandler
from threads.event_handler import EventHandler
from threads.update_subscribers_on_pooltool import UpdateSubscribers

from modules import common as c


def main():
    db = DBHelper()
    db.setup()

    c.handle_wallet_newpool(db)

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
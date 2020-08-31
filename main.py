import threading
import time

from modules.dbhelper import DBHelper
from modules.telegramhelper import TelegramHelper

from threads.telegram_handler import TelegramHandler
from threads.event_handler import EventHandler


def main():
    db = DBHelper()
    db.setup()

    tg = TelegramHelper()

    tg_handler = TelegramHandler(db, tg)
    ev_handler = EventHandler(db, tg)

    telegram_handler = threading.Thread(target=tg_handler.run)
    # event_handler = threading.Thread(target=ev_handler.run)

    telegram_handler.start()
    # event_handler.start()

    while True:
        if not telegram_handler.is_alive():
            telegram_handler = threading.Thread(target=tg_handler.run)
            telegram_handler.start()
        # if not event_handler.is_alive():
        #     event_handler = threading.Thread(target=ev_handler.run)
        #     event_handler.start()
        time.sleep(5 * 60)


if __name__ == '__main__':
    main()
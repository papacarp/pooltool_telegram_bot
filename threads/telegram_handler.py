import time
import json

from modules.emoji import Emoji as e
from modules import common as c


class TelegramHandler:
    def __init__(self, db, tg):
        self.db = db
        self.tg = tg

        self.options_string_builder = {}
        self.options_old = ['See options', 'Block minted', 'Battle', 'Sync status', 'Block adjustment', 'Stake change',
                            'Epoch summary',
                            'Slot loaded', 'Stake change threshold', 'Back']
        self.options = ['See options', 'Block minted', 'Pool change', 'Stake change', 'Stake change threshold', 'Award', 'Back']
        self.states = ['Enable', 'Disable', 'Silence']

    def handle_start(self, chat):
        message = f"{e.globe}Welcome to PoolTool Bot!{e.globe}\n" \
                  "\n" \
                  "Please enter the TICKER of the pool(s) you want to follow\n" \
                  "\n" \
                  "Example: KUNO\n" \
                  "\n" \
                  "In order to remove a TICKER from the list, you have two options:\n" \
                  "1. Enter the TICKER again\n" \
                  "2. Enter \"/DELETE\" to get a list with possible TICKERs to delete\n" \
                  "\n" \
                  "For more information, enter \"/HELP\"\n" \
                  "\n" \
                  "This pooltool bot was created for pooltool by Kuno Heltborg \[ETR] (Erik The Red)\n" \
                  "\n" \
                  "*NOTE: This Bot is not case sensitive! text in upper- and lower case work!*"
        self.tg.send_message(message, chat)

    def handle_help(self, chat):
        message = "*Add/Remove pool:*\n" \
                  "Enter ticker of the pool, this will both add the pool to the list or delete if it is already on the list\n" \
                  "\n" \
                  "Example: KUNO\n" \
                  "\n" \
                  "\n" \
                  "*Options for each pool:*\n" \
                  "You can enable/disable/silent each specific notification you want for each pool on your list\n" \
                  "\n" \
                  "Enter: /OPTION\n" \
                  "\n" \
                  "*NOTE: This Bot is not case sensitive! text in upper- and lower case work!*"
        self.tg.send_message(message, chat)

    def send_option_type(self, chat):
        keyboard = self.tg.build_keyboard(self.options)
        self.tg.send_message('Select option to change', chat, keyboard)

    def handle_option_start(self, chat, tickers):
        self.options_string_builder[chat] = {}
        self.options_string_builder[chat]['string'] = ' '
        self.options_string_builder[chat]['next'] = 'option_pool'
        tickers.append('QUIT')
        keyboard = self.tg.build_keyboard(tickers)
        self.tg.send_message("Select pool", chat, keyboard)

    def validate_option_type(self, type):
        for option in self.options:
            if type == option.upper():
                return True
        return False

    def validate_option_state(self, type):
        if type in [x.upper() for x in self.states]:
            return True
        return False

    def validate_option_threshold(self, type):
        if type == '0':
            return True
        elif type in [c.set_prefix(x).upper() for x in [1000, 10000, 100000, 1000000, 10000000]]:
            return True
        return False

    def convert_option_value(self, value):
        if value == 1:
            return 'On'
        elif value == 2:
            return 'Silent'
        else:
            return 'Off'

    def get_current_options(self, chat, text):
        if len(text):
            options_string = f'\\[ {text[0]} ] Options:\n' \
                             f'\n' \
                             f"Block minted: {self.convert_option_value(self.db.get_option(chat, text[0], 'block_minted'))}\n" \
                             f"Pool change: {self.convert_option_value(self.db.get_option(chat, text[0], 'pool_change'))}\n" \
                             f"Award: {self.convert_option_value(self.db.get_option(chat, text[0], 'award'))}\n" \
                             f"Stake change: {self.convert_option_value(self.db.get_option(chat, text[0], 'stake_change'))}\n" \
                             f"Stake change threshold: {c.set_prefix(self.db.get_option(chat, text[0], 'stake_change_threshold'))}"
            return options_string
        return ''

    def update_option(self, chat, text, new_value):
        self.db.update_option(chat, text[0], text[1], new_value)

    def adjust_string_if_duplicate(self, text):
        list = text.split(' ')
        if len(list) > 1:
            if list[1].isdigit():  # Assuming we work with a duplicate ticker
                new_list = []
                if len(list) == 2:
                    new_list.extend([' '.join([list[0], list[1]])])
                elif len(list) == 3:
                    new_list.extend([' '.join([list[0], list[1]]), list[2]])
                return new_list
        return list

    def go_back_to_option_type(self, chat):
        self.send_option_type(chat)
        tmp_list = self.adjust_string_if_duplicate(self.options_string_builder[chat]['string'])
        self.options_string_builder[chat]['string'] = tmp_list[0]
        self.options_string_builder[chat]['next'] = 'option_type'

    def send_option_stake_threshold(self, chat):
        thresholds = [c.set_prefix(x) for x in [1000, 10000, 100000, 1000000, 10000000]]
        thresholds.insert(0, '0')
        keyboard = self.tg.build_keyboard(thresholds)
        self.tg.send_message('Select new threshold', chat, keyboard)

    def convert_values_from_prefix(self, value):
        if value[len(value) - 1] == 'K':
            return int(value.split('.')[0]) * 1000
        elif value[len(value) - 1] == 'M':
            return int(value.split('.')[0]) * 1000000
        else:
            return 0

    def get_pool_id_from_ticker_file(self, ticker):
        with open(c.ticker_reverse_file_path, 'r') as ticker_file:
            tickers = json.load(ticker_file)
        return tickers.get(ticker)

    def on_ticker_valid(self, ticker, number, chat, pool_id):
        self.db.add_new_pool(pool_id[number], ticker)
        try:
            self.db.add_new_user_pool(chat, pool_id[number], ticker)
        except Exception as e:
            print(f"Something went wrong trying to add ticker {ticker}, {number}, {pool_id}")
        tickers = self.db.get_tickers_from_chat_id(chat)
        message = "List of pools you watch:\n\n" + "\n".join(tickers)
        self.tg.send_message(message, chat)

    def handle_duplicate_ticker(self, text, chat, pool_id):
        if len(text) > 1:
            try:
                number = int(text[1])
                if len(pool_id) > number >= 0:
                    text = f'{text[0]} {text[1]}'
                    self.on_ticker_valid(text, number, chat, pool_id)
                else:
                    raise Exception("Please enter a number that fit the provided listing!")
            except Exception as e:
                message = "Please enter a valid ticker"
                self.tg.send_message(message, chat)
                return
        else:
            count = 0
            pool_ids = ''
            for pool in pool_id:
                pool_ids = pool_ids + f'{count}. {pool[:5]}...{pool[len(pool) - 5:]}\n'
                count += 1
            message = "There's more than one pool with this ticker!\n" \
                      "\n" \
                      f"{pool_ids}\n" \
                      f"Please specify which pool you want listed, eg.\n" \
                      f"{text[0]} x, where x is the listing number"
            self.tg.send_message(message, chat)

    def handle_new_ticker(self, text, chat):
        text = text.split(' ')
        pool_id = self.get_pool_id_from_ticker_file(text[0])

        if pool_id is None:
            c.handle_wallet_newpool(None)
            pool_id = self.get_pool_id_from_ticker_file(text[0])
            if pool_id is None:
                message = "This is not a valid TICKER!"
                self.tg.send_message(message, chat)
                return
        if len(pool_id) > 1:
            self.handle_duplicate_ticker(text, chat, pool_id)
            return

        self.on_ticker_valid(text[0], 0, chat, pool_id)

    def send_option_state(self, chat):
        keyboard = self.tg.build_keyboard(self.states)
        self.tg.send_message('Select new state', chat, keyboard)

    def handle_next_option_step(self, chat, text, tickers):
        next_step = self.options_string_builder[chat]['next']
        if next_step == 'option_pool':
            if text in tickers:
                self.send_option_type(chat)
                self.options_string_builder[chat]['string'] = text
                self.options_string_builder[chat]['next'] = 'option_type'
            elif text == 'QUIT':
                del self.options_string_builder[chat]
                self.tg.send_message("Options has been saved!", chat, self.tg.remove_keyboard(True))
                return
            else:
                message = "Not a valid pool, try again!"
                self.tg.send_message(message, chat)
                self.handle_option_start(chat, tickers)
        elif next_step == 'option_type':
            if self.validate_option_type(text):
                if text == 'SEE OPTIONS':
                    message = self.get_current_options(chat,
                                                       self.adjust_string_if_duplicate(
                                                           self.options_string_builder[chat]['string']))
                    if not message == '':
                        self.tg.send_message(message, chat)
                    self.go_back_to_option_type(chat)
                    return
                elif text == 'BACK':
                    self.handle_option_start(chat, tickers)
                    return
                elif text == 'Stake Change Threshold'.upper():
                    self.send_option_stake_threshold(chat)
                    self.options_string_builder[chat]['next'] = 'option_threshold'
                else:
                    self.send_option_state(chat)
                    self.options_string_builder[chat]['next'] = 'option_state'
                self.options_string_builder[chat]['string'] = ' '.join(
                    [self.options_string_builder[chat]['string'], text.replace(' ', '_')])
            else:
                message = "Not a possible option type, try again!"
                self.tg.send_message(message, chat)
                self.send_option_type(chat)
        elif next_step == 'option_state':
            if self.validate_option_state(text):
                if text == 'ENABLE':
                    # options_string_builder[chat]['string'] = ' '.join([options_string_builder[chat]['string'], '1'])
                    self.update_option(chat,
                                       self.adjust_string_if_duplicate(self.options_string_builder[chat]['string']), 1)
                elif text == 'DISABLE':
                    # options_string_builder[chat]['string'] = ' '.join([options_string_builder[chat]['string'], '0'])
                    self.update_option(chat,
                                       self.adjust_string_if_duplicate(self.options_string_builder[chat]['string']), 0)
                elif text == 'SILENCE':
                    # options_string_builder[chat]['string'] = ' '.join([options_string_builder[chat]['string'], '2'])
                    self.update_option(chat,
                                       self.adjust_string_if_duplicate(self.options_string_builder[chat]['string']), 2)
                message = self.get_current_options(chat,
                                                   self.adjust_string_if_duplicate(
                                                       self.options_string_builder[chat]['string']))
                self.tg.send_message(message, chat)
                self.go_back_to_option_type(chat)
                # del options_string_builder[chat]
            else:
                message = "Not a possible option state, try again!"
                self.tg.send_message(message, chat)
                self.send_option_state(chat)
        elif next_step == 'option_threshold':
            if self.validate_option_threshold(text):
                self.update_option(chat, self.adjust_string_if_duplicate(self.options_string_builder[chat]['string']),
                                   self.convert_values_from_prefix(text))
                message = self.get_current_options(chat,
                                                   self.adjust_string_if_duplicate(
                                                       self.options_string_builder[chat]['string']))
                self.tg.send_message(message, chat)
                self.go_back_to_option_type(chat)
                # del options_string_builder[chat]
            else:
                message = "Not a possible option for threshold, try again!"
                self.tg.send_message(message, chat)
                self.send_option_stake_threshold(chat)

    def handle_updates(self, updates):
        if 'result' in updates:
            for update in updates["result"]:
                if 'message' in update:
                    print(update)
                    if 'text' not in update["message"]:
                        continue
                    text = update["message"]["text"].upper()
                    chat = update["message"]["chat"]["id"]

                    tickers = self.db.get_tickers_from_chat_id(chat)
                    if chat in self.options_string_builder:
                        self.handle_next_option_step(chat, text, tickers)
                        continue
                    if text == "/DELETE":
                        if not tickers:
                            self.tg.send_message("No TICKERs added", chat)
                            continue
                        keyboard = self.tg.build_keyboard(tickers)
                        self.tg.send_message("Select pool to delete", chat, keyboard)
                    elif text == "/START":
                        self.handle_start(chat)
                        if 'username' in update["message"]["from"]:
                            name = update["message"]["from"]["username"]
                            try:
                                self.db.add_user(chat, name)
                            except Exception as e:
                                print('Assuming user is already added')
                        else:
                            try:
                                self.db.add_user(chat, None)
                            except Exception as e:
                                print('Assuming user is already added')
                    elif text == "/HELP":
                        self.handle_help(chat)
                    elif text == "/OPTION":
                        self.handle_option_start(chat, tickers)
                    elif text.startswith("/"):
                        message = "Unknown command, try /help"
                        self.tg.send_message(message, chat)
                        continue
                    elif text in tickers:
                        self.db.delete_user_pool(chat, text)
                        # db.delete_item(chat, text)
                        tickers = self.db.get_tickers_from_chat_id(chat)
                        # tickers = db.get_tickers(chat)
                        message = "List of pools you watch:\n\n" + "\n".join(tickers)
                        self.tg.send_message(message, chat)
                    else:
                        self.handle_new_ticker(text, chat)

    def run(self):
        last_update_id = None
        while True:
            updates = self.tg.get_updates(last_update_id)
            if updates is not None:
                if updates['ok']:
                    if len(updates["result"]) > 0:
                        last_update_id = self.tg.get_last_update_id(updates) + 1
                        self.handle_updates(updates)
            time.sleep(0.5)

import time
import json
import subprocess

from modules import pooltool_dbhelper

from modules.emoji import Emoji as e
from modules import common as c


class TelegramHandler:
    def __init__(self, db, tg):
        self.db = db
        self.tg = tg

        self.options_string_builder = {}
        # self.options_old = ['See options', 'Block minted', 'Battle', 'Sync status', 'Block adjustment', 'Stake change',
        #                     'Epoch summary',
        #                     'Slot loaded', 'Stake change threshold', 'Back']
        self.options = ['See options', 'Block minted', 'Pool change', 'Stake change', 'Stake change threshold', 'Award', 'Epoch summary', 'Block Estimation', 'Back']
        self.states = ['Enable', 'Disable', 'Silence']

    def handle_start(self, chat):
        message = f"{e.globe}Welcome to PoolTool Bot!{e.globe}\n" \
                  "\n" \
                  "Please enter the TICKER of the pool(s) you want to follow\n" \
                  "\n" \
                  "Example: ETR\n" \
                  "\n" \
                  "In order to remove a TICKER from the list, you have two options:\n" \
                  "1. Enter the TICKER again\n" \
                  "2. Enter \"/DELETE\" to get a list with possible TICKERs to delete\n" \
                  "\n" \
                  "*Rewards:*\n" \
                  "Notifies how much reward you got at each epoch transition\n" \
                  "\n" \
                  "Enter: /reward <stake address>\n" \
                  "\n" \
                  "For more information, enter \"/HELP\"\n" \
                  "\n" \
                  "_This pooltool bot was created for pooltool by Kuno Heltborg_ *[ ETR ]* _(Erik The Red)_\n" \
                  "\n" \
                  "*NOTE: This Bot is not case sensitive! text in upper- and lower case work!*"
        self.tg.send_message(message, chat)

    def handle_help(self, chat):
        message = "*Add/Remove pool:*\n" \
                  "Enter ticker of the pool, this will both add the pool to the list or delete if it is already on the list\n" \
                  "\n" \
                  "Example: ETR\n" \
                  "\n" \
                  "Or add a pool using pool id: \n" \
                  "\n" \
                  "/poolid <pool id>" \
                  "\n" \
                  "\n" \
                  "*Options for each pool:*\n" \
                  "You can enable/disable/silent each specific notification you want for each pool on your list\n" \
                  "\n" \
                  "Enter: /option\n" \
                  "\n" \
                  "*Rewards:*\n" \
                  "Notifies how much reward you got at each epoch transition\n" \
                  "\n" \
                  "Enter: /reward <stake address>\n" \
                  "\n" \
                  "\n" \
                  "/clear_keyboard - removes the telegram keyboard if it's stuck" \
                  "\n" \
                  "*NOTE: This Bot is not case sensitive! text in upper- and lower case work!*"
        self.tg.send_message(message, chat)

    def get_list_of_pools(self, tickers, pool_ids):
        message = "List of pools you watch:\n\n"
        ptdb = pooltool_dbhelper.PoolToolDb()
        for i in range(0, len(tickers)):
            pool_name = ptdb.get_pool_name(pool_ids[i])
            if len(pool_name) > 20:
                pool_name = pool_name[:20] + "..."
            message += f"{tickers[i]} - {pool_name}\n"
        return message

    def get_list_of_reward_addr(self, reward_addrs):
        message = "List of addresses you watch:\n\n"
        for addr in reward_addrs:
            message += f"{addr}\n\n"
        return message

    def send_option_type(self, chat):
        keyboard = self.tg.build_keyboard(self.options)
        self.tg.send_message('Select option to change', chat, keyboard)
  
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
        try:
            self.db.update_ticker(pool_id[number], ticker)
        except Exception as e:
            print("Assuming ticker does not exist.. adding to db")
        try:
            self.db.add_new_pool(pool_id[number], ticker)
        except Exception as e:
            print("Assuiming pool is already added")
        try:
            self.db.add_new_user_pool(chat, pool_id[number], ticker)
        except Exception as e:
            print(f"Something went wrong trying to add ticker {ticker}, {number}, {pool_id}")
        tickers, pool_ids = self.db.get_ticker_poolid_from_chat_id(chat)
        message = self.get_list_of_pools(tickers, pool_ids)
        self.tg.send_message(message, chat)

    def handle_duplicate_ticker(self, text, chat, pool_id, type):
        if type == 'adding_duplicate':
            if text == 'QUIT':
                del self.options_string_builder[chat]
                tickers, pool_ids = self.db.get_ticker_poolid_from_chat_id(chat)
                message = self.get_list_of_pools(tickers, pool_ids)
                self.tg.send_message(message, chat, self.tg.remove_keyboard(True))
                return
            elif text.lower() not in self.options_string_builder[chat]['pool_ids']:
                keyboard = self.tg.build_keyboard(self.options_string_builder[chat]['pool_ids'])
                message = f"Please select a pool id listed below {e.pointDown}"
                self.tg.send_message(message, chat, keyboard)
                return
            else:
                self.handle_new_pool_id(text.lower(), chat)
                keyboard = self.tg.build_keyboard(self.options_string_builder[chat]['pool_ids'])
                message = "Do you want to add more?"
                self.tg.send_message(message, chat, keyboard)
        else:
            message = f"There's more than one pool with ticker {text[0]}!\n" \
                      "\n" \
                      f"Please specify which pool you want listed, by using the keyboard {e.pointDown}"
            pool_id.append('QUIT')
            keyboard = self.tg.build_keyboard(pool_id)
            self.tg.send_message(message, chat, keyboard)
            self.options_string_builder[chat] = {}
            self.options_string_builder[chat]['type'] = 'adding_duplicate'
            self.options_string_builder[chat]['pool_ids'] = pool_id

    def handle_new_pool_id(self, pool_id, chat):
        try:
            ticker = self.db.get_ticker_from_pool_id(pool_id)[0]
        except Exception as e:
            message = "Not a valid pool id!:"
            self.tg.send_message(message, chat)
            return
        self.db.add_new_user_pool(chat, pool_id, ticker)
        tickers = self.db.get_tickers_from_chat_id(chat)

    def handle_new_ticker(self, text, chat):
        text = text.split(' ')
        pool_id = self.db.get_pool_id_from_ticker(text[0])

        if not pool_id :
            c.handle_wallet_newpool(self.db)
            pool_id = self.db.get_pool_id_from_ticker(text[0])
            if not pool_id:
                message = "This is not a valid TICKER!"
                self.tg.send_message(message, chat)
                return
        if len(pool_id) > 1:
            self.handle_duplicate_ticker(text, chat, pool_id, None)
            return

        self.on_ticker_valid(text[0], 0, chat, pool_id)

    def handle_new_pool_on_pool_id(self, chat, text):
        text = text.split(' ')
        if len(text) < 2:
            message = "Please add a pool id, usage:\n"
            message += "/poolid <poolid>"
            self.tg.send_message(message, chat)
            return
        pool_id = text[1].lower()
        if not self.db.does_pool_id_exist(pool_id):
            c.handle_wallet_newpool(self.db)
        if self.db.does_pool_id_exist(pool_id):
            ticker = self.db.get_ticker_from_pool_id(pool_id)
            self.handle_new_ticker(ticker[0], chat)

    def send_option_state(self, chat):
        keyboard = self.tg.build_keyboard(self.states)
        self.tg.send_message('Select new state', chat, keyboard)

    def go_back_to_option_type(self, chat): 
        self.send_option_type(chat)
        self.options_string_builder[chat]['next'] = 'option_type'

    def get_current_options(self, chat, pool_id, pool_name):
        if len(pool_id):
            options_string = f'\\[ {pool_name} ] Options:\n' \
                                f'\n' \
                                f"Block minted: {self.convert_option_value(self.db.get_option_value_poolid(chat, pool_id, 'block_minted'))}\n" \
                                f"Pool change: {self.convert_option_value(self.db.get_option_value_poolid(chat, pool_id, 'pool_change'))}\n" \
                                f"Award: {self.convert_option_value(self.db.get_option_value_poolid(chat, pool_id, 'award'))}\n" \
                                f"Stake change: {self.convert_option_value(self.db.get_option_value_poolid(chat, pool_id, 'stake_change'))}\n" \
                                f"Stake change threshold: {c.set_prefix(self.db.get_option_value_poolid(chat, pool_id, 'stake_change_threshold'))}\n" \
                                f"Block Estimation: {self.convert_option_value(self.db.get_option_value_poolid(chat, pool_id, 'block_estimation'))}"
            return options_string
        return ''

    def handle_next_option_step(self, chat, text, tickers, pool_ids):
        next_step = self.options_string_builder[chat]['next']
        if next_step == 'option_pool':
            try:
                pool_id = self.options_string_builder[chat][text]
            except:
                pool_id = ''
            if pool_id in pool_ids:
                ptdb = pooltool_dbhelper.PoolToolDb()
                pool_name = ptdb.get_pool_name(self.options_string_builder[chat][text])
                self.send_option_type(chat)
                self.options_string_builder[chat]['chosen_pool_id'] = pool_id
                self.options_string_builder[chat]['chosen_pool_name'] = pool_name
                self.options_string_builder[chat]['next'] = 'option_type'
            elif text == 'QUIT':
                del self.options_string_builder[chat]
                self.tg.send_message("Options has been saved!", chat, self.tg.remove_keyboard(True))
                return
            else:
                message = "Not a valid pool, try again!"
                self.tg.send_message(message, chat)
                self.handle_option_start(chat, tickers, pool_ids)
        elif next_step == 'option_type':
            if self.validate_option_type(text):
                if text == 'SEE OPTIONS':
                    message = self.get_current_options(chat, self.options_string_builder[chat]['chosen_pool_id'], self.options_string_builder[chat]['chosen_pool_name'])
                    if not message == '':
                        self.tg.send_message(message, chat)
                    self.go_back_to_option_type(chat)
                    return
                elif text == 'BACK':
                    self.handle_option_start(chat, tickers, pool_ids)
                    return
                elif text == 'Stake Change Threshold'.upper():
                    self.send_option_stake_threshold(chat)
                    self.options_string_builder[chat]['next'] = 'option_threshold'
                    self.options_string_builder[chat]['option_type'] = 'stake_change_threshold'
                else:
                    self.send_option_state(chat)
                    self.options_string_builder[chat]['next'] = 'option_state'
                    self.options_string_builder[chat]['option_type'] = text.replace(' ', '_').lower()
            else:
                message = "Not a possible option type, try again!"
                self.tg.send_message(message, chat)
                self.send_option_type(chat)
        elif next_step == 'option_state':
            if self.validate_option_state(text):
                if text == 'ENABLE':
                    self.db.update_option(chat, self.options_string_builder[chat]['chosen_pool_id'], 
                                                self.options_string_builder[chat]['option_type'], 
                                                1)
                elif text == 'DISABLE':
                    self.db.update_option(chat, self.options_string_builder[chat]['chosen_pool_id'], 
                                                self.options_string_builder[chat]['option_type'], 
                                                0)
                elif text == 'SILENCE':
                    self.db.update_option(chat, self.options_string_builder[chat]['chosen_pool_id'], 
                                                    self.options_string_builder[chat]['option_type'], 
                                                    2)
                message = self.get_current_options(chat, self.options_string_builder[chat]['chosen_pool_id'], self.options_string_builder[chat]['chosen_pool_name'])
                self.tg.send_message(message, chat)
                self.go_back_to_option_type(chat)
            else:
                message = "Not a possible option state, try again!"
                self.tg.send_message(message, chat)
                self.send_option_state(chat)
        elif next_step == 'option_threshold':
            if self.validate_option_threshold(text):
                self.db.update_option(chat, self.options_string_builder[chat]['chosen_pool_id'], 
                                            self.options_string_builder[chat]['option_type'], 
                                            self.convert_values_from_prefix(text))
                message = self.get_current_options(chat, self.options_string_builder[chat]['chosen_pool_id'], self.options_string_builder[chat]['chosen_pool_name'])
                self.tg.send_message(message, chat)
                self.go_back_to_option_type(chat)
            else:
                message = "Not a possible option for threshold, try again!"
                self.tg.send_message(message, chat)
                self.send_option_stake_threshold(chat)

    def handle_option_start(self, chat, tickers, pool_ids):
        self.options_string_builder[chat] = {}
        self.options_string_builder[chat]['string'] = ' '
        self.options_string_builder[chat]['next'] = 'option_pool'

        keyboard_list = []
        ptdb = pooltool_dbhelper.PoolToolDb()
        for i in range(0, len(tickers)):
            pool_name = ptdb.get_pool_name(pool_ids[i])
            if len(pool_name) > 20:
                pool_name = pool_name[:20] + "..."
            pool_name = c.deEmojify(pool_name).rstrip()
            pool_name = pool_name.replace("#", "")
            keyboard_list.append(f"{tickers[i]} - {pool_name}")
            self.options_string_builder[chat][f"{tickers[i]} - {pool_name.upper()}"] = pool_ids[i]

        keyboard_list.append('QUIT')
        keyboard = self.tg.build_keyboard(keyboard_list)
        self.tg.send_message("Select pool", chat, keyboard)

    def handle_reward(self, chat, text):
        text = text.split(' ')
        if len(text) == 1:
            message = "How to add a stake address:\n" \
                      "/reward <stake address(Bech32)>"
            self.tg.send_message(message, chat)
            return
        if len(text) < 2:
            ## Do some help message
            return
        ptdb = pooltool_dbhelper.PoolToolDb()
        reward_addr = text[1].lower()
        if not reward_addr.isascii():
            message = "Please use ASCII characters"
            self.tg.send_message(message, chat)
            return
        try:
            out = subprocess.check_output(f'echo {reward_addr} | /home/kuno/.cabal/bin/cardano-address address inspect', shell=True).decode('utf-8')
            print(out)
            reward_addr_json = json.loads(out)
            stake_key_hash = reward_addr_json['stake_key_hash']
        except Exception as e:
            if ptdb.does_rewards_addr_exist(reward_addr):
                stake_key_hash = reward_addr
            else:
                message = "Please use a valid stake address! (Bech32)"
                #print("Please use a valid stake address!")
                self.tg.send_message(message, chat)
                return
        addr = self.db.get_reward_addr_from_chat_id(chat)
        if reward_addr in addr:
            self.db.delete_user_reward(chat, reward_addr)
            addrs = self.db.get_reward_addr_from_chat_id(chat)
            message = "List of addresses you watch:\n\n" + "\n".join(addrs)
            self.tg.send_message(message, chat)
            return
        if ptdb.does_rewards_addr_exist(stake_key_hash):
            self.db.add_new_reward_addr(chat, reward_addr)
            addrs = self.db.get_reward_addr_from_chat_id(chat)
            message = "List of addresses you watch:\n\n" + "\n".join(addrs)
            self.tg.send_message(message, chat)
        else:
            message = "Not a valid reward address!"
            self.tg.send_message(message, chat)

    def handle_updates(self, updates):
        if 'result' in updates:
            for update in updates["result"]:
                if 'message' in update:
                    #print(update)
                    if 'text' not in update["message"]:
                        continue
                    text = update["message"]["text"].upper()
                    chat = update["message"]["chat"]["id"]
                    message = ''
                    
                    tickers, pool_ids = self.db.get_ticker_poolid_from_chat_id(chat)
                    if chat in self.options_string_builder:
                        if 'type' in self.options_string_builder[chat] and self.options_string_builder[chat]['type'] == 'adding_duplicate':
                            self.handle_duplicate_ticker(text, chat, None, self.options_string_builder[chat]['type'])
                        else:
                            self.handle_next_option_step(chat, text, tickers, pool_ids)
                        continue
                    if text == "/DELETE":
                        if not tickers:
                            self.tg.send_message("No TICKERs added", chat)
                            continue
                        keyboard = self.tg.build_keyboard(tickers)
                        self.tg.send_message("Select pool to delete", chat, keyboard)
                    elif text == "/CLEAR_KEYBOARD":
                        self.tg.send_message("Keyboard removed", chat, self.tg.remove_keyboard(True))
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
                        self.handle_option_start(chat, tickers, pool_ids)
                    elif "/REWARD" in text:
                        self.handle_reward(chat, text)
                    elif "/POOLID" in text:
                        self.handle_new_pool_on_pool_id(chat, text)
                    elif "/LISTREWARD" in text:
                        reward_addrs = self.db.get_reward_addr_from_chat_id(chat)
                        message = self.get_list_of_reward_addr(reward_addrs)
                        self.tg.send_message(message, chat)
                    elif "/LIST" in text:
                        message = self.get_list_of_pools(tickers, pool_ids)
                        self.tg.send_message(message, chat)
                    elif text.startswith("/"):
                        message = "Unknown command, try /help"
                        self.tg.send_message(message, chat)
                        continue
                    elif text in tickers:
                        self.db.delete_user_pool(chat, text)
                        tickers, pool_ids = self.db.get_ticker_poolid_from_chat_id(chat)                        
                        message = self.get_list_of_pools(tickers, pool_ids)
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

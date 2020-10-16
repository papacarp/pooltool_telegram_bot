import time
import boto3
import json
import matplotlib.pyplot as plt
import math
import io
import threading
import subprocess
import plotly.express as px
import plotly.io

from scipy.stats import binom
from os import environ

from modules.emoji import Emoji as e
from modules import common as c
from modules import pooltool_dbhelper

plotly.io.orca.config.executable = '/usr/local/bin/orca'
plotly.io.orca.config.save()

class EventHandler:
    def __init__(self, db, tg):
        self.db = db
        self.tg = tg

        environ["AWS_PROFILE"] = "bot_iam"
        self.client = boto3.client('sts')
        self.session = boto3.Session(profile_name='bot_iam')
        self.sqs = boto3.client('sqs')
        self.queue_url = 'https://sqs.us-west-2.amazonaws.com/637019325511/pooltoolevents.fifo'

        self.plot_number = 0

    def get_aws_event(self):
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                AttributeNames=[
                    'SentTimestamp'
                ],
                MaxNumberOfMessages=1,
                MessageAttributeNames=[
                    'All'
                ],
                # VisibilityTimeout=0,
                WaitTimeSeconds=20
            )
            if 'Messages' in response:
                if len(response['Messages']) > 0:
                    return response['Messages'][0]
        except:
            return ''
        return ''

    def delete_aws_event_from_queue(self, receipt_handle):
        self.sqs.delete_message(
            QueueUrl=self.queue_url,
            ReceiptHandle=receipt_handle
        )

    def get_ticker_from_pool_id_file(self, pool_id):
        with open(c.ticker_file_path, 'r') as ticker_file:
            tickers = json.load(ticker_file)
        if pool_id in tickers['tickers']:
            return tickers['tickers'][pool_id]
        return 'UNKNOWN'

    def handle_battle(self, data):
        with open('battle', 'w') as f:
            f.write(json.dumps(data))

        def what_battle_type(players):
            slot_check = ''
            for player in players:
                if slot_check == '':
                    slot_check = player['slot']
                else:
                    if slot_check != player['slot']:
                        return 'Height'
            return 'Slot'

        def who_battled(players, winner):
            tickers = []
            for player in players:
                if winner == player['pool']:
                    tickers.append(self.get_ticker_from_pool_id_file(player['pool']) + f'{e.crown}')
                else:
                    tickers.append(self.get_ticker_from_pool_id_file(player['pool']))
            return ' vs '.join(tickers)

        def which_slot(players):
            slots = []
            for player in players:
                if 'slot' in player and player['slot'] is not None:
                    slots.append(player['slot'])
                else:
                    slots.append('UNKNOWN')
            return ' vs '.join(slots)

        players = data['players']
        height = data['height']
        battle_type = what_battle_type(players)
        competitors = who_battled(players, self.get_ticker_from_pool_id_file(data['winner']))
        slots = which_slot(players)
        for player in data['players']:
            if player['pool'] == data['winner']:
                chat_ids = self.db.get_chat_ids_from_pool_id(player['pool'])
                for chat_id in chat_ids:
                    ticker = self.db.get_ticker_from_pool_id(player['pool'])[0]
                    message_type = self.db.get_option_value(chat_id, ticker, 'battle')
                    if message_type:
                        message = f'\\[ {ticker} ] You won! {e.throphy}\n' \
                                  f'\n' \
                                  f'{e.swords}{battle_type} battle: {competitors}\n' \
                                  f'{e.clock} Slot: {slots}\n' \
                                  f'{e.brick} Height: {height}\n' \
                                  f'\n' \
                                  f'https://pooltool.io/competitive\n' \
                                  f'#{ticker}'
                        if message_type == 2:
                            self.tg.send_message(message, chat_id, silent=True, disable_web_preview=True)
                        else:
                            self.tg.send_message(message, chat_id, disable_web_preview=True)
            else:
                chat_ids = self.db.get_chat_ids_from_pool_id(player['pool'])
                for chat_id in chat_ids:
                    ticker = self.db.get_ticker_from_pool_id(player['pool'])[0]
                    message_type = self.db.get_option_value(chat_id, ticker, 'battle')
                    if message_type:
                        message = f'\\[ {ticker} ] You lost! {e.annoyed}\n' \
                                  f'\n' \
                                  f'{e.swords} {battle_type} battle: {competitors}\n' \
                                  f'{e.clock} Slot: {slots}\n' \
                                  f'{e.brick} Height: {height}\n' \
                                  f'\n' \
                                  f'https://pooltool.io/competitive \n' \
                                  f'#{ticker}'
                        if message_type == 2:
                            self.tg.send_message(message, chat_id, silent=True, disable_web_preview=True)
                        else:
                            self.tg.send_message(message, chat_id, disable_web_preview=True)

    def handle_wallet_poolchange(self, data):
        with open('wallet_poolchange', 'w') as f:
            f.write(json.dumps(data))

        pool_id = data['pool']
        pooltool_url = f'https://pooltool.io/pool/{pool_id}'
        chat_ids = self.db.get_chat_ids_from_pool_id(pool_id)
        if chat_ids:
            ticker = self.db.get_ticker_from_pool_id(pool_id)[0]
        else:
            return
        if 'ticker' in data['change']:
            new_ticker = data['change']['ticker']['new_value']
            self.db.update_ticker(pool_id, new_ticker)
            message = f"\\[ {ticker} ] Pool change {e.warning} Ticker\n" \
                      f"\n" \
                      f"From: `{data['change']['ticker']['old_value']}`\n" \
                      f"To: `{data['change']['ticker']['new_value']}`\n" \
                      f"\n" \
                      f"More info at:\n" \
                      f"[Pooltool]({pooltool_url})\n" \
                      f"#{ticker}"
        elif 'cost' in data['change']:
            if int(data['change']['cost']['old_value']) < int(data['change']['cost']['new_value']):
                message = f"\\[ {ticker} ] Pool change {e.warning} Fixed cost\n" \
                          f"\n" \
                          f"From: `{e.ada}{data['change']['cost']['old_value']}`\n" \
                          f"To: `{e.ada}{data['change']['cost']['new_value']}`\n" \
                          f"\n" \
                          f"More info at:\n" \
                          f"[Pooltool]({pooltool_url})\n" \
                          f"#{ticker}"
            else:
                message = f"\\[ {ticker} ] Pool change {e.party} Fixed cost\n" \
                          f"\n" \
                          f"From: `{e.ada}{data['change']['cost']['old_value']}`\n" \
                          f"To: `{e.ada}{data['change']['cost']['new_value']}`\n" \
                          f"\n" \
                          f"More info at:\n" \
                          f"[Pooltool]({pooltool_url})\n" \
                          f"#{ticker}"
        elif 'margin' in data['change']:
            if float(data['change']['margin']['old_value']) < float(data['change']['margin']['new_value']):
                message = f"\\[ {ticker} ] Pool change {e.warning} Margin\n" \
                          f"\n" \
                          f"From: `{float(data['change']['margin']['old_value']) * 100}%`\n" \
                          f"To: `{float(data['change']['margin']['new_value']) * 100}%`\n" \
                          f"\n" \
                          f"More info at:\n" \
                          f"[Pooltool]({pooltool_url})\n" \
                          f"#{ticker}"
            else:
                message = f"\\[ {ticker} ] Pool change {e.party} Margin\n" \
                          f"\n" \
                          f"From: `{float(data['change']['margin']['old_value']) * 100}%`\n" \
                          f"To: `{float(data['change']['margin']['new_value']) * 100}%`\n" \
                          f"\n" \
                          f"More info at:\n" \
                          f"[Pooltool]({pooltool_url})\n" \
                          f"#{ticker}"
        elif 'pledge' in data['change']:
            if int(data['change']['pledge']['old_value']) < int(data['change']['pledge']['new_value']):
                message = f"\\[ {ticker} ] Pool change {e.party} Pledge\n" \
                          f"\n" \
                          f"From: `{e.ada}{c.set_prefix(round(int(data['change']['pledge']['old_value']) / 1000000))}`\n" \
                          f"To: `{e.ada}{c.set_prefix(round(int(data['change']['pledge']['new_value']) / 1000000))}`\n" \
                          f"\n" \
                          f"More info at:\n" \
                          f"[Pooltool]({pooltool_url})\n" \
                          f"#{ticker}"
            else:
                message = f"\\[ {ticker} ] Pool change {e.warning} Pledge\n" \
                          f"\n" \
                          f"From: `{e.ada}{c.set_prefix(round(int(data['change']['pledge']['old_value']) / 1000000))}`\n" \
                          f"To: `{e.ada}{c.set_prefix(round(int(data['change']['pledge']['new_value']) / 1000000))}`\n" \
                          f"\n" \
                          f"More info at:\n" \
                          f"[Pooltool]({pooltool_url})\n" \
                          f"#{ticker}"
        for chat_id in chat_ids:
            self.tg.send_message(message, chat_id)
            message_type = self.db.get_option_value(chat_id, ticker, 'pool_change')
            if message_type:
                if message_type == 2:
                    self.tg.send_message(message, chat_id, silent=True)
                else:
                    self.tg.send_message(message, chat_id)

    def handle_block_minted(self, data):
        with open('block_minted', 'w') as f:
            f.write(json.dumps(data))

        pool_id = data['pool']
        pooltool_url = f'https://pooltool.io/pool/{pool_id}/blocks'
        chat_ids = self.db.get_chat_ids_from_pool_id(pool_id)
        for chat_id in chat_ids:
            ticker = self.db.get_ticker_from_pool_id(pool_id)[0]
            message_type = self.db.get_option_value(chat_id, ticker, 'block_minted')
            if message_type:
                message = f'\\[ {ticker} ] New block! {e.fire}\n' \
                          f'\n' \
                          f"{e.tools} Blocks this epoch: `{data['nbe']}`\n" \
                          f"{e.brick} Total blocks: `{data['nb']}`\n" \
                          f"\n" \
                          f"More info at:\n" \
                          f"[Pooltool]({pooltool_url})\n" \
                          f'#{ticker}'
                if message_type == 2:
                    self.tg.send_message(message, chat_id, silent=True)
                else:
                    self.tg.send_message(message, chat_id)

    def check_delegation_changes(self, chat_id, ticker, delegations, new_delegations, message_type, threshold,
                                 pooltool_url):
        if c.DEBUG:
            stake_millis = c.get_current_time_millis()

        if abs(delegations - new_delegations) < threshold or abs(delegations - new_delegations) < 1:
            return
        if delegations > new_delegations:
            message = f'\\[ {ticker} ] Stake decreased 💔\n' \
                      f'`-{e.ada}{c.set_prefix(round(delegations - new_delegations))}`\n' \
                      f'Livestake: `{e.ada}{c.set_prefix(round(new_delegations))}`\n' \
                      f'\n' \
                      f'More info at:\n' \
                      f'[Pooltool]({pooltool_url})\n' \
                      f'#{ticker}'
            if c.DEBUG:
                print(f"creating message: {c.get_current_time_millis() - stake_millis}")
                stake_millis = c.get_current_time_millis()
            if message_type == 2:
                self.tg.send_message(message, chat_id, silent=True)
            else:
                self.tg.send_message(message, chat_id)
        elif delegations < new_delegations:
            message = f'\\[ {ticker} ] Stake increased 💚\n' \
                      f'`+{e.ada}{c.set_prefix(round(new_delegations - delegations))}`\n' \
                      f'Livestake: `{e.ada}{c.set_prefix(round(new_delegations))}`\n' \
                      f'\n' \
                      f'More info at:\n' \
                      f'[Pooltool]({pooltool_url})\n' \
                      f'#{ticker}'
            if c.DEBUG:
                print(f"creating message: {c.get_current_time_millis() - stake_millis}")
                stake_millis = c.get_current_time_millis()
            if message_type == 2:
                self.tg.send_message(message, chat_id, silent=True)
            else:
                self.tg.send_message(message, chat_id)

    def handle_stake_change(self, data):
        if c.DEBUG:
            stake_millis = c.get_current_time_millis()

        with open('stake_change', 'w') as f:
            f.write(json.dumps(data))

        if c.DEBUG:
            print(f"write to file: {c.get_current_time_millis() - stake_millis}")
            stake_millis = c.get_current_time_millis()

        pool_id = data['pool']
        pooltool_url = f'https://pooltool.io/pool/{pool_id}/delegators'
        chat_ids = self.db.get_chat_ids_from_pool_id(pool_id)

        if c.DEBUG:
            print(f"getting chat ids from db: {c.get_current_time_millis() - stake_millis}")
            stake_millis = c.get_current_time_millis()

        if chat_ids:
            ticker = self.db.get_ticker_from_pool_id(pool_id)[0]

            if c.DEBUG:
                print(f"getting ticker from db: {c.get_current_time_millis() - stake_millis}")

            for chat_id in chat_ids:
                message_type = self.db.get_option_value(chat_id, ticker, 'stake_change')
                if message_type:
                    threshold = self.db.get_option_value(chat_id, ticker, 'stake_change_threshold')
                    self.check_delegation_changes(chat_id, ticker, data['old_stake'] / 1000000,
                                                  data['livestake'] / 1000000,
                                                  message_type, threshold, pooltool_url)

    def handle_block_adjustment(self, data):
        with open('block_adjustment', 'w') as f:
            f.write(json.dumps(data))

        if data['old_epoch_blocks'] == data['new_epoch_blocks']:
            return
        pool_id = data['pool']
        chat_ids = self.db.get_chat_ids_from_pool_id(pool_id)
        current_epoch = '?'
        for chat_id in chat_ids:
            ticker = self.db.get_ticker_from_pool_id(pool_id)[0]
            message_type = self.db.get_option_value(chat_id, ticker, 'block_adjustment')
            if message_type:
                message = f'\\[ {ticker} ] Block adjustment{e.warning}\n' \
                          f'\n' \
                          f"Total blocks has changed: {data['old_epoch_blocks']} to {data['new_epoch_blocks']}\n" \
                          f"Epoch: {current_epoch}\n" \
                          f"\n" \
                          f"More info at:\n" \
                          f"https://pooltool.io/\n" \
                          f"#{ticker}"
                if message_type == 2:
                    self.tg.send_message(message, chat_id, silent=True)
                else:
                    self.tg.send_message(message, chat_id)

    def handle_sync_status(self, data):
        with open('sync_status', 'w') as f:
            f.write(json.dumps(data))

        pool_id = data['pool']
        chat_ids = self.db.get_chat_ids_from_pool_id(pool_id)
        for chat_id in chat_ids:
            ticker = self.db.get_ticker_from_pool_id(pool_id)[0]
            message_type = self.db.get_option_value(chat_id, ticker, 'sync_status')
            if message_type:
                if not data['new_status']:
                    message = f'\\[ {ticker} ] Out of sync {e.alert}'
                    if message_type == 2:
                        self.tg.send_message(message, chat_id, silent=True)
                    else:
                        self.tg.send_message(message, chat_id)
                else:
                    message = f'\\[ {ticker} ] Back in sync {e.like}'
                    if message_type == 2:
                        self.tg.send_message(message, chat_id, silent=True)
                    else:
                        self.tg.send_message(message, chat_id)

    def handle_epoch_summary_old(self, data):
        with open('epoch_summary', 'w') as f:
            f.write(json.dumps(data))

        pool_id = data['pool']
        delegations = data['liveStake'] / 1000000
        rewards_stakers = data['value_for_stakers'] / 1000000
        rewards_tax = data['value_taxed'] / 1000000
        blockstake = data['blockstake'] / 1000000
        last_epoch = data['epoch']
        wins = data['w']
        losses = data['l']
        blocks_minted = int(data['blocks'])
        epoch_slots = data['epochSlots']
        if isinstance(epoch_slots, str):
            try:
                epoch_slots = int(epoch_slots)
            except Exception as ex:
                print("Could not convert to int, trying float")
            try:
                epoch_slots = int(float(epoch_slots))
            except Exception as ex:
                print("Could not convert to float either. Skipping...")
                return
        if epoch_slots:
            print(
                f'blocks_minted: {blocks_minted}, type: {type(blocks_minted)} - epochSlots: {epoch_slots}, type: {type(epoch_slots)}, star: {blocks_minted == epoch_slots}')
            if blocks_minted == epoch_slots and epoch_slots > 0:
                blocks_created_text = f'/{epoch_slots} {e.star}'
            else:
                blocks_created_text = f'/{epoch_slots}'
        else:
            blocks_created_text = ''

        if blockstake:
            current_ros = round((math.pow((rewards_stakers / blockstake) + 1, 365) - 1) * 100, 2)
        else:
            current_ros = 0

        chat_ids = self.db.get_chat_ids_from_pool_id(pool_id)
        for chat_id in chat_ids:
            ticker = self.db.get_ticker_from_pool_id(pool_id)[0]
            message_type = self.db.get_option_value(chat_id, ticker, 'epoch_summary')
            if message_type:
                message = f'\\[ {ticker} ] Epoch {last_epoch} stats {e.globe}\n' \
                          f'\n' \
                          f'{e.meat} Live stake {c.set_prefix(delegations)}\n' \
                          f"{e.tools} Blocks created: {blocks_minted}{blocks_created_text}\n" \
                          f'{e.swords} Slot battles: {wins}/{wins + losses}\n' \
                          f'\n' \
                          f'{e.moneyBag} Stakers rewards: {c.set_prefix(round(rewards_stakers))} ADA\n' \
                          f'{e.flyingMoney} Tax rewards: {c.set_prefix(round(rewards_tax))} ADA\n' \
                          f'\n' \
                          f'Current ROS: {current_ros}%\n' \
                          f'\n' \
                          f'More info at:\n' \
                          f'[Pooltool](https://pooltool.io/pool/{pool_id})/\n' \
                          f'#{ticker}'
                if message_type == 2:
                    self.tg.send_message(message, chat_id, silent=True)
                else:
                    self.tg.send_message(message, chat_id)

    def handle_slot_loaded(self, data):
        return
        with open('slot_loaded', 'w') as f:
            f.write(json.dumps(data))

        pool_id = data['poolid']
        epoch = data['epoch']
        slots_assigned = data['epochSlots']
        last_epoch_validated = data['verifiedPreviousEpoch']
        chat_ids = self.db.get_chat_ids_from_pool_id(pool_id)
        for chat_id in chat_ids:
            ticker = self.db.get_ticker_from_pool_id(pool_id)[0]
            message_type = self.db.get_option_value(chat_id, ticker, 'slot_loaded')
            if message_type:
                message = f'\\[ {ticker} ] Epoch {epoch} {e.dice}\n' \
                          f'\n' \
                          f'Blocks assigned: {slots_assigned}\n' \
                          f'Last epoch validated: {last_epoch_validated}\n' \
                          f'#{ticker}'
                if message_type == 2:
                    self.tg.send_message(message, chat_id, silent=True)
                else:
                    self.tg.send_message(message, chat_id)

    def handle_announcement(self, data):
        pool_id = data['pool']
        text = data['text'].replace("_", "\\_").replace("*", "\\*").replace("[", "\\[").replace("`", "\\`");
        chat_ids = self.db.get_chat_ids_from_pool_id(pool_id)
        for chat_id in chat_ids:
            ticker = self.db.get_ticker_from_pool_id(pool_id)[0]
            message = f'\\[ {ticker} ] Announcement {e.globe}\n' \
                      f'\n' \
                      f"{text}\n" \
                      f"#{ticker}"
            self.tg.send_message(message, chat_id)

    def handle_award(self, data):
        pool_id = data['pool']
        chat_ids = self.db.get_chat_ids_from_pool_id(pool_id)
        for chat_id in chat_ids:
            ticker = self.db.get_ticker_from_pool_id(pool_id)[0]
            message_type = self.db.get_option_value(chat_id, ticker, 'award')
            if not message_type:
                continue
            award_data = data['award']
            nl = '\n'
            image_url = f"https://pooltool.io/{award_data['award']}.png"
            message = f'\\[ {ticker} ] Award! {e.throphy}\n' \
                      f'\n' \
                      f"{award_data['text'].replace('<br/>', nl)}\n" \
                      f"{award_data['hash']}\n" \
                      f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(award_data['value']))} UTC\n" \
                      f"#{ticker}"

            if message_type == 2:
                self.tg.send_message(message, chat_id, silent=True)
                self.tg.send_image_remote_file(image_url, chat_id, award_data['award'] + '.png')
            else:
                self.tg.send_message(message, chat_id)
                self.tg.send_image_remote_file(image_url, chat_id, award_data['award'] + '.png')

    def handle_block_estimation(self, data):
        pool_id = data['pool']
        chat_ids = self.db.get_chat_ids_from_pool_id(pool_id)
        if chat_ids:
            ticker = self.db.get_ticker_from_pool_id(pool_id)[0]

            active_stake = data['active_stake']
            pool_stake = data['pool_stake']
            d = data['d']
            epoch = data['epoch']

            total_block = 21600
            n = total_block * (1 - d)
            p = pool_stake / active_stake
            var = n * p * (1 - p)

            r_values = list(range(int(var * 2 + 1) if var > 1 else 10 + 1))

            dist = [binom.pmf(r, n, p) * 100 for r in r_values]

            plt.figure(self.plot_number)
            self.plot_number = self.plot_number + 1
            plt.title(f'{ticker} Epoch {epoch}: # of expected blocks')
            plt.xlabel('Number of blocks')
            plt.ylabel('Probability in %')
            plt.bar(r_values, dist)
            buf = io.BytesIO()
            plt.savefig(buf, format='png')

            for chat_id in chat_ids:
                message_type = self.db.get_option_value(chat_id, ticker, 'block_estimation')
                if not message_type:
                    continue
                buf.seek(0)
                if message_type == 2:
                    self.tg.send_image(buf, chat_id)
                else:
                    self.tg.send_image(buf, chat_id)
    
    def handle_reward(self, data):
        epoch = data['epoch']
        chat_ids = self.db.get_all_reward_users()
        ptdb = pooltool_dbhelper.PoolToolDb()
        for chat_id in chat_ids:
            addrs = self.db.get_reward_addr_from_chat_id(chat_id)
            for addr in addrs:
                try:
                    reward_addr_json = json.loads(subprocess.check_output(f'echo {addr} | ~/wallet/cardano-wallet-shelley-linux64/cardano-address address inspect', shell=True).decode('utf-8'))
                    stake_key_hash = reward_addr_json['stake_key_hash']
                except:
                    if ptdb.does_rewards_addr_exist(addr):
                        stake_key_hash = addr
                    else:
                        continue
                url = f'https://pooltool.io/address/{stake_key_hash}'
                reward = ptdb.get_stake_rewards(stake_key_hash, epoch)
                total_reward = ptdb.get_total_stake_rewards(stake_key_hash)
                operator_rewards = ptdb.get_operator_rewards(stake_key_hash, epoch)
                total_operator_rewards = ptdb.get_total_operator_rewards(stake_key_hash)
                message = f'Rewards! {e.moneyBag} epoch {epoch}\n' \
                          f'`{addr[:5]}...{addr[len(addr) - 5:]}`\n' \
                          f'\n'
                if reward < 1000000:
                    message += f'Rewards: `{c.set_prefix(round(reward))} Lovelace`\n'
                else:
                    message += f'Rewards: `{e.ada}{c.set_prefix(round(reward / 1000000))}`\n'
                if operator_rewards:
                    message += f'Operator rewards: `{e.ada}{c.set_prefix(round(operator_rewards / 1000000))}`\n' \
                               f'Lifetime rewards: `{e.ada}{c.set_prefix(round((total_reward + total_operator_rewards) / 1000000))}`\n'
                else:
                    if total_reward < 1000000:
                        message += f'Lifetime rewards: `{c.set_prefix(round(total_reward))} Lovelace`\n'
                    else:    
                        message += f'Lifetime rewards: `{e.ada}{c.set_prefix(round(total_reward / 1000000))}`\n'
                message += f'\n' \
                           f'More info at:\n' \
                           f'[Pooltool]({url})' 
                self.tg.send_message(message, chat_id)

    def handle_epoch_summary(self, data):
        pools = self.db.get_all_subscribed_pool()
        ptdb = pooltool_dbhelper.PoolToolDb()
        epoch = data['epoch']
        d = data['d']
        total_block = 21600
        total_circ_supply = 31112484646000000

        #pools = ['dcfbfc65083fd8a1d931b826e67549323d4946f02eda20622b618321'] * 20
        for pool in pools:
            ticker = self.db.get_ticker_from_pool_id(pool)
            if len(ticker) < 1:
                continue
            ticker = ticker[0]
            chat_ids = self.db.get_chat_ids_from_pool_id(pool)  
            #chat_ids = [488598281]
            total_delegators = ptdb.get_total_delegators(pool, epoch)

            livestake, pool_first_epoch, pool_lifetime_rewards, pool_lifetime_stake, pool_donestake, block_stake = ptdb.get_pools_data_for_summary(pool)
            block_stake_epoch, blocks_minted, forecasted_tax, forecasted_reward = ptdb.get_pool_epoch_data_for_summary(pool, epoch)
            if block_stake_epoch == -1 and blocks_minted == -1 and forecasted_tax == -1 and forecasted_reward == -1:
                continue
            block_stake_epoch_prev, blocks_minted_prev, forecasted_tax_prev, forecasted_reward_prev = ptdb.get_pool_epoch_data_for_summary(pool, epoch - 1)
            if block_stake_epoch_prev == -1 and blocks_minted_prev == -1 and forecasted_tax_prev == -1 and forecasted_reward_prev == -1:
                continue
            
            delegator_rewards, pool_rewards = ptdb.get_pool_epoch_rewards(pool, epoch - 1)
            current_genesis_epoch, genesis_total_stake = ptdb.get_genesis_data_for_summary()

            compoundingperiods = 1 #(current_genesis_epoch - pool_first_epoch - 1) #fix when epoch is not a day
            if pool_lifetime_stake - pool_donestake - block_stake <= 0:
                roioverspan = 0
            else:
                roioverspan = pool_lifetime_rewards / ((pool_lifetime_stake - pool_donestake - block_stake) / compoundingperiods)
            ros = math.pow(roioverspan + 1, 1 / (compoundingperiods / (365 / 5))) - 1
            if block_stake_epoch_prev == 0:
                current_ros = 0
            else:
                current_ros = math.pow((delegator_rewards / block_stake_epoch_prev) + 1, 365 / 5) - 1
            if block_stake_epoch == 0:
                estimated_ros = 0
            else:
                estimated_ros = math.pow((forecasted_reward / block_stake_epoch) + 1, 365 / 5) - 1

            pool_stake = block_stake
            n = total_block * (1 - d)
            p = pool_stake / genesis_total_stake
            var = n * p * (1 - p)

            tmp_r_values = list(range(300))
            tmp_dist = [binom.pmf(r, n, p) * 100 for r in tmp_r_values]

            # Do a cleanup and remove ~0 values
            r_values = []
            dist = []
            for i, di in enumerate(tmp_dist):
                if di < 0.005:
                    continue
                r_values.append(tmp_r_values[i])
                dist.append(tmp_dist[i])

            estimated_blocks = round(var, 2)  

            saturation = (pool_stake / (total_circ_supply  / 150)) * 100

            fig = px.bar(x=r_values, y=dist, template='plotly_dark')
            fig.update_layout(
                title=f"{ticker} Epoch {epoch + 1}: # of expected block",
                xaxis_title="Number of blocks",
                yaxis_title="Probability in %",
                paper_bgcolor='rgb(24, 37, 51)',
                plot_bgcolor='rgb(24, 37, 51)'
            )
            fig.update_traces(marker_color='rgb(49, 100, 192)')
            img_bytes = fig.to_image(format="png")

            for chat_id in chat_ids:       
                message_type = self.db.get_option_value(chat_id, ticker, 'epoch_summary')

                if message_type:
                    message = f'*[ {ticker} ] Epoch {epoch} summary {e.globe}*\n' 
                    message += f'\n' 
                    message += f"Active stake: `{e.ada}{c.set_prefix(round(block_stake_epoch / 1000000)).replace(' ', '')}`\n" 
                    message += f'Blocks minted: `{blocks_minted}`\n' 
                    message += f'Total stakeholders: `{total_delegators}`\n' 
                    message += f'\n'
                    message += f'Lifetime ROS: `{round(ros * 100, 2)}%`\n'
                    message += f"Live stake: `{e.ada}{c.set_prefix(round(livestake / 1000000)).replace(' ', '')}`\n" 
                    message += f'Pool Saturation: `{round(saturation, 2)}%`\n' 
                    message += f'\n' 
                    message += f'*Rewards for epoch {epoch - 1}*\n' 
                    message += f"  Active stake: `{e.ada}{c.set_prefix(round(block_stake_epoch_prev / 1000000)).replace(' ', '')}`\n"
                    message += f"  Stakeholder rewards: `{e.ada}{c.set_prefix(round(delegator_rewards / 1000000)).replace(' ', '')}`\n" 
                    message += f"  Pool rewards: `{e.ada}{c.set_prefix(round(pool_rewards / 1000000)).replace(' ', '')}`\n" 
                    message += f'  Stakeholder ROS: `{round(current_ros * 100, 2)}%`\n'
                    message += f'\n' 
                    message += f'*Estimated rewards for epoch {epoch}*\n' 
                    message += f"  Stakeholder rewards: `{e.ada}{c.set_prefix(round(forecasted_reward / 1000000)).replace(' ', '')}`\n" 
                    message += f"  Pool rewards: `{e.ada}{c.set_prefix(round(forecasted_tax / 1000000)).replace(' ', '')}`\n"
                    message += f'  Stakeholder ROS: `{round(estimated_ros * 100, 2)}%`' 
                    message += f'\n' 
                    message += f'Estimated blocks epoch {epoch + 1}: `{estimated_blocks}`\n' 
                    message += f'\n' 
                    message += f'_This Bot is brought to you by_ *[ ETR ]*'
                    if message_type == 2:
                        self.tg.send_message(message, chat_id, silent=True)
                    else:
                        self.tg.send_message(message, chat_id)

                    message_type = self.db.get_option_value(chat_id, ticker, 'block_estimation')
                    if not message_type:
                        continue
                    #buf.seek(0)
                    if message_type == 2:
                        self.tg.send_image(img_bytes, chat_id)
                    else:
                        self.tg.send_image(img_bytes, chat_id)
                        

    def handle_event(self, body):
        data = body['data']
        if body['type'] == 'battle':
            self.handle_battle(data)
        elif body['type'] == 'wallet_poolchange':
            self.handle_wallet_poolchange(data)
        elif body['type'] == 'wallet_newpool':
            c.handle_wallet_newpool(self.db)
        elif body['type'] == 'block_minted':
            self.handle_block_minted(data)
        elif body['type'] == 'stake_change':
            self.handle_stake_change(data)
        elif body['type'] == 'block_adjustment':
            self.handle_block_adjustment(data)
        elif body['type'] == 'sync_change':
            self.handle_sync_status(data)
        elif body['type'] == 'epoch_summary':
            self.handle_epoch_summary(data)
        elif body['type'] == 'slots_loaded':
            self.handle_slot_loaded(data)
        elif body['type'] == 'announcement':
            self.handle_announcement(data)
        elif body['type'] == 'award':
            self.handle_award(data)
        elif body['type'] == 'block_estimation':
            self.handle_block_estimation(data)
        elif body['type'] == 'reward':
            self.handle_reward(data)

    def run(self):
        if c.DEBUG:
            get_event_millis = c.get_current_time_millis()

        while True:
            event = self.get_aws_event()
            if event != '':

                if c.DEBUG:
                    print(f"New event - time since last event: {c.get_current_time_millis() - get_event_millis}")
                    get_event_millis = c.get_current_time_millis()

                self.delete_aws_event_from_queue(event['ReceiptHandle'])

                if c.DEBUG:
                    handle_event_millis = c.get_current_time_millis()

                event_handler = threading.Thread(target=self.handle_event, args=(json.loads(event['Body']),))
                event_handler.start()

                #self.handle_event(json.loads(event['Body']))

                if c.DEBUG:
                    print(f"Time it took to handle event: {c.get_current_time_millis() - handle_event_millis}")

            time.sleep(0.5)

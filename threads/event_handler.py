import time
import boto3
import json
import matplotlib.pyplot as plt
import math
import io

from scipy.stats import binom
from os import environ

from modules.emoji import Emoji as e
from modules import common as c


class EventHandler:
    def __init__(self, db, tg):
        self.db = db
        self.tg = tg

        environ["AWS_PROFILE"] = "bot_iam"
        self.client = boto3.client('sts')
        self.session = boto3.Session(profile_name='bot_iam')
        self.sqs = boto3.client('sqs')
        self.queue_url = 'https://sqs.us-west-2.amazonaws.com/637019325511/pooltoolevents.fifo'

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

    def get_current_time_millis(self):
        return int(round(time.time() * 1000))

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
                      f"From: {data['change']['ticker']['old_value']}\n" \
                      f"To: {data['change']['ticker']['new_value']}\n" \
                      f"\n" \
                      f"More info at:\n" \
                      f"[Pooltool]({pooltool_url})\n" \
                      f"#{ticker}"
        elif 'cost' in data['change']:
            if int(data['change']['cost']['old_value']) < int(data['change']['cost']['new_value']):
                message = f"\\[ {ticker} ] Pool change {e.warning} Fixed cost\n" \
                          f"\n" \
                          f"From: {data['change']['cost']['old_value']} {e.ada}\n" \
                          f"To: {data['change']['cost']['new_value']} {e.ada}\n" \
                          f"\n" \
                          f"More info at:\n" \
                          f"[Pooltool]({pooltool_url})\n" \
                          f"#{ticker}"
            else:
                message = f"\\[ {ticker} ] Pool change {e.party} Fixed cost\n" \
                          f"\n" \
                          f"From: {data['change']['cost']['old_value']} {e.ada}\n" \
                          f"To: {data['change']['cost']['new_value']} {e.ada}\n" \
                          f"\n" \
                          f"More info at:\n" \
                          f"[Pooltool]({pooltool_url})\n" \
                          f"#{ticker}"
        elif 'margin' in data['change']:
            if float(data['change']['margin']['old_value']) < float(data['change']['margin']['new_value']):
                message = f"\\[ {ticker} ] Pool change {e.warning} Margin\n" \
                          f"\n" \
                          f"From: {float(data['change']['margin']['old_value']) * 100}%\n" \
                          f"To: {float(data['change']['margin']['new_value']) * 100}%\n" \
                          f"\n" \
                          f"More info at:\n" \
                          f"[Pooltool]({pooltool_url})\n" \
                          f"#{ticker}"
            else:
                message = f"\\[ {ticker} ] Pool change {e.party} Margin\n" \
                          f"\n" \
                          f"From: {float(data['change']['margin']['old_value']) * 100}%\n" \
                          f"To: {float(data['change']['margin']['new_value']) * 100}%\n" \
                          f"\n" \
                          f"More info at:\n" \
                          f"[Pooltool]({pooltool_url})\n" \
                          f"#{ticker}"
        elif 'pledge' in data['change']:
            if int(data['change']['pledge']['old_value']) < int(data['change']['pledge']['new_value']):
                message = f"\\[ {ticker} ] Pool change {e.party} Pledge\n" \
                          f"\n" \
                          f"From: {c.set_prefix(round(int(data['change']['pledge']['old_value']) / 1000000))} {e.ada}\n" \
                          f"To: {c.set_prefix(round(int(data['change']['pledge']['new_value']) / 1000000))} {e.ada}\n" \
                          f"\n" \
                          f"More info at:\n" \
                          f"[Pooltool]({pooltool_url})\n" \
                          f"#{ticker}"
            else:
                message = f"\\[ {ticker} ] Pool change {e.warning} Pledge\n" \
                          f"\n" \
                          f"From: {c.set_prefix(round(int(data['change']['pledge']['old_value']) / 1000000))} {e.ada}\n" \
                          f"To: {c.set_prefix(round(int(data['change']['pledge']['new_value']) / 1000000))} {e.ada}\n" \
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
                          f"{e.tools} Blocks this epoch: {data['nbe']}\n" \
                          f"{e.brick} Total blocks: {data['nb']}\n" \
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
        if abs(delegations - new_delegations) < threshold or abs(delegations - new_delegations) < 1:
            return
        if delegations > new_delegations:
            message = f'\\[ {ticker} ] Stake decreased 💔\n' \
                      f'-{c.set_prefix(round(delegations - new_delegations))} {e.ada}\n' \
                      f'Livestake: {c.set_prefix(round(new_delegations))} {e.ada}\n' \
                      f'\n' \
                      f'More info at:\n' \
                      f'[Pooltool]({pooltool_url})\n' \
                      f'#{ticker}'
            if message_type == 2:
                self.tg.send_message(message, chat_id, silent=True)
            else:
                self.tg.send_message(message, chat_id)
        elif delegations < new_delegations:
            message = f'\\[ {ticker} ] Stake increased 💚\n' \
                      f'+{c.set_prefix(round(new_delegations - delegations))} {e.ada}\n' \
                      f'Livestake: {c.set_prefix(round(new_delegations))} {e.ada}\n' \
                      f'\n' \
                      f'More info at:\n' \
                      f'[Pooltool]({pooltool_url})\n' \
                      f'#{ticker}'
            if message_type == 2:
                self.tg.send_message(message, chat_id, silent=True)
            else:
                self.tg.send_message(message, chat_id)

    def handle_stake_change(self, data):
        with open('stake_change', 'w') as f:
            f.write(json.dumps(data))

        pool_id = data['pool']
        pooltool_url = f'https://pooltool.io/pool/{pool_id}/delegators'
        chat_ids = self.db.get_chat_ids_from_pool_id(pool_id)
        if chat_ids:
            ticker = self.db.get_ticker_from_pool_id(pool_id)[0]
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

    def handle_epoch_summary(self, data):
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
        chat_ids = self.db.get_chat_ids_from_pool_id(pool_id)
        for chat_id in chat_ids:
            ticker = self.db.get_ticker_from_pool_id(pool_id)[0]
            message = f'\\[ {ticker} ] Announcement {e.globe}\n' \
                      f'\n' \
                      f"{data['text']}\n" \
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

            plt.clf()
            plt.title(f'{ticker} Epoch {epoch}: # of expected blocks')
            plt.xlabel('Number of blocks')
            plt.ylabel('Probability in %')
            plt.bar(r_values, dist)
            buf = io.BytesIO()
            plt.savefig(buf, format='png')
            buf.seek(0)

            for chat_id in chat_ids:
                message_type = self.db.get_option_value(chat_id, ticker, 'block_estimation')
                if not message_type:
                    continue
                if message_type == 2:
                    self.tg.send_image(buf, chat_id)
                else:
                    self.tg.send_image(buf, chat_id)

    def run(self):
        get_event_millis = self.get_current_time_millis()
        while True:
            event = self.get_aws_event()
            if event != '':
                body = json.loads(event['Body'])
                print(f"{body['type']} - time since last event: {self.get_current_time_millis() - get_event_millis}")
                get_event_millis = self.get_current_time_millis()
                self.delete_aws_event_from_queue(event['ReceiptHandle'])
                data = body['data']
                handle_event_millis = self.get_current_time_millis()
                if body['type'] == 'battle':
                    self.handle_battle(data)
                elif body['type'] == 'wallet_poolchange':
                    self.handle_wallet_poolchange(data)
                elif body['type'] == 'wallet_newpool':
                    c.handle_wallet_newpool(data)
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
                print(f"Time it took to handle event: {self.get_current_time_millis() - handle_event_millis}")

            time.sleep(0.5)

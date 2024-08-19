#import psycopg2.extras
import urllib
import json
import psycopg2.extras
from psycopg2.extras import Json 

class PoolToolDb:

    def __init__(self):
        #self.pw = open('files/pw', 'r').read()
        self.conn = psycopg2.connect(database='pooltool',
                                    user='postgres',
                                    password='postgres',
                                    host='localhost',
                                    port='5432')

        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def update_pooltool_db(self, pool_id, number_of_subs): #TODO
        pass
        # self.cur.execute("update pools set pooltoolbot_subscribers=%s where \"pool_id\"=%s",
        #                 [number_of_subs, pool_id])
        # self.conn.commit()


    def does_rewards_addr_exist(self, addr): #TODO: Change to use current epoch
        if '\n' in addr:
            return False
        # epoch, _, _ = self.get_genesis_data_for_summary()
        # try:
        #     with urllib.request.urlopen(f"https://pegasus-pool.firebaseio.com/Mainnet/stake_history/{epoch}/S{addr[:1]}/{addr}/amount.json") as url:
        #         data = json.loads(url.read().decode())
        # except urllib.error.HTTPError as e:
        #     print(e)
        #     return False
        self.cur.execute("select stake_key from live_delegators where stake_key=lower(%s)", [addr])
        row = self.cur.fetchone()
        if row is not None:
           return True
        else:
           return False
        # if data is None:
        #     return False
        # else:
        #     return True

    def get_stake_rewards(self, addr, epoch):
        for i in range(0,3):
            while True:
                try:        
                    with urllib.request.urlopen(f"https://pegasus-pool.firebaseio.com/Mainnet/stake_history/{epoch}/S{addr[:1]}/{addr}/stakeRewards.json") as url:
                        stakeRewards = json.loads(url.read().decode())
                except Exception:
                    continue
                break
        if stakeRewards is None:
            return None
        return stakeRewards
        self.cur.execute("select stake_rewards from stake_history where stake_key=lower(%s) and epoch=%s",[addr, epoch])
        #self.cur.execute("select stake_rewards from pt_addresses_history where stake_address=lower(%s) and epoch=%s", [addr, epoch])
        row = self.cur.fetchone()
        if row is None:
           print(f"Could not get any rewards for this addr: {addr}")
           return None
        return row["stake_rewards"]
    
    def get_total_stake_rewards(self, addr, epoch): #TODO
        self.cure.execute("select sum(operator_rewards) as oprew, sum(stake_rewards) as strew from stake_history where stake_key=%s and forecast=false",[addr])
        row = self.cur.fetchone()
        if row is None:
            return 0
        else:
            return row['strew']
        # sum = 0
        # for e in range(211, epoch + 1):
        #     for i in range(0,3):
        #         while True:
        #             try:        
        #                 with urllib.request.urlopen(f"https://pegasus-pool.firebaseio.com/Mainnet/stake_history/{e}/S{addr[:1]}/{addr}/stakeRewards.json") as url:
        #                     stakeRewards = json.loads(url.read().decode())
        #             except Exception:
        #                 continue
        #             break
        #     if stakeRewards is None:
        #         stakeRewards = 0
        #         continue
        #     sum += stakeRewards
        # #self.cur.execute("select sum(operator_rewards) from pt_addresses_history where stake_address=lower(%s)", [addr])
        # #row = self.cur.fetchone()
        # #return row[0]
        # return sum
        #self.cur.execute("select sum(stake_rewards) from pt_addresses_history where stake_address=lower(%s)", [addr])
        #row = self.cur.fetchone()
        #return row[0]
    
    def get_operator_rewards(self, addr, epoch):

        # for i in range(0,3):
        #     while True:
        #         try:        
        #             with urllib.request.urlopen(f"https://pegasus-pool.firebaseio.com/Mainnet/stake_history/{epoch}/S{addr[:1]}/{addr}/operatorRewards.json") as url:
        #                 operatorRewards = json.loads(url.read().decode())
        #         except Exception:
        #             continue
        #         break
        # if operatorRewards is None:
        #     return 0
        self.cur.execute("select operator_rewards from stake_history where stake_key=lower(%s) and epoch=%s", [addr, epoch])
        row = self.cur.fetchone()
        if row is not None:
            return row['operator_rewards']
        else:
            return 0
        # return operatorRewards
    
    def get_total_operator_rewards(self, addr, epoch): #TODO
        self.cure.execute("select sum(operator_rewards) as oprew, sum(stake_rewards) as strew from stake_history where stake_key=%s and forecast=false",[addr])
        row = self.cur.fetchone()
        if row is None:
            return 0
        else:
            return row['oprew']

    def get_assigned_blocks(self, pool_id, epoch): #TODO
        return None
        #self.cur.execute("select assigned_slots from pool_epoch_data where \"poolPubKey\"=lower(%s) and epoch=%s", [pool_id, epoch])
        #row = self.cur.fetchone()
        #return row[0]
    
    def get_pool_name(self, pool_id):
        # for i in range(0,3):
        #     while True:
        #         try:        
        #             with urllib.request.urlopen(f"https://pegasus-pool.firebaseio.com/Mainnet/stake_pools/{pool_id}/n.json") as url:
        #                 name = json.loads(url.read().decode())
        #         except Exception:
        #             continue
        #         break
        self.cur.execute("select pool_name from pools where \"pool_id\"=lower(%s)", [pool_id])
        row = self.cur.fetchone()
        if row is None:
           return ''
        if row['pool_name'] is None:
            return ''
        return row['pool_name']
        # if name is None:
        #     return ''
        # return name

    #def get_block_stake_for_epoch(self, pool_id, epoch):
    #    self.cur.execute("select blockstake from pool_epoch_data where \"poolPubKey\"=lower(%s) and epoch=%s", [pool_id, epoch])
    #    row = self.cur.fetchone()
    #    return row[0]
    
    #def get_blocks_minted_for_epoch(self, pool_id, epoch):
    #    self.cur.execute("select epoch_blocks from pool_epoch_data where \"poolPubKey\"=lower(%s) and epoch=%s", [pool_id, epoch])
    #    row = self.cur.fetchone()
    #    return row[0]
    
    #def get_rewards_for_epoch(self, pool_id, epoch):
    #    self.cur.execute("select epoch_rewards from pool_epoch_data where \"poolPubKey\"=lower(%s) and epoch=%s", [pool_id, epoch])
    #    row = self.cur.fetchone()
    #    return row[0]
    
    #def get_tax_for_epoch(self, pool_id, epoch):
    #    self.cur.execute("select epoch_tax from pool_epoch_data where \"poolPubKey\"=lower(%s) and epoch=%s", [pool_id, epoch])
    #    row = self.cur.fetchone()
    #    return row[0]

    #def get_livestake(self, pool_id):
    #    self.cur.execute("select livestake from pools where \"poolPubKey\"=lower(%s)", [pool_id])
    #    row = self.cur.fetchone()
    #    return row[0]
    
    #def get_total_genesis_stake(self,):
    #    self.cur.execute("select total_staked from genesis where genesis_id=%s", [18])
    #    row = self.cur.fetchone()
    #    return row[0]
    
    #def get_current_genesis_epoch(self,):
    #    self.cur.execute("select epoch from genesis where genesis_id=%s", [18])
    #    row = self.cur.fetchone()
    #    return row[0]
    
    #def get_pool_first_epoch(self, pool_id):
    #    self.cur.execute("select first_epoch from pools where \"poolPubKey\"=%s", [pool_id])
    #    row = self.cur.fetchone()
    #    return row[0]

    #def get_pool_lifetime_reward(self, pool_id):
    #    self.cur.execute("select lifetime_rewards from pools where \"poolPubKey\"=%s", [pool_id])
    #    row = self.cur.fetchone()
    #    return row[0]
    
    #def get_pool_lifetime_stake(self, pool_id):
    #    self.cur.execute("select lifetime_stake from pools where \"poolPubKey\"=%s", [pool_id])
    #    row = self.cur.fetchone()
    #    return row[0]

    #def get_pool_donestake(self, pool_id):
    #    self.cur.execute("select donestake from pools where \"poolPubKey\"=%s", [pool_id])
    #    row = self.cur.fetchone()
    #    return row[0]
    
    #def get_pool_blockstake(self, pool_id):
    #    self.cur.execute("select blockstake from pools where \"poolPubKey\"=%s", [pool_id])
    #    row = self.cur.fetchone()
    #    return row[0]
    def is_pool_retired(self, pool_id, epoch):
        self.cur.execute("select retired,fretired,fretired_epoch from pools where pool_id=%s",[pool_id])
        row = self.cur.fetchone()
        if row is not None:
            return row['retired']
        return False

        # for i in range(0,3):
        #     while True:
        #         try:
        #             with urllib.request.urlopen(f"https://pegasus-pool.firebaseio.com/Mainnet/stake_pools/{pool_id}/retiredEpoch.json") as url:
        #                 data = json.loads(url.read().decode())
        #         except Exception:
        #             continue
        #         break

        # print(data)
        # if data is None:
        #     return False
        # #self.cur.execute("select operator_rewards from pt_addresses_history where stake_address=lower(%s) and epoch=%s", [addr, epoch])
        # #row = self.cur.fetchone()
        # #return row[0]
        # return epoch > data

    # def get_total_delegators(self, pool_id, epoch): # NOTE: not used
    #     self.cur.execute("select delegator_count from pools where \"pool_id\"=%s", [pool_id, epoch])
    #     row = self.cur.fetchone()

    #     for i in range(0,3):
    #         while True:
    #             try:        
    #                 with urllib.request.urlopen(f"https://pegasus-pool.firebaseio.com/Mainnet/stake_pools/{pool_id}.json") as url:
    #                     data = json.loads(url.read().decode())
    #             except Exception:
    #                 continue
    #             break
        
    #     #self.cur.execute("select count(distinct \"stake_address\") from pt_addresses_history where \"poolid\"=%s and epoch=%s", [pool_id, epoch])
    #     #row = self.cur.fetchone()
    #     delegators = data['delegators'] if 'delegators' in data else 0
    #     return delegators
    
    #def get_forecasted_tax_reward(self, pool_id, epoch):
    #    self.cur.execute("select fcp1_epoch_tax, fcp1_epoch_rewards from pool_epoch_data where \"poolPubKey\"=%s and epoch=%s", [pool_id, epoch])
    #    row = self.cur.fetchone()
    #    return row['fcp1_epoch_tax'], row['fcp1_epoch_rewards']

    # def get_pools_data_for_summary(self, pool_id): # NOTE: not used
    #     for i in range(0,3):
    #         while True:
    #             try:        
    #                 with urllib.request.urlopen(f"https://pegasus-pool.firebaseio.com/Mainnet/stake_pools/{pool_id}.json") as url:
    #                     data = json.loads(url.read().decode())
    #             except Exception:
    #                 continue
    #             break
    #     #self.cur.execute("select livestake, first_epoch, lifetime_rewards, lifetime_stake, donestake, blockstake from pools where \"poolPubKey\"=lower(%s)", [pool_id])
    #     #row = self.cur.fetchone()
    #     #return row['livestake'], row['first_epoch'], int(row['lifetime_rewards']), int(row['lifetime_stake']), row['donestake'], row['blockstake']
    #     livestake = data['liveStake'] if 'liveStake' in data else 0
    #     first_epoch = data['firstEpoch'] if 'firstEpoch' in data else 0
    #     lifetime_rewards = data['lifetimeRewards'] if 'lifetimeRewrads' in data else 0
    #     lifetime_stake = data['lifetimeStake'] if 'lifetimeStake' in data else 0
    #     done_stake = data['doneStake'] if 'doneStake' in data else 0
    #     active_stake = data['activeStake'] if 'activeStake' in data else 0
    #     return livestake, first_epoch, lifetime_rewards, lifetime_stake, done_stake, active_stake

    # def get_pool_epoch_data_for_summary(self, pool_id, epoch): # NOTE: not used
    #     for i in range(0,3):
    #         while True:
    #             try:        
    #                 with urllib.request.urlopen(f"https://pegasus-pool.firebaseio.com/Mainnet/pool_stats/{pool_id}/stake/{epoch}.json") as url:
    #                     blockstake = json.loads(url.read().decode())
    #                 with urllib.request.urlopen(f"https://pegasus-pool.firebaseio.com/Mainnet/pool_stats/{pool_id}/blocks/{epoch}.json") as url:
    #                     epoch_blocks = json.loads(url.read().decode())
    #                 with urllib.request.urlopen(f"https://pegasus-pool.firebaseio.com/Mainnet/pool_stats/{pool_id}/delegators_rewards/{epoch + 1}.json") as url:
    #                     rewards = json.loads(url.read().decode())
    #                 with urllib.request.urlopen(f"https://pegasus-pool.firebaseio.com/Mainnet/pool_stats/{pool_id}/pool_fees/{epoch + 1}.json") as url:
    #                     tax = json.loads(url.read().decode())
    #             except Exception:
    #                 continue
    #             break
    #     #try:
    #     #    self.cur.execute("select blockstake, epoch_blocks, fcp1_epoch_tax, fcp1_epoch_rewards from pool_epoch_data where \"poolPubKey\"=%s and epoch=%s", [pool_id, epoch])
    #     #    row = self.cur.fetchone()
    #     #    return row['blockstake'], row['epoch_blocks'], row['fcp1_epoch_tax'], row['fcp1_epoch_rewards']
    #     #except:
    #     #    print(f"Could not get epoch data for pool; {pool_id}")
    #     #    return -1, -1, -1, -1
    #     return blockstake, epoch_blocks, tax, rewards
    
    # def get_pool_epoch_rewards(self, pool_id, epoch): # NOTE: not used
    #     for i in range(0,3):
    #         while True:
    #             try:        
    #                 with urllib.request.urlopen(f"https://pegasus-pool.firebaseio.com/Mainnet/pool_stats/{pool_id}/delegators_rewards/{epoch}.json") as url:
    #                     rewards = json.loads(url.read().decode())
    #                 with urllib.request.urlopen(f"https://pegasus-pool.firebaseio.com/Mainnet/pool_stats/{pool_id}/pool_fees/{epoch}.json") as url:
    #                     tax = json.loads(url.read().decode())
    #             except Exception:
    #                 continue
    #             break
    #     #self.cur.execute("select epoch_rewards, epoch_tax from pool_epoch_data where \"poolPubKey\"=%s and epoch=%s", [pool_id, epoch])
    #     #row = self.cur.fetchone()
    #     return rewards, tax
    
    def get_genesis_data_for_summary(self,):
        self.cur.execute("select current_epoch as epoch, total_staked, reserves from ecosystem where id=1")
        row=self.cur.fetchone()
        # for i in range(0,3):
        #     while True:
        #         try:        
        #             with urllib.request.urlopen("https://pegasus-pool.firebaseio.com/Mainnet/ecosystem.json") as url:
        #                 ecosystem = json.loads(url.read().decode())
        #             with urllib.request.urlopen("https://pegasus-pool.firebaseio.com/Mainnet/recent_block.json") as url:
        #                 recent_block = json.loads(url.read().decode())
        #         except Exception:
        #             continue
        #         break
        #self.cur.execute("select epoch, total_staked from genesis where genesis_id=%s", [18])
        #row = self.cur.fetchone()
        return row['epoch'], row['total_staked'], row['reserves']
    
    def get_reward_data(self, epoch):
        # data={}
        # self.cur.execute("select pool_id, value from pool_fees where epoch=%s",[epoch])
        # row=self.cur.fetchone()
        # while row:
        #     if row['pool_id'] not in data:
        #         data[row['pool_id']]={"f":false,"epochRos":0,"epochTax":0,"epochRewards":0}
        #     data[row['pool_id']]['epochTax']=row['value']
        #     row=self.cur.fetchone()
        
        # self.cur.execute("select pool_id, value from pool_delegators_rewards where epoch=%s",[epoch])
        # row=self.cur.fetchone()
        # while row:
        #     if row['pool_id'] not in data:
        #         data[row['pool_id']]={"f":false,"epochRos":0,"epochTax":0,"epochRewards":0}
        #     data[row['pool_id']]['epochRewards']=row['value']
        #     row=self.cur.fetchone()
        
        # self.cur.execute("select pool_id, value from pool_ros where epoch=%s",[epoch])
        # row=self.cur.fetchone()
        # while row:
        #     if row['pool_id'] not in data:
        #         data[row['pool_id']]={"f":false,"epochRos":0,"epochTax":0,"epochRewards":0}
        #     data[row['pool_id']]['epochRos']=row['ros']
        #     row=self.cur.fetchone()

        for i in range(0,3):
            while True:
                try:        
                    with urllib.request.urlopen(f"https://s3-us-west-2.amazonaws.com/data.pooltool.io/Mainnet/stake_pool_columns/{epoch}/rewards.json") as url:
                        data = json.loads(url.read().decode())
                except Exception:
                    continue
                break
        if data is None:
            return 0
        #self.cur.execute("select operator_rewards from pt_addresses_history where stake_address=lower(%s) and epoch=%s", [addr, epoch])
        #row = self.cur.fetchone()
        #return row[0]
        return data

    def get_stake_data(self, epoch):
        for i in range(0,3):
            while True:
                try:        
                    with urllib.request.urlopen(f"https://s3-us-west-2.amazonaws.com/data.pooltool.io/Mainnet/stake_pool_columns/{epoch}/stake.json") as url:
                        data = json.loads(url.read().decode())
                except Exception:
                    continue
                break

        if data is None:
            return 0
        #self.cur.execute("select operator_rewards from pt_addresses_history where stake_address=lower(%s) and epoch=%s", [addr, epoch])
        #row = self.cur.fetchone()
        #return row[0]
        return data    

    def get_pool_stats(self, addr, epoch):
        self.cur.execute("select delegator_count from pools where pool_id=%s",[addr])
        row=self.cur.fetchone()
        if row:
            delegators = row['delegator_count']

            self.cur.execute("select block_count from pool_epoch_blocks where pool_id=%s and epoch=%s",[addr,epoch])
            row=self.cur.fetchone()
            if row:
                epoch_blocks=row['block_count']
            else:
                epoch_blocks=0
            
            self.cur.execute("select slots from pool_assigned_slots where pool_id=%s and epoch=%s",[addr,epoch])
            row=self.cur.fetchone()
            if row:
                assigned_slots=row['slots']
            else:
                assigned_slots=0    
            
        # for i in range(0,3):
        #     while True:
        #         try:
        #             with urllib.request.urlopen(f"https://pegasus-pool.firebaseio.com/Mainnet/pool_stats/{addr}.json") as url:
        #                 data = json.loads(url.read().decode())
        #         except Exception:
        #             continue
        #         break
        # if data is None:
        #     return 0
        # #self.cur.execute("select operator_rewards from pt_addresses_history where stake_address=lower(%s) and epoch=%s", [addr, epoch])
        # #row = self.cur.fetchone()
        # #return row[0]
        # if 'assigned_slots' in data:
        #     if str(epoch) in data['assigned_slots']:
        #         if 'slots' in data['assigned_slots'][str(epoch)]:
        #             assigned_slots = data['assigned_slots'][str(epoch)]['slots']
        #         else:
        #             assigned_slots = 0
        #     else:
        #         assigned_slots = 0
        # else:
        #     assigned_slots = 0

        # if 'blocks' in data:
        #     if str(epoch) in data['blocks']:
        #         epoch_blocks = data['blocks'][str(epoch)]
        #     else:
        #         epoch_blocks = 0
        # else:
        #     epoch_blocks = 0

        # if 'delegatorCount' in data:
        #     delegators = data['delegatorCount']
        # else:
        #     delegators = 0

        return epoch_blocks, delegators, assigned_slots

    def get_livestake(self, addr):
        self.cur.execute("select live_stake from pools where pool_id=%s",[addr])
        row=self.cur.fetchone()
        if row is None:
            return 0
        else:
            return row['live_stake']
        # for i in range(0,3):
        #     while True:
        #         try:
        #             with urllib.request.urlopen(f"https://pegasus-pool.firebaseio.com/Mainnet/stake_pools/{addr}/ls.json") as url:
        #                 livestake = json.loads(url.read().decode())
        #         except Exception:
        #             continue
        #         break

        # if livestake is None:
        #     return 0
        # #self.cur.execute("select operator_rewards from pt_addresses_history where stake_address=lower(%s) and epoch=%s", [addr, epoch])
        # #row = self.cur.fetchone()
        # #return row[0]
        return livestake
        
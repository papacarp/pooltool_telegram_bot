import psycopg2.extras


class PoolToolDb:

    def __init__(self):
        self.pw = open('files/pw', 'r').read()
        self.conn = psycopg2.connect(database='postgres',
                                     user='postgres',
                                     password=self.pw,
                                     host='pooltool2.cepwjus5jmyc.us-west-2.rds.amazonaws.com',
                                     port='5432')
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def update_pooltool_db(self, pool_id, number_of_subs):
        self.cur.execute("update pools set pooltoolbot_subscribers=%s where \"poolPubKey\"=%s",
                         [number_of_subs, pool_id])
        self.conn.commit()

    def does_rewards_addr_exist(self, addr):
        self.cur.execute("select count(*) from pt_addresses_history where stake_address=lower(%s)", [addr])
        row = self.cur.fetchone()
        if row[0] > 0:
            return True
        else:
            return False

    def get_stake_rewards(self, addr, epoch):
        self.cur.execute("select stake_rewards from pt_addresses_history where stake_address=lower(%s) and epoch=%s", [addr, epoch])
        row = self.cur.fetchone()
        return row[0]
    
    def get_total_stake_rewards(self, addr):
        self.cur.execute("select sum(stake_rewards) from pt_addresses_history where stake_address=lower(%s)", [addr])
        row = self.cur.fetchone()
        return row[0]
    
    def get_operator_rewards(self, addr, epoch):
        self.cur.execute("select operator_rewards from pt_addresses_history where stake_address=lower(%s) and epoch=%s", [addr, epoch])
        row = self.cur.fetchone()
        return row[0]
    
    def get_total_operator_rewards(self, addr):
        self.cur.execute("select sum(operator_rewards) from pt_addresses_history where stake_address=lower(%s)", [addr])
        row = self.cur.fetchone()
        return row[0]

    def get_total_stake(self, addr):
        self.cur.execute("select sum(stake) from pt_addresses_history where stake_address=lower(%s)", [addr])
        row = self.cur.fetchone()
        return row[0]

    def get_block_stake_for_epoch(self, pool_id, epoch):
        self.cur.execute("select blockstake from pool_epoch_data where \"poolPubKey\"=lower(%s) and epoch=%s", [pool_id, epoch])
        row = self.cur.fetchone()
        return row[0]
    
    def get_blocks_minted_for_epoch(self, pool_id, epoch):
        self.cur.execute("select epoch_blocks from pool_epoch_data where \"poolPubKey\"=lower(%s) and epoch=%s", [pool_id, epoch])
        row = self.cur.fetchone()
        return row[0]
    
    def get_rewards_for_epoch(self, pool_id, epoch):
        self.cur.execute("select epoch_rewards from pool_epoch_data where \"poolPubKey\"=lower(%s) and epoch=%s", [pool_id, epoch])
        row = self.cur.fetchone()
        return row[0]
    
    def get_tax_for_epoch(self, pool_id, epoch):
        self.cur.execute("select epoch_tax from pool_epoch_data where \"poolPubKey\"=lower(%s) and epoch=%s", [pool_id, epoch])
        row = self.cur.fetchone()
        return row[0]

    def get_livestake(self, pool_id):
        self.cur.execute("select livestake from pools where \"poolPubKey\"=lower(%s)", [pool_id])
        row = self.cur.fetchone()
        return row[0]
    
    def get_total_genesis_stake(self,):
        self.cur.execute("select total_staked from genesis where genesis_id=%s", [18])
        row = self.cur.fetchone()
        return row[0]
    
    def get_current_genesis_epoch(self,):
        self.cur.execute("select epoch from genesis where genesis_id=%s", [18])
        row = self.cur.fetchone()
        return row[0]
    
    def get_pool_first_epoch(self, pool_id):
        self.cur.execute("select first_epoch from pools where \"poolPubKey\"=%s", [pool_id])
        row = self.cur.fetchone()
        return row[0]

    def get_pool_lifetime_reward(self, pool_id):
        self.cur.execute("select lifetime_rewards from pools where \"poolPubKey\"=%s", [pool_id])
        row = self.cur.fetchone()
        return row[0]
    
    def get_pool_lifetime_stake(self, pool_id):
        self.cur.execute("select lifetime_stake from pools where \"poolPubKey\"=%s", [pool_id])
        row = self.cur.fetchone()
        return row[0]

    def get_pool_donestake(self, pool_id):
        self.cur.execute("select donestake from pools where \"poolPubKey\"=%s", [pool_id])
        row = self.cur.fetchone()
        return row[0]
    
    def get_pool_blockstake(self, pool_id):
        self.cur.execute("select blockstake from pools where \"poolPubKey\"=%s", [pool_id])
        row = self.cur.fetchone()
        return row[0]
    
    def get_total_delegators(self, pool_id, epoch):
        self.cur.execute("select count(distinct \"stake_address\") from pt_addresses_history where \"poolid\"=%s and epoch=%s", [pool_id, epoch])
        row = self.cur.fetchone()
        return row[0]
    
    def get_forecasted_tax_reward(self, pool_id, epoch):
        self.cur.execute("select fcp1_epoch_tax, fcp1_epoch_rewards from pool_epoch_data where \"poolPubKey\"=%s and epoch=%s", [pool_id, epoch])
        row = self.cur.fetchone()
        return row['fcp1_epoch_tax'], row['fcp1_epoch_rewards']

    def get_pools_data_for_summary(self, pool_id):
        self.cur.execute("select livestake, first_epoch, lifetime_rewards, lifetime_stake, donestake, blockstake from pools where \"poolPubKey\"=lower(%s)", [pool_id])
        row = self.cur.fetchone()
        return row['livestake'], row['first_epoch'], int(row['lifetime_rewards']), int(row['lifetime_stake']), row['donestake'], row['blockstake']

    def get_pool_epoch_data_for_summary(self, pool_id, epoch):
        try:
            self.cur.execute("select blockstake, epoch_blocks, fcp1_epoch_tax, fcp1_epoch_rewards from pool_epoch_data where \"poolPubKey\"=%s and epoch=%s", [pool_id, epoch])
            row = self.cur.fetchone()
            return row['blockstake'], row['epoch_blocks'], row['fcp1_epoch_tax'], row['fcp1_epoch_rewards']
        except:
            print(f"Could not get epoch data for pool; {pool_id}")
            return -1, -1, -1, -1
    
    def get_pool_epoch_rewards(self, pool_id, epoch):
        self.cur.execute("select epoch_rewards, epoch_tax from pool_epoch_data where \"poolPubKey\"=%s and epoch=%s", [pool_id, epoch])
        row = self.cur.fetchone()
        return row['epoch_rewards'], row['epoch_tax']
    
    def get_genesis_data_for_summary(self,):
        self.cur.execute("select epoch, total_staked from genesis where genesis_id=%s", [18])
        row = self.cur.fetchone()
        return row['epoch'], row['total_staked']
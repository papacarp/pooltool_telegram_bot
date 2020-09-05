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
        self.cur.execute("select count(*) from pt_addresses_history where stake_address=%s", [addr])
        row = self.cur.fetchone()
        print(f"{addr}, {row[0]}")
        if row[0] > 0:
            return True
        else:
            return False

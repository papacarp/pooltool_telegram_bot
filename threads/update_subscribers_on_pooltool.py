import psycopg2.extras


class UpdateSubscribers:
    def __init__(self, db):
        self.db = db
        self.pw = open('files/pw', 'r').read()
        self.conn = psycopg2.connect(database='postgres',
                                     user='postgres',
                                     password=self.pw,
                                     host='pooltool2.cepwjus5jmyc.us-west-2.rds.amazonaws.com',
                                     port='5432')
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def update_pooltool_db(self, pool_id, number_of_subs):
        self.cur.execute("update pools set pooltoolbot_subscribers=%s where \"poolPubKey\"=%s", [number_of_subs,pool_id])
        self.conn.commit()

    def run(self):
        pool_ids = self.db.get_all_subscribed_pool()

        for pool_id in pool_ids:
            number_of_subscribers = len(self.db.get_chat_ids_from_pool_id(pool_id))
            self.update_pooltool_db(pool_id, number_of_subscribers)

        print("Subscribers updated to pooltool")
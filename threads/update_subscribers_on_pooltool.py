from modules import pooltool_dbhelper


class UpdateSubscribers:
    def __init__(self, db):
        self.db = db

    def run(self):
        pool_ids = self.db.get_all_subscribed_pool()

        ptdb = pooltool_dbhelper.PoolToolDb()

        for pool_id in pool_ids:
            number_of_subscribers = len(self.db.get_chat_ids_from_pool_id(pool_id))
            ptdb.update_pooltool_db(pool_id, number_of_subscribers)
            
        #print("Subscribers updated to pooltool")
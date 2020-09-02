import pprint, subprocess, platform, requests
from os import environ
import json
from datetime import datetime, timedelta

genesis_id = 18
import psycopg2
import psycopg2.extras
from psycopg2.extras import Json

pw = open('files/pw', 'r').read()

conn = psycopg2.connect(database='postgres',
                        user='postgres',
                        password=pw,
                        host='pooltool2.cepwjus5jmyc.us-west-2.rds.amazonaws.com',
                        port='5432')

cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

block_production_epoch = 212

cur.execute("select poolid, blocks from blocks_by_epoch_by_leader where epoch_no=%s", [block_production_epoch])
row = cur.fetchone()
blocksbypool = {}
while row:
    blocksbypool[row['poolid']] = row['blocks']
    row = cur.fetchone()
    print(row['blocks'])

import logging
import os
import time
from datetime import datetime

import mysql.connector

PROCESSLIST_SQL = """
SELECT
  id,
  user,
  host,
  db,
  command,
  state,
  info statement,
  time_ms,
  rows_sent,
  rows_examined,
  UNIX_TIMESTAMP(t.trx_started) trx_utime
FROM information_schema.processlist p, information_schema.innodb_trx t
WHERE p.id = t.trx_mysql_thread_id AND command NOT IN ('Sleep','Daemon','Binlog Dump') AND time_ms >= 1000
"""


def parser_dsn(dsn: str):
    userpass, addrdb, = dsn.split('@')
    user, pswd = userpass.split('/')
    db_addr, _ = (addrdb if addrdb.endswith('/') else addrdb + '/').split('/')
    host, port = db_addr.split(':')
    return {'host': host, 'port': port, 'user': user, 'password': pswd}


class ProcesslistDB:
    def __init__(self, dbconfig):
        self.dbconfig = dbconfig
        self.con = mysql.connector.connect(**self.dbconfig)
        self.query_addr()

    def query_addr(self):
        with self.con.cursor() as cur:
            cur.execute("select concat(@@hostname, ':', @@port)")
            self.db_addr = cur.fetchone()[0]

    def query_processlist(self):
        with self.con.cursor() as cur:
            cur.execute(PROCESSLIST_SQL)
            row_headers = [x[0] for x in cur.description]
            for result in cur.fetchall():
                yield dict(zip(row_headers, result))


class TempDB:
    def __init__(self, temp_dbconfig):
        self.dbconfig = temp_dbconfig
        self.con = mysql.connector.connect(**self.dbconfig)
        self.con.autocommit = True
        self.create_table()

    def create_table(self):
        with self.con.cursor()as cur:
            cur.execute("CREATE DATABASE IF NOT EXISTS temp")

        with self.con.cursor()as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS temp.processlist(
                    db_addr varchar(40),
                    timestamp TIMESTAMP NOT NULL,
                    command varchar(16),
                    db varchar(64),
                    host varchar(261),
                    id INTEGER NOT NULL,
                    rows_examined INTEGER NOT NULL,
                    rows_sent INTEGER NOT NULL,
                    state varchar(64),
                    statement TEXT,
                    time_ms INTEGER NOT NULL,
                    trx_utime INTEGER NOT NULL,
                    user varchar(32),
                    update_cnt INTEGER default 0,
                    PRIMARY KEY(db_addr, id, trx_utime),
                    index(db_addr,timestamp)
                )
                """)

    def insert_process(self, value):
        v = value
        with self.con.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO temp.processlist(db_addr, timestamp, command, db, host, id, rows_examined, rows_sent, state, statement, time_ms, trx_utime, user)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE time_ms={v['time_ms']}, update_cnt=update_cnt+1 
                """,
                (v['db_addr'], v['timestamp'], v['command'], v['db'], v['host'], v['id'], v['rows_examined'], v['rows_sent'], v['state'], v['statement'], v['time_ms'], v['trx_utime'], v['user']))


if __name__ == "__main__":
    MON_DB_DSN = os.getenv('MON_DB_DSN')
    TEMP_DB_DSN = os.getenv('TEMP_DB_DSN')

    temp_db = TempDB(parser_dsn(TEMP_DB_DSN))
    processlist_db = ProcesslistDB(parser_dsn(MON_DB_DSN))

    logging.basicConfig(
        format=f'%(asctime)s %(levelname)s {processlist_db.db_addr} %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    while True:
        ts = datetime.now()
        query_ts = ts.replace(second=0, microsecond=0)
        query_strts = query_ts.strftime('%Y-%m-%dT%H:%M:%S')

        cnt = 0
        for pl in processlist_db.query_processlist():
            pl['timestamp'] = query_strts
            pl['db_addr'] = processlist_db.db_addr
            temp_db.insert_process(pl)
            cnt += 1
        logging.info(f"processlist number: {cnt}, timestamp: {ts.strftime('%Y-%m-%dT%H:%M:%S')}")

        time.sleep(1)

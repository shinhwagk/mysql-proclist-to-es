from threading import Thread
import os
import sys
import time
from datetime import datetime, timedelta

import mysql.connector
from elasticsearch import Elasticsearch, helpers

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
WHERE p.id = t.trx_mysql_thread_id AND command NOT IN ('Sleep','Daemon','Binlog Dump') AND time_ms >= 10
"""


def parser_dsn(dsn: str):
    userpass, addrdb, = dsn.split('@')
    user, pswd = userpass.split('/')
    db_addr, database = (addrdb if addrdb.endswith('/') else addrdb + '/').split('/')
    host, port = db_addr.split(':')
    return {'host': host, 'port': port, 'user': user, 'password': pswd}


class ProcesslistDB:
    def __init__(self, dbconfig):
        self.dbconfig = dbconfig
        self.con = mysql.connector.connect(**self.dbconfig)
        self.query_addr()

    def query_addr(self):
        with mysql.connector.connect(**self.dbconfig) as con:
            with con.cursor() as cur:
                cur.execute("select concat(@@hostname, ':', @@port)")
                self.db_addr = cur.fetchone()[0]

    def query_processlist(self):
        with self.con.cursor() as cur:
            cur.execute(PROCESSLIST_SQL)
            row_headers = [x[0] for x in cur.description]
            for result in cur.fetchall():
                yield dict(zip(row_headers, result))


class InnerTempDB:

    def __init__(self, inner_dbconfig):
        self.dbconfig = inner_dbconfig
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

    @staticmethod
    def delete_process(dbconfig, db_addr: str, timestamp: datetime):
        print("delete")
        with mysql.connector.connect(**dbconfig) as con:
            con.autocommit = True
            with con.cursor() as cur:
                cur.execute("DELETE FROM temp.processlist WHERE db_addr = %s AND timestamp <= %s", (db_addr, timestamp))
        print("delete down.")

    def check_process_exist(self, db_addr: str, timestamp: datetime):
        with self.con.cursor() as cur:
            cur.execute("SELECT count(*) FROM temp.processlist WHERE db_addr = %s AND timestamp <= %s", (db_addr, timestamp))
            return cur.fetchone()[0]

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

    def query_process(self, db_addr: str, timestamp: datetime):
        with self.con.cursor()as cur:
            cur.execute("SELECT * FROM temp.processlist WHERE db_addr = %s AND timestamp <= %s", (db_addr, timestamp))
            row_headers = [x[0] for x in cur.description]
            for result in cur.fetchall():
                yield dict(zip(row_headers, result))


if __name__ == "__main__":
    MYSQL_DSN = os.getenv('MYSQL_DSN')
    INNER_DB_DSN = os.getenv('INNER_DB_DSN')
    ELASTICSEARCH_ADDR = os.getenv('ELASTICSEARCH_ADDR')

    es = Elasticsearch(f"http://{ELASTICSEARCH_ADDR}")

    innser_temp_db = InnerTempDB(parser_dsn(INNER_DB_DSN))
    processlist_db = ProcesslistDB(parser_dsn(MYSQL_DSN))

    print(processlist_db.db_addr)

    while True:
        _now = datetime.now()
        _today = _now.strftime('%Y-%m-%d')
        _es_index_today_name = f"mysql-processlist-{_today}"

        print("query processlist", _now)

        last_timestamp = datetime.now()
        ts = _now.replace(second=0, microsecond=0)
        ts_strftime = ts.strftime('%Y-%m-%dT%H:%M:00')

        cnt = 0
        for pl in processlist_db.query_processlist():
            pl['timestamp'] = ts_strftime
            pl['db_addr'] = processlist_db.db_addr
            innser_temp_db.insert_process(pl)
            cnt += 1
        print(f"processlit {cnt}")
        cnt = 0

        last_ts = ts - timedelta(minutes=1)
        if innser_temp_db.check_process_exist(processlist_db.db_addr, last_ts) >= 1:
            for doc in innser_temp_db.query_process(processlist_db.db_addr, last_ts):
                doc['@timestamp'] = doc['timestamp']
                del doc['timestamp']
                resp = es.index(index=_es_index_today_name, document=doc)
                cnt += 1

            print(f"push es {cnt}")
            print(f"{last_ts}")

            # Thread(target=InnerTempDB.delete_process, args=(innser_temp_db.dbconfig, processlist_db.db_addr, last_ts)).run()
        time.sleep(1)

import os
import time
from datetime import datetime, timedelta

import mysql.connector
from elasticsearch import Elasticsearch

import logging

logging.basicConfig(
    format=f'%(asctime)s %(levelname)s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)


def parser_dsn(dsn: str):
    userpass, addrdb, = dsn.split('@')
    user, pswd = userpass.split('/')
    db_addr, _ = (addrdb if addrdb.endswith('/') else addrdb + '/').split('/')
    host, port = db_addr.split(':')
    return {'host': host, 'port': port, 'user': user, 'password': pswd}


class TempDB:
    def __init__(self, temp_dbconfig):
        self.dbconfig = temp_dbconfig
        self.con = mysql.connector.connect(**self.dbconfig)
        self.con.autocommit = True
        self.digest_con = mysql.connector.connect(**self.dbconfig, database="digest")
        self.create_table()

    def create_table(self):
        with self.con.cursor()as cur:
            cur.execute("CREATE DATABASE IF NOT EXISTS digest")

    def query_process(self, timestamp: datetime):
        with self.con.cursor()as cur:
            cur.execute("SELECT * FROM temp.processlist WHERE timestamp < %s", (timestamp,))
            row_headers = [x[0] for x in cur.description]
            for result in cur.fetchall():
                yield dict(zip(row_headers, result))

    def query_sqltext_digest(self, sqltext: str):
        with self.digest_con.cursor()as cur:
            cur.execute("SELECT STATEMENT_DIGEST(%s), STATEMENT_DIGEST_TEXT(%s)", (sqltext, sqltext,))
            return cur.fetchone()

    def clear_processlist(self, timestamp: datetime):
        with self.con.cursor() as cur:
            cur.execute("DELETE FROM temp.processlist WHERE timestamp < %s", (timestamp,))
            return cur.fetchone()


if __name__ == "__main__":
    TEMP_DB_DSN = os.getenv('TEMP_DB_DSN')
    ELASTICSEARCH_ADDR = os.getenv('ELASTICSEARCH_ADDR')

    elastic = Elasticsearch(f"http://{ELASTICSEARCH_ADDR}")

    temp_db = TempDB(parser_dsn(TEMP_DB_DSN))

    while True:
        _now = datetime.now()

        ts = _now.replace(second=0, microsecond=0)
        ts_strftime = ts.strftime('%Y-%m-%dT%H:%M:%S')

        query_timestamp = ts - timedelta(minutes=2)

        cnt = 0
        for doc in temp_db.query_process(query_timestamp):
            _timestamp = doc['timestamp']
            del doc['timestamp']
            doc['@timestamp'] = _timestamp

            try:
                digest, digest_text = temp_db.query_sqltext_digest(doc['statement'])
                doc['digest'] = digest
                doc['digest_text'] = digest_text
            except Exception as e:
                logging.error(f'digest error: {e}, statement: {doc}.')
                doc['digest'] = str(e)
                doc['digest_text'] = str(e)
            es_index_today_name = f"mysql-processlist-{_timestamp.strftime('%Y-%m-%d')}"
            resp = elastic.index(index=es_index_today_name, document=doc)
            cnt += 1

        del_rows = temp_db.clear_processlist(query_timestamp)
        logging.info(f"delete row: {del_rows}")

        time.sleep(60)

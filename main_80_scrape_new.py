import os
import time
from datetime import datetime, timezone

import mysql.connector
from elasticsearch import Elasticsearch


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
    (
        userpass,
        addrdb,
    ) = dsn.split("@")
    user, pswd = userpass.split("/")
    db_addr, _ = (addrdb if addrdb.endswith("/") else addrdb + "/").split("/")
    host, port = db_addr.split(":")
    return {"host": host, "port": port, "user": user, "password": pswd}


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

    def query_sqltext_digest(self, sqltext: str):
        with self.digest_con.cursor() as cur:
            cur.execute(
                "SELECT STATEMENT_DIGEST(%s), STATEMENT_DIGEST_TEXT(%s)",
                (sqltext, sqltext),
            )
            return cur.fetchone()


if __name__ == "__main__":
    MON_DB_DSN = os.getenv("MON_DB_DSN")
    ELASTICSEARCH_ADDR = os.getenv("ELASTICSEARCH_ADDR")

    processlist_db = ProcesslistDB(parser_dsn(MON_DB_DSN))
    elastic = Elasticsearch(f"http://{ELASTICSEARCH_ADDR}")

    while True:
        cnt = 0

        ts = datetime.now()
        utcts = ts.astimezone(timezone.utc)
        utcts = utcts.replace(second=0, microsecond=0)
        # query_strts = query_ts.strftime('%Y-%m-%dT%H:%M:%S')

        es_index_today_name = f"mysql-processlist-{ts.strftime('%Y-%m-%d')}"

        for pl in processlist_db.query_processlist():
            pl["@timestamp"] = utcts.isoformat() + "Z"
            pl["db_addr"] = processlist_db.db_addr

            try:
                digest, digest_text = processlist_db.query_sqltext_digest(pl["statement"])
                pl["digest"] = digest
                pl["digest_text"] = digest_text
            except Exception as e:
                pl["digest"] = str(e)
                pl["digest_text"] = str(e)

            resp = elastic.index(index=es_index_today_name, document=pl)
            cnt += 1

        print(f"processlist number: {cnt}, timestamp: {ts.strftime('%Y-%m-%dT%H:%M:%S')}")

        time.sleep(1)

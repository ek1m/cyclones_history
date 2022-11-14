"""Database client."""
import logging
import psycopg2


class Database:
    """PostgreSQL Database class."""

    def __init__(self, host=None, user=None, password=None, port=None, dbname=None):
        self.host = host
        self.username = user
        self.password = password
        self.port = port
        self.dbname = dbname
        self.conn = None

    def connect(self):
        """Connect to a Postgres database."""
        if self.conn is None:
            try:
                self.conn = psycopg2.connect(
                    host=self.host,
                    user=self.username,
                    password=self.password,
                    port=self.port,
                    dbname=self.dbname
                )
            except psycopg2.DatabaseError as e:
                logging.error(e)
                raise e
            finally:
                logging.info('Connection opened successfully.')

    def exec(self, sql):
        """Execution query"""
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute(sql)
            self.conn.commit()
        cur.close()
        return f"{cur.rowcount} rows affected."

    def copy_expert(self, sql, filename):
        """Executes SQL using psycopg2 copy_expert method"""
        import os
        logging.info("Running copy expert: %s, filename: %s", sql, filename)
        if not os.path.isfile(filename):
            open(filename, 'w')

        self.connect()
        with open(filename, 'r+') as file:
            with self.conn.cursor() as cur:
                cur.copy_expert(sql, file)
                file.truncate(file.tell())
                self.conn.commit()
            cur.close()

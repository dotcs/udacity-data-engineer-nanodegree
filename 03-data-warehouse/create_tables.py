import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import logging
import sys

logger = logging.getLogger('create_tables')


def drop_tables(cur, conn):
    """
    Drop all tables in AWS Redshift if they existed previously.
    
    :param cur: Database cursor
    :param conn: Database connection
    """
    logger.debug("Drop tables ...")
    for query in drop_table_queries:
        logger.debug(f'RUN query: {query}')
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create all tables in AWS Redshift.
    
    :param cur: Database cursor
    :param conn: Database connection
    """
    logger.debug("Create tables ...")
    for query in create_table_queries:
        logger.debug(f'RUN query: {query}')
        cur.execute(query)
        conn.commit()


def main():
    """
    Main program.
    """
    handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import logging
import sys

logger = logging.getLogger('etl')

def load_staging_tables(cur, conn):
    """
    Load data into the AWS Redshift staging tables.
    
    :param cur: Database cursor
    :param conn: Database connection
    """
    logger.debug("Load staging tables ...")
    for query in copy_table_queries:
        logger.debug(f'RUN query: {query}')
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data into AWS Redshift Data Warehouse tables.
    
    :param cur: Database cursor
    :param conn: Database connection
    """
    logger.debug("Insert data into warehouse ...")
    for query in insert_table_queries:
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
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
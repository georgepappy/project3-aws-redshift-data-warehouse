import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Copies log data/metadata JSON files from S3 to the staging tables 
        using the queries in `copy_table_queries` list.
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Transforms and inserts data from the staging tables into the fact
        and dimension tables
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    - Reads cluster parameters from 'dwh.cfg' and establishes connection
        to specified AWS Redshift cluster.
    
    - Copies log data/metadata JSON files from S3 to the staging tables 
        using the queries in `copy_table_queries` list.  
    
    - Transforms and inserts data from the staging tables into the fact
        and dimension tables
    
    - Finally, closes the connection.
    
    - NOTE: create_tables.py must be run prior to running this script
    '''
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
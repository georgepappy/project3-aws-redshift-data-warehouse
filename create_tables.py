import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    Drops each table (if it exists) using the queries in `drop_table_queries` list.
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Creates each table (2 staging, 1 fact, 4 dimenson) using the queries in `create_table_queries` list.
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    - Reads cluster parameters from 'dwh.cfg' and establishes connection
          to specified AWS Redshift cluster.
    
    - Drops all the tables (if they exist).  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection.
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
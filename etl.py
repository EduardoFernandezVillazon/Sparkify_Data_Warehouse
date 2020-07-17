import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function runs the query to copy all the input data into temporary staging tables from which we will perform operations while inserting the data into the final schema.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function runs the query to inster all the data from the staging tables while performing operations on the data: cleaning data, handling duplicates, etc.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    The main function of this script reads the cfg file and extracts from it the necessary information to connect to the database 'dbname' in the Redshift cluster. It then creates a cursor for this database and calls the functions load the data into staging tables to then insert them into the final tables of our schema.
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
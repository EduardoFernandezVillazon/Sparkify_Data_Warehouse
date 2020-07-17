import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function will run the queries to drop all pre-existing tables, allowing to run the program from the same initial state.    
    """
    
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)


def create_tables(cur, conn):
    """
    This function will run the queries to create all tables necessary for the program to run.    
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)


def main():
    """
    The main function of this script reads the cfg file and extracts from it the necessary information to connect to the database 'dbname' in the Redshift cluster. It then creates a cursor for this database and calls the functions to drop pre-existing tables and create new ones from scratch.    
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
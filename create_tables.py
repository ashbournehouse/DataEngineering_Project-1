########################################################################
# 21/05/2021 - This script kills and rebuilds the tables required
#   by Udacity Data Engineering - Project 1 'Modeling in Postgres'
#
########################################################################
    #
    ####################################################################
    # Imports
    #
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

UNDERLINE_1 = "========================================================"
UNDERLINE_2 = "++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
UNDERLINE_3 = "--------------------------------------------------------"
    ####################################################################
    # Basic logger setup
    #
import logging
logging.basicConfig(level=logging.INFO)
    ####################################################################
    # Create database
    #
def create_database():
        ################################################################
        # Create and connect to the sparkifydb
        # Returns the connection and cursor to sparkifydb
        ################################################################
        # Create a connection to postgreSQL
        #
    try:
        admin_connection = psycopg2.connect(
            'host=127.0.0.1 dbname=ajb_test user=ajb password=hsc1857'
            )
        logging.info(
            f'\n'
            f'  Connection open:\n'
            f'    {admin_connection}\n'
            f'{UNDERLINE_3}'
            )
    except psycopg2.Error as e:
        logging.error(
            f'\n'
            f'  Error trying to open connection:\n'
            f'{admin_connection}\n'
            f'{UNDERLINE_2}'
            )
#23456789_123456789_123456789_123456789_123456789_123456789_123456789_12
        ################################################################
        # Set auto-commit for this connection
        #
    try:
        admin_connection.set_session(autocommit=True)
        logging.info(
            f'\n'
            f'  Auto-commit active\n'
            f'{UNDERLINE_3}'
            )
    except psycopg2.Error as e:
        logging.error(
            f'\n'
            f'  Error setting auto-commit on connection:\n'
            f'{admin_connection}\n'
            f'  Error returned by psycopg2:\n'
            f'    {e}\n'
            f'{UNDERLINE_2}'
            )
        ################################################################
        # Use that conection to get a 'cursor' that can be used to
        #   execute queries
        #
    try:
        cursor = admin_connection.cursor()
        logging.info(
            f'\n'
            f'  Cursor active\n'
            f'{UNDERLINE_3}'
            )
    except psycopg2.Error as e:
        logging.error(
            f'\n'
            f'  Error obtaining a cursor on connection:\n'
            f'    {admin_connection}\n'
            f'  Error returned by psycopg2:\n'
            f'    {e}\n'
            f'{UNDERLINE_2}'
            )
#23456789_123456789_123456789_123456789_123456789_123456789_123456789_12
            ############################################################
            # Drop and create sparkify database with UTF8 encoding
            #
    database_name = 'sparkify'
    sql = f'DROP DATABASE IF EXISTS {database_name}'
    try:
        cursor.execute(f'{sql}')
        logging.info(
            f'\n'
            f'  Database {database_name} dropped successfully\n'
            f'{UNDERLINE_3}'
            )
    except psycopg2.Error as e:
        logging.error(
            f'\n'
            f'  Error executing query\n'
            f'    {sql}\n'
            f'  Error returned by psycopg2:\n'
            f'    {e}\n'
            f'{UNDERLINE_2}'
            )
            ############################################################
            # Create the sparkify database
            #
    sql = (
            f'CREATE DATABASE {database_name}'
            f' WITH ENCODING \'utf8\' TEMPLATE template0'
            )
    try:
        cursor.execute(f'{sql}')
        logging.info(
            f'\n'
            f'  Database {database_name} created successfully\n'
            f'{UNDERLINE_3}'
            )
    except psycopg2.Error as e:
        logging.error(
            f'\n'
            f'  Error executing query\n'
            f'    {sql}\n'
            f'  Error returned by psycopg2:\n'
            f'    {e}\n'
            f'{UNDERLINE_2}'
            )
        ################################################################
        # close connection to default database
        #
    admin_connection.close()
        ################################################################
        # Create a connection to 'sparkify' database
        #
    try:
        sparkify_connection = psycopg2.connect(
            'host=127.0.0.1 dbname=sparkify user=ajb password=hsc1857'
            )
        logging.info(
            f'\n'
            f'  Connection open:\n'
            f'    {sparkify_connection}\n'
            f'{UNDERLINE_3}'
            )
    except psycopg2.Error as e:
        logging.error(
            f'\n'
            f'  Error trying to open connection:\n'
            f'    {sparkify_connection}\n'
            f'  Error returned by psycopg2:\n'
            f'    {e}\n'
            f'{UNDERLINE_2}'
            )
#23456789_123456789_123456789_123456789_123456789_123456789_123456789_12
        ################################################################
        # Use that conection to get a 'cursor' that can be used to
        #   execute queries
        #
    try:
        sparkify_cursor = sparkify_connection.cursor()
        logging.info(
            f'\n'
            f'  Sparkify cursor active\n'
            f'{UNDERLINE_3}'
            )
    except psycopg2.Error as e:
        logging.error(
            f'\n'
            f'  Error obtaining a cursor on connection:\n'
            f'    {sparkify_connection}\n'
            f'  Error returned by psycopg2:\n'
            f'    {e}\n'
            f'{UNDERLINE_2}'
            )
        ################################################################
        # EMERGENCY close all pids where something has gone wrong!!!
        #
    #sql = f'SELECT pg_terminate_backend(pid) '
    #       f'FROM pg_stat_activity WHERE '
    #       f'datname=\'sparkify\';'
    #sparkify_cursor.execute(sql)
        ################################################################
        # set return values
        #
    return sparkify_cursor, sparkify_connection


def drop_tables(cursor, connecction):
        ################################################################
        # Drop all tables using the queries in `drop_table_queries`
        #   list.
        #
    for query in drop_table_queries:
        cursor.execute(query)
        connecction.commit()

def create_tables(cursor, connecction):
        ################################################################
        # Creates all required the tables using the queries in
        #   `create_table_queries` list.
        #
    for query in create_table_queries:
        cursor.execute(query)
        connecction.commit()

def main():
    logging.warning(
        f'\n'
        f'  We\'re at the beginning ...\n'
        f'{UNDERLINE_3}'
        )
        ################################################################
        # Drops (if it exists) and creates the sparkify database, then
        #   establishes connection with the sparkify database and
        #   gets a cursor to it.
        #
    sparkify_cursor, sparkify_connection = create_database()
        ################################################################
        # Drop all the tables.
        #
    drop_tables(sparkify_cursor, sparkify_connection)
        ################################################################
        # Creates all tables needed.
        #
    create_tables(sparkify_cursor, sparkify_connection)
        ################################################################
        #
        # Do a clean shutdown of the cursor and connection
        #
    try:
        sparkify_cursor.close()
        logging.info(
            f'\n'
            f'  Cursor closed\n'
            f'{UNDERLINE_3}'
            )
    except psycopg2.Error as e:
        logging.error(
            f'\n'
            f'  Error when trying to close the cursor\n'
            f'  Error returned by psycopg2:\n'
            f'    {e}\n'
            f'{UNDERLINE_2}'
            )
        ################################################################
        # Close the connection
        #
    try:
        sparkify_connection.close()
        logging.info(
            f'\n'
            f'  Connection closed\n'
            f'{UNDERLINE_3}'
            )
    except psycopg2.Error as e:
        logging.error(
            f'\n'
            f'  Error when trying to close the connection\n'
            f'  Error returned by psycopg2:\n'
            f'    {e}\n'
            f'{UNDERLINE_2}'
            )
    logging.warning(
        f'\n'
        f'  We\'ve got to the end!!\n\n'
        f'{UNDERLINE_1}\n\n'
        )

if __name__ == "__main__":
    main()
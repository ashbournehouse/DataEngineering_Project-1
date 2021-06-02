import psycopg2
from sql_queries import create_table_queries, drop_table_queries

UNDERLINE_1 = "--------------------------------------------------------------------------------------------------------------------"
UNDERLINE_2 = "===================================================================================================================="

def create_database():
        #############################################################################################
        # Create and connect to the sparkifydb
        # Returns the connection and cursor to sparkifydb
        #############################################################################################
        # Create a connection to postgreSQL
        #
    try:
        admin_connection = psycopg2.connect("host=127.0.0.1 dbname=ajb_test user=ajb password=hsc1857")
        print(f'Connection open\n{admin_connection}\n\n')
    except psycopg2.Error as e:
        print(f'Error trying to open connection\n {admin_connection}')   # NOTE: the use of string interpolation; 'f-strings' in Python
        print(e)
        print(f'{UNDERLINE_1}\n')
            #############################################################################################
            # Set auto-commit for this connection
            #
    try:
        admin_connection.set_session(autocommit=True)
        print(f'Auto-commit active\n')
    except psycopg2.Error as e:
        print(f'Error setting auto-commit on connection\n {admin_connection}')
        print(f'{UNDERLINE_1}\n')
            #############################################################################################
            # Use that conection to get a 'cursor' that can be used to execute queries
            #
    try:
        cursor = admin_connection.cursor()
        print("Cursor active\n")
    except psycopg2.Error as e:
        print(f'Error obtaining a cursor on connection\n {admin_connection}')
        print(e)
            #############################################################################################
            # drop and create sparkify database with UTF8 encoding
            #
    database_name = 'sparkify'
    sql = f'DROP DATABASE IF EXISTS {database_name}'
    try:
        cursor.execute(f'{sql}')
        print(f'Database {database_name} dropped successfully')
        print(f'{UNDERLINE_2}\n')
    except psycopg2.Error as e:
        print(f'Error executing query\n {sql}')
        print(e)
        print(f'{UNDERLINE_1}\n')
    sql = f"CREATE DATABASE {database_name} WITH ENCODING 'utf8' TEMPLATE template0"
    try:
        cursor.execute(f'{sql}')
        print(f'Database {database_name} created successfully')
        print(f'{UNDERLINE_2}\n')
    except psycopg2.Error as e:
        print(f'Error executing query\n {sql}')
        print(e)
        print(f'{UNDERLINE_1}\n')
            #############################################################################################
            # close connection to default database
            #
    admin_connection.close()
        #############################################################################################
        # Create a connection to 'sparkify' database
        #
    try:
        sparkify_connection = psycopg2.connect("host=127.0.0.1 dbname=sparkify user=ajb password=hsc1857")
        print(f'Connection open\n{sparkify_connection}\n\n')
    except psycopg2.Error as e:
        print(f'Error trying to open connection\n {sparkify_connection}')
        print(e)
        print(f'{UNDERLINE_1}\n')
            #############################################################################################
            # Use that conection to get a 'cursor' that can be used to execute queries
            #
    try:
        sparkify_cursor = sparkify_connection.cursor()
        print("Sparkify cursor active\n")
    except psycopg2.Error as e:
        print(f'Error obtaining a cursor on connection\n {sparkify_connection}')
        print(e)
            #############################################################################################
            # EMERGENCY close all pids where something has gone wrong!!!
            #
        #sql = "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='sparkify';"
        #sparkify_cursor.execute(sql)
            #############################################################################################
            # set return values
            #
    return sparkify_cursor, sparkify_connection


def drop_tables(cursor, connecction):
        #############################################################################################
        # Drop all tables using the queries in `drop_table_queries` list.
        #############################################################################################
    for query in drop_table_queries:
        cursor.execute(query)
        connecction.commit()

def create_tables(cursor, connecction):
        #############################################################################################
        # Creates all required the tables using the queries in `create_table_queries` list.
        #############################################################################################
    for query in create_table_queries:
        cursor.execute(query)
        connecction.commit()

def main():
    print("\nWe're at the beginning ...\n\n")
        #############################################################################################
        # Drops (if it exists) and creates the sparkify database.
        # Establishes connection with the sparkify database and get cursor to it.
        #############################################################################################
    sparkify_cursor, sparkify_connection = create_database()
        #############################################################################################
        # Drop all the tables.
        #############################################################################################
    drop_tables(sparkify_cursor, sparkify_connection)
        #############################################################################################
        # Creates all tables needed.
        #############################################################################################
    create_tables(sparkify_cursor, sparkify_connection)
        #############################################################################################
        #
        # Do a clean shutdown of the cursor and connection
        #
    try:
        sparkify_cursor.close()
        print("Cursor closed")
    except psycopg2.Error as e:
        print(f'Error when trying to close the cursor\n')
        print(e)
        print(f'{UNDERLINE_1}\n')
            #
            # Close the connection
            #
    try:
        sparkify_connection.close()
        print("Connection closed\n")
    except psycopg2.Error as e:
        print(f'Error when trying to close the connection\n')
        print(e)
        print(f'{UNDERLINE_1}\n')

    print("We've got to the end!!\n\n")

if __name__ == "__main__":
    main()
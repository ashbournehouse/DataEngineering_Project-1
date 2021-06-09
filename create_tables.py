"""
21/05/2021 - Udacity Data Engineering - Create Tables Script
============================================================

  Standalone version create_tables script.

  'Kills and rebuilds' each of the five table definitions
  required by the etl.py script.

  Part of my submission for:
    Udacity Data Engineering - Project 1 'Modelling in Postgres'

"""

"""
Imports
=======

  psycopg2 to handle interaction with PostgreSQL
  sql_queries.py is part of the submission required by this porject.

"""

import psycopg2
from sql_queries import create_table_queries, drop_table_queries

"""
Underlining
===========
  'Standardise' some underlining and use with logging output
  helping to understand what's going on as we develop code.

"""

UNDERLINE_1 = "========================================================"
UNDERLINE_2 = "++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
UNDERLINE_3 = "--------------------------------------------------------"

"""
Logging
=======

  Using the Python logger makes code much more readable then using
  'debug print' statements

"""

import logging
logging.basicConfig(level=logging.INFO)


"""
Create Database
===============
"""

def create_database():

    """
    Connects to the default database on a local postgreSQL
    installation, then:

     - Obtains a 'cursor' to allow submission of queries
     - Perfoms a 'kill and rebuild' of the 'sparkify' database by:
       * Droping the 'sparkify' datbase if it exists.
       * Creating a new 'sparkify' database
       * Closing the connection to the default database
       * Obtaining a connection to the sparkify database
       * Obtaining a cursor to the sparkify database

    Parameters: none.

    Returns:

     - The connection object for the new database
     - The cursor object for the new database

    """

    """
    Create a connection to default postgreSQL database
    """

    try:
        admin_connection = psycopg2.connect(
            'host=127.0.0.1 dbname=studentdb user=student password=student'
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

    """
    Set auto-commit for this connection
    """

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

    """
    Use that conection to get a 'cursor' that can be used to
      execute queries
    """

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

    """
    Drop the 'sparkify' database, if it already exits, this
      is the 'kill' part of 'kill and rebuild'
    """

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

    """
    Create the 'sparkify' database, this is the start of the
      rebuild part of 'kill and rebuild'
    """

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

    """
    Close connection to default database to avoid any confusion and
      it's generally a safer way to do things.
    """

    admin_connection.close()

    """
    Create a new connection to the new 'sparkify' database for all
      further operations.
    """

    try:
        sparkify_connection = psycopg2.connect(
            'host=127.0.0.1 dbname=studentdb user=student password=student'
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

    """
    Get a new cursor object for the 'sparkify' database to allow
      execution of all further queries.
    """

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
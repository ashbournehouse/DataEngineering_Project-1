"""
05/06/2021 - Udacity Data Engineering - Test Script
===================================================

  07/07/2021 - Reviewed for submission
  07/07/2021 - TEST 6 added 2nd submission

  Standalone version of the tests offered in test.ipynb


"""

"""
Imports
=======

  psycopg2 to handle interaction with PostgreSQL

"""

import psycopg2


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

  Using the Python logger make code much more readable then using
  'debug print' statements

"""

import logging
logging.basicConfig(level=logging.INFO)


"""
Limit Constants
===============

  Aloow limits in SQL queries ... or not by commemting out one or
  other of the lines below. This allows comprehensive or summarised
  testing.

"""

LIMIT_STRING = ' LIMIT 5'
    #
    # For debugging, have an option not to limit the records returned
    #
#LIMIT_STRING = ''


"""
Main
====
"""

def main():

    """
    Main runs queries on each of the five tables counting the number
    of lines in each table then displaying 5 (or more, see above)
    lines of actual data.

    The number of lines in each table is repeated at the end of the
    test run to give a summary check.

    The tests could, possibly should, be spun out to a single
    functio that is called 5 times, but this s a quick-and-dirty
    test script.

    Parameters: none.

    Returns: none.

    """

    logging.info(
        f'\n'
        f'  We\'re at the beginning of the test script ...\n'
        f'{UNDERLINE_3}'
        )

    """
    Create and connect to the sparkifydb
    Returns the connection and cursor to sparkifydb

    """
    try:
        sparkify_connection = psycopg2.connect(
            'host=127.0.0.1 dbname=sparkify user=student password=student'
            )
        sparkify_connection.set_session(autocommit=True)
        logging.info(
            f'\n'
            f'  Connection open\n  {sparkify_connection}\n'
            f'{UNDERLINE_3}\n'
            )
    except psycopg2.Error as e:
        handled_errors += 1
        logging.error(
            f'\n'
            f'  Error trying to open connection:\n'
            f'    {sparkify_connection}\n\n'
            f'  Error raied is: \n'
            f'    {e}\n'
            f'{UNDERLINE_1}'
            )

    """
    Use the conection to get a 'cursor' that can be used to
    execute queries.

    """

    try:
        sparkify_cursor = sparkify_connection.cursor()
        logging.info(
            f'\n'
            f'  Sparkify cursor active\n'
            f'{UNDERLINE_1}'
            )
    except psycopg2.Error as e:
        handled_errors += 1
        logging.error(
            f'\n'
            f'  Error obtaining a cursor on connection:\n'
            f'    {sparkify_connection}\n\n'
            f'  Error raied is: \n'
            f'    {e}\n'
            f'{UNDERLINE_1}'
            )

    """
    TEST 1
    ======
      Selects first 'n' records from the songplays table

    """

    sql = f'SELECT songplay_id, song_id, artist_id FROM songplays{LIMIT_STRING}'
    try:
        sparkify_cursor.execute(sql)
        songplays_rows_found = sparkify_cursor.rowcount
        log_string = (
            f'\n'
            f'  Executed {sql}\n'
            f'  Rows in sparkify_cursor: {songplays_rows_found}\n\n'
            f'First five songplays records found are: \n'
            f'{UNDERLINE_3}\n'
            )
        for record in sparkify_cursor:
            log_string += (
                f'{record}\n'
                )
        log_string += (f'{UNDERLINE_2}')
        logging.info(log_string)
    except psycopg2.Error as e:
        logging.error(
            f'\n'
            f'  Something went wrong while trying to run the query: \n '
            f'    {sql}\n'
            f'  Error raised is: \n'
            f'    {e}\n'
            f'{UNDERLINE_2}'
            )

    """
    TEST 2
    ======
      Selects first 'n' records from the users table.

    """

    sql = f'SELECT * FROM users{LIMIT_STRING}'
    try:
        sparkify_cursor.execute(sql)
        users_rows_found = sparkify_cursor.rowcount
        log_string = (
            f'\n'
            f'  Executed {sql}\n'
            f'  Rows in sparkify_cursor: {users_rows_found}\n\n'
            f'First five users records found are: \n'
            f'{UNDERLINE_3}\n'
            )
        for record in sparkify_cursor:
            log_string += (
                f'{record}\n'
                )
        log_string += (f'{UNDERLINE_2}')
        logging.info(log_string)
    except psycopg2.Error as e:
        logging.error(
            f'\n'
            f'  Something went wrong while trying to run the query: \n '
            f'    {sql}\n'
            f'  Error raised is: \n'
            f'    {e}\n'
            f'{UNDERLINE_2}'
            )

    """
    TEST 3
    ======
      Selects first 'n' records from the songs table.

    """

    sql = f'SELECT * FROM songs{LIMIT_STRING}'
    try:
        sparkify_cursor.execute(sql)
        songs_rows_found = sparkify_cursor.rowcount
        log_string = (
            f'\n'
            f'  Executed {sql}\n'
            f'  Rows in sparkify_cursor: {songs_rows_found}\n\n'
            f'First five songs records found are: \n'
            f'{UNDERLINE_3}\n'
            )
        for record in sparkify_cursor:
            log_string += (
                f'{record}\n'
                )
        log_string += (f'{UNDERLINE_2}')
        logging.info(log_string)
    except psycopg2.Error as e:
        logging.error(
            f'\n'
            f'  Something went wrong while trying to run the query: \n '
            f'    {sql}\n'
            f'  Error raised is: \n'
            f'    {e}\n'
            f'{UNDERLINE_2}'
            )

    """
    TEST 4
    ======
      Selects first 'n' records from the artists table.

    """

    sql = f'SELECT * FROM artists{LIMIT_STRING}'
    try:
        sparkify_cursor.execute(sql)
        artists_rows_found = sparkify_cursor.rowcount
        log_string = (
            f'\n'
            f'  Executed {sql}\n'
            f'  Rows in sparkify_cursor: {artists_rows_found}\n\n'
            f'First five artists records found are: \n'
            f'{UNDERLINE_3}\n'
            )
        for record in sparkify_cursor:
            log_string += (
                f'{record}\n'
                )
        log_string += (f'{UNDERLINE_2}')
        logging.info(log_string)
    except psycopg2.Error as e:
        logging.error(
            f'\n'
            f'  Something went wrong while trying to run the query: \n '
            f'    {sql}\n'
            f'  Error raised is: \n'
            f'    {e}\n'
            f'{UNDERLINE_2}'
            )

    """
    TEST 5
    ======
      Selects first 'n' records from the time table.

    """

    sql = f'SELECT * FROM time{LIMIT_STRING}'
    try:
        sparkify_cursor.execute(sql)
        time_rows_found = sparkify_cursor.rowcount
        log_string = (
            f'\n'
            f'  Executed {sql}\n'
            f'  Rows in sparkify_cursor: {time_rows_found}\n\n'
            f'First five time records found are: \n'
            f'{UNDERLINE_3}\n'
            )
        for record in sparkify_cursor:
            log_string += (
                f'{record}\n'
                )
        log_string += (f'{UNDERLINE_2}')
        logging.info(log_string)
    except psycopg2.Error as e:
        logging.error(
            f'\n'
            f'  Something went wrong while trying to run the query: \n '
            f'    {sql}\n'
            f'  Error raised is: \n'
            f'    {e}\n'
            f'{UNDERLINE_2}'
            )
    """
    TEST 6
    ======
      Ensure that only one record from 'songplays' has both
        a valid songid and artist id as requested in first review

    """

    sql = (f'SELECT * FROM songplays'
          f' WHERE song_id is NOT NULL'
          f' AND artist_id is NOT NULL'
          f' {LIMIT_STRING}'
          )
    try:
        sparkify_cursor.execute(sql)
        informative_songrows_found = sparkify_cursor.rowcount
        log_string = (
            f'\n'
            f'=====================================================\n'
            f'Ensure that only one record from "songplays" has both\n'
            f'  a valid songid and artist id as requested in first \n'
            f'  review\n'
            f'=====================================================\n'
            f'  Executed {sql}\n'
            f'  Rows in sparkify_cursor: {time_rows_found}\n\n'
            f'songplays records found are: \n'
            f'{UNDERLINE_3}\n'
            )
        for record in sparkify_cursor:
            log_string += (
                f'{record}\n'
                )
        log_string += (f'{UNDERLINE_2}')
        logging.info(log_string)
    except psycopg2.Error as e:
        logging.error(
            f'\n'
            f'  Something went wrong while trying to run the query: \n '
            f'    {sql}\n'
            f'  Error raised is: \n'
            f'    {e}\n'
            f'{UNDERLINE_2}'
            )

    """
    Cleanup
    =======
      Do a clean shutdown of the cursor and connection.

    """

    try:
        sparkify_cursor.close()
        logging.info(
            f'\n'
            f'  Sparkify cursor closed\n'
            f'{UNDERLINE_3}\n'
            )
    except psycopg2.Error as e:
        handled_errors += 1
        logging.error(
            f'\n'
            f'  Error when trying to close the cursor\n'
            f'  Error raied is: \n'
            f'    {e}\n'
            f'{UNDERLINE_1}'
            )

    try:
        sparkify_connection.close()
        logging.info(
            f'\n'
            f'  Sparkify connection closed\n'
            f'{UNDERLINE_3}\n'
            )
    except psycopg2.Error as e:
        handled_errors += 1
        logging.error(
            f'\n'
            f'  Error when trying to close the connection\n'
            f'  Error raied is: \n'
            f'    {e}\n'
            f'{UNDERLINE_1}'
            )

    """
    Log a summary
    =============
      At the end of many rows of log output this gives a quick
      check that things are working consistently during
      development.

    """

    logging.info(
            f'\n'
            f'  Summary of tests\n'
            f'  =================\n'
            f'   1 - Rows found in songplays table: {songplays_rows_found}\n'
            f'   2 - Rows found in users table: {users_rows_found}\n'
            f'   3 - Rows found in songs table: {songs_rows_found}\n'
            f'   4 - Rows found in artists table: {artists_rows_found}\n'
            f'   5 - Rows found in time table: {time_rows_found}\n'
            f'   6 - Rows from songplays with non-null '
            f'artist and song IDs: {informative_songrows_found}\n'
            )

    """
    Log completion
    ==============
      When using try-except clauses it's worth logging the
      intended completion og the script.

    """

    logging.info(
        f'\n'
        f'  We\'ve got to the end!!\n\n'
        f'{UNDERLINE_2}\n\n'
        )


if __name__ == "__main__":
    main()
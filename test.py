########################################################################
# 05/06/2021 - Udacity Data Engineering - test Script
#  
#  Standalone version of the tests offered in test.ipynb
#
########################################################################
    #
    ####################################################################
    # imports
    #
import psycopg2
    ####################################################################
    # 'Standardise' some underlining and use wit some 'debugging print'
    #   statements to help understand what's going on as we develop code
    #
UNDERLINE_1 = "========================================================"
UNDERLINE_2 = "++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
UNDERLINE_3 = "--------------------------------------------------------"
    ####################################################################
    # LOGGING
    # =======
    #
import logging
logging.basicConfig(level=logging.INFO)
    #
    ####################################################################
    # ALLOW limits in SQL queries ... or not
    # ======================================
    #
#LIMIT_STRING = ' LIMIT 5'
LIMIT_STRING = ''
    ####################################################################
    # MAIN
    # ====
    #
def main():
    logging.warning(
        f'\n'
        f'  We\'re at the beginning of the test script ...\n'
        f'{UNDERLINE_3}'
        )
        ################################################################
        # Create and connect to the sparkifydb
        # Returns the connection and cursor to sparkifydb
        #
    try:
        sparkify_connection = psycopg2.connect(
            'host=127.0.0.1 dbname=studentdb user=student password=student'
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
        ################################################################
        # Use that conection to get a 'cursor' that can be used to
        #   execute queries
        #
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
        ################################################################
        # TEST 1
        # ======
        #   Select first five records from songplays;
        #
    sql = f'SELECT * FROM songplays{LIMIT_STRING}'
    try:
        sparkify_cursor.execute(sql)
        log_string = (
            f'\n'
            f'  Executed {sql}\n'
            f'  Rows in sparify_cursor: {sparkify_cursor.rowcount}\n\n'
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
        ################################################################
        # TEST 2
        # ======
        #   Select first five records from users;
        #
    sql = f'SELECT * FROM users{LIMIT_STRING}'
    try:
        sparkify_cursor.execute(sql)
        log_string = (
            f'\n'
            f'  Executed {sql}\n'
            f'  Rows in sparify_cursor: {sparkify_cursor.rowcount}\n\n'
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
        ################################################################
        # TEST 3
        # ======
        #   Select first five records from songs;
        #
    sql = f'SELECT * FROM songs{LIMIT_STRING}'
    try:
        sparkify_cursor.execute(sql)
        log_string = (
            f'\n'
            f'  Executed {sql}\n'
            f'  Rows in sparify_cursor: {sparkify_cursor.rowcount}\n\n'
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
        ################################################################
        # TEST 4
        # ======
        #   Select first five records from artists;
        #
    sql = f'SELECT * FROM artists{LIMIT_STRING}'
    try:
        sparkify_cursor.execute(sql)
        log_string = (
            f'\n'
            f'  Executed {sql}\n'
            f'  Rows in sparify_cursor: {sparkify_cursor.rowcount}\n\n'
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
        ################################################################
        # TEST 5
        # ======
        #   Select first five records from time;
        #
    sql = f'SELECT * FROM time{LIMIT_STRING}'
    try:
        sparkify_cursor.execute(sql)
        log_string = (
            f'\n'
            f'  Executed {sql}\n'
            f'  Rows in sparify_cursor: {sparkify_cursor.rowcount}\n\n'
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
        #################################################################
        #
        # Do a clean shutdown of the cursor and connection
        #
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
        ################################################################
        # Close the connection
        #
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
    logging.warning(
        f'\n'
        f'  We\'ve got to the end!!\n\n'
        f'{UNDERLINE_2}\n\n'
        )

if __name__ == "__main__":
    main()
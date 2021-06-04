########################################################################
# 21/05/2021 - This is the 'official' version of a script to populate
#  the songs and artists table ... completed after experimentation in
#   ./Exp[eriments/print_dataframe.py
#
########################################################################
    #
    ####################################################################
    # imports
    #
import os
import sys                    # handy in interpreting errors
import glob
import psycopg2
import json                   # handy for debug-printing dictionaries
import pandas as pd
import numpy as np
from sql_queries import *
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
    # Handling so much data, especially when in a learning/development
    #   environment I'm realising that I need a more capable logger to
    #   help with debugging and learnig than just using 'debug print'
    #   statements where the amount of information becomes as hard to
    #   sort through as the original data. And Python has a capable
    #   logger so let's learn to use it.
    #
    #   Acknowledging online turorials from Corey Scaher, starting here:
    #      https://www.youtube.com/watch?v=-ARI4Cz-awo
    #
    # REMEMBER:
    #   There are five (Python) logging levels provided (in proiority
    #       order):
    #
    #   - DEBUG ...... detailed info, typically only of interest when
    #         diagnosing problems
    #   - INFO ....... confirmation that things are working as expected
    #   - WARNING .... an indication that something unexpected happened
    #   - ERROR ...... a serious problem, the software has not been able
    #         to do something
    #   - CRITICAL ... a serious error where the software may be unable
    #         to continue
    #
    #   It might be a good idea to comment out the 'debug print' stuff
    #     as a learning reminder. Using trile quotes style comments
    #     might make this more obvious as we scan through the code.
    #
    #  DEBUG_PRINT
    #  ===========
    #
    #  'debug print' statements now removed (superceeding the
    #    paragraph below) as the content and formatting used with the
    #    logging system has diverged too much for the comparison to
    #    be useful.
    #
    #  ----------------------------------------------------------------
    #  Keeping the old 'debug print' statements as comments will also
    #    highlight any difference in formatting when using logging over
    #    'debug print'. For example copious use of underlining can help
    #    comprehension in 'debug print' but may be inappropriate when
    #    logging to a file, especially if further (perhaps autometed)
    #    log analysis is to be used
    #  ----------------------------------------------------------------
    #
import logging
logging.basicConfig(level=logging.INFO)
    #
    ####################################################################
    # Using a global variable to count handled errors is clunky but ...
    #   ... pragmatic here as it saves hunting through lots of output
    #   when there are no error messages to find.
    #
handled_errors = 0

def process_song_file(cursor, filepath):
    global handled_errors
    songs_saved_to_database = 0
    artists_saved_to_database = 0
        ################################################################
        # This processes one song file ... I think ...
        #
        # - Use pandas to open the data file as requested ...
        #    ... though, I think that, you could just read the json)
        #
        #  NOTE: When using pandas.read_json with a single line of json
        #    data per file, then it is necessary to specify typ='series'
        #
    logging.info(
        f'\n'
        f'  Entering process_song_file for: '
        f'{os.path.basename(filepath)}\n'
        f'{UNDERLINE_3}')
    try:
        dataframe = pd.read_json(filepath, typ='series')
    except:
        handled_errors += 1
        logging.error(
            f'\n'
            f'  Something went wrong converting to a dataframe for:\n'
            f'    {os.path.basename(filepath)}\n'
            f'{UNDERLINE_1}')
        ################################################################
        # Task #1: Populate Songs Table
        # =============================
        #
        #   NOTE a): dataframe.values as recommended by the course
        #     notes is, sort-of, deprecated and dataframe.to_numpy()
        #     recommended instead. This retunrns an array of values,
        #     which I'll enumerate here for clarity!
        #
        #   NOTE b): single quotes are a problem in postgreSQL
        #     queries so replace them with two quotes where they're
        #     likely to occur
        #
        #   NOTE c): rather than a string containing the field
        #     values for the song_data, we need to make a tuple, so
        #     use a cast
        #
    try:
        song_fields = dataframe.to_numpy()
        num_songs = song_fields[0]
        artist_id = song_fields[1]
        artist_latitude = song_fields[2]
        artist_longitude = song_fields[3]
        artist_location = song_fields[4].replace("'", "''")
        artist_name = song_fields[5].replace("'", "''")
        song_id = song_fields[6]
        title = song_fields[7].replace("'", "''")
        duration = song_fields[8]
        year = song_fields[9]
        song_data = tuple([song_id, title, artist_id, year, duration])
        logging.debug(f'\n  song_data tuple is: {song_data}')

    except:
        handled_errors += 1
        logging.error(
            f'\n'
            f'  Something went wrong building the song_data tuple\n'
            f'{UNDERLINE_1}')
        ################################################################
        # With everything prepared, insert the song data into the
        #   database.
        #
    try:
        cursor.execute(song_table_insert, song_data)
        songs_saved_to_database += 1
        logging.debug(
            f'\n'
            f'  Song data record saved to database')
    except psycopg2.Error as e:
        handled_errors += 1
        logging.error(
            f'  Error saving song data record\n'
            f'    {e}\n'
            f'{UNDERLINE_1}\n')
        ################################################################
        # Task #2: Populate Artists Table
        #
        #   Use the same basic techniques as used for the song data
        #
    try:
        artist_data = tuple([artist_id, artist_name, artist_location,
            artist_latitude, artist_longitude])
        logging.debug(
            f'\n'
            f'  artist_data tuple is: {artist_data}')
    except:
        handled_errors += 1
        logging.error(
            f'\n'
            f'Something went wrong building the artist_data string\n'
            f'{UNDERLINE_1}')
        ############################################################
        # With everything prepared, insert the artist data into
        #   the database.
        #
    try:
        cursor.execute(artist_table_insert, artist_data)
        artists_saved_to_database += 1
        logging.debug(
            f'\n'
            f'  Artist data record saved to database')
    except psycopg2.Error as e:
        handled_errors += 1
        logging.error(
            f'\n'
            f'  Error saving artist data record\n'
            f'    {e}')
        logging.error(f'{UNDERLINE_1}\n')
    logging.info(
        f'\n'
        f'  process_song_file complete\n'
        f'    Songs saved to database: {songs_saved_to_database}\n'
        f'    Artists saved to database: {artists_saved_to_database}\n'
        f'{UNDERLINE_3}')

def process_log_file(cursor, filepath):
    global handled_errors
        ################################################################
        # This processes one log file ...
        #
        #   - Use pandas to open the data file as requested ...
        #
        #  NOTE: Each log file has many lines of user activity data
        #
        #  NOTE: When using pandas.read_json with a multiple lines of
        #    json data per file, then it is necessary to specify
        #    lines=True
        #
       #################################################################
        # Use pandas to open the log file ...
        #
        #
    logging.info(f'\n\
      Entering process_log_file for: \
      {os.path.basename(filepath)}\n{UNDERLINE_3}')
    try:
        dataframe = pd.read_json(filepath, lines=True)
            # ... works for a multi-line JSON data file ...?
        logging.debug(
            f'\n'
            f'  File: {os.path.basename(filepath)}\n\n'
            f'  Number of lines read:'
            f'{len(dataframe.index)}\n\n'
            f'Data fields (head):\n'
            f'{dataframe.head()}\n\n'
            f'Data fields (tail)\n'
            f'{dataframe.tail()}\n\n'
            f'{UNDERLINE_3}')
#23456789_123456789_123456789_123456789_123456789_123456789_123456789_12
           #############################################################
            # This error handling might be better spun out to a function
            #
    except OSError as ose:
        handled_errors += 1
        logging.error(
            f'\n'
            f'  Something went wrong converting to a dataframe for:\n'
            f'    {os.path.basename(filepath)}\n'
            f'  OS returned:\n\n{ose}\n'
            f'{UNDERLINE_1}\n')
    except ValueError as ve:
        handled_errors += 1
        logging.error(
            f'\n'
            f'  A ValueError occurred converting to a dataframe for:\n'
            f'    {os.path.basename(filepath)}'
            f'  Error message is:\n\n{ve}\n'
            f'{UNDERLINE_1}\n')
    except NameError as ne:
        handled_errors += 1
        logging.error(
            f'\n'
            f'  A NameError occurred convertingto a dataframe for:\n'
            f'    {os.path.basename(filepath)}'
            f'  Error message is:\n\n{ve}\n'
            f'{UNDERLINE_1}\n')
    except:
        handled_errors += 1
        logging.error(
            f'\n'
            f'  Some other non-OSError occurred reading:\n'
            f'    {os.path.basename(filepath)}'
            f'{UNDERLINE_1}\n')
        ################################################################
        # filter by NextSong action to remove unwanted data
        #
        # NOTE: This is placing quite a lot of data in memory, so
        #   perhaps expect problems if the log files are huge ...
        #
    try:
        next_song_rows = dataframe.loc[dataframe["page"] == "NextSong"]
        logging.info(
            f'\n'
            f'  Filtering records where page == NextSong for:\n'
            f'    dataframe from: {os.path.basename(filepath)}\n'
            f'  Rows found: {len(next_song_rows.index)}\n\n'
            f'Data fields (head):\n'
            f'{next_song_rows.head()}\n\n'
            f'Data fields (tail)\n'
            f'{next_song_rows.tail()}\n\n'
            f'{UNDERLINE_3}'
            )
    except OSError as ose:
        handled_errors += 1
        logging.error(
            f'\n'
            f'  Something went wrong filtering rows in: \n'
            f'    dataframe from: {os.path.basename(filepath)}\n'
            f'  OS returned:\n\n{ose}\n\n'
            f'{UNDERLINE_3}'
            )
    except ValueError as ve:
        handled_errors += 1
        logging.error(
            f'\n'
            f'  A ValueError occurred filtering rows in: \n'
            f'    dataframe from: {os.path.basename(filepath)}\n'
            f'  Error message is:\n\n{ve}\n\n'
            f'{UNDERLINE_3}'
            )
    except NameError as ne:
        handled_errors += 1
        logging.error(
            f'\n'
            f'  A NameError occurred filtering rows in: \n'
            f'    dataframe from: {os.path.basename(filepath)}\n'
            f'  Error message is:\n\n{ne}\n\n'
            f'{UNDERLINE_3}'
            )
    except:
        handled_errors += 1
        logging.error(
            f'\n'
            f'Some error occurred filtering rows in:\n'
            f'    dataframe from: {os.path.basename(filepath)}\n'
            f'{UNDERLINE_3}'
            )
            ############################################################
            # DEBUG --- Check for sensible looking user table data ...
            #
    log_string = (
        f'\n'
        f'  Extracting required data fields (for debugging ONLY):\n\n'
        )
    for index, row in next_song_rows.iterrows():
        log_string += (
            f'{index} --- '
            f' User ID: {row["userId"]},'
            f' Name: {row["firstName"]} {row["lastName"]},'
            f' Gender: {row["gender"]},'
            f' Level: {row["level"]}'
            f'\n'
            )
    log_string += f'{UNDERLINE_3}'
    logging.debug(log_string)
        ################################################################
        # Task #3: Populate Time Table
        # ============================
        #
        #  NOTE a): Further reading ...
        #    Apparently, appending rows to a dataframe one at a time is
        #     incredibly slow which may be why the Jupyter notebook
        #     (eti.pynb) suggests making a dictionary of all the records
        #     first, then convert that to a dataframe and submit that to
        #     psycopg2.
        #
        #  NOTE b): Running some tests on diffeent ways of doing things
        #    seems to confirm this. Handling many dataframes each
        #    containing one record seems very slow.
        #
        #  NOTE c) So, look at the format that we need to submit the
        #    processed data to the database to decide a strategy.
        #    We'll be using (something like):
        #
        #        for i, row in time_dataframe.iterrows():
        #             logging.debug(f'Row {i} list(row) is: {list(row)}')
        #             cursor.execute(time_table_insert, list(row))
        #
        #   We will want each row of our timedata_list to be a tuple of
        #     the values of the field names:
        #       i.e. start_time, hour, day, week, month, year, weekday
        #          --- as specified in sql_queries.py
        #
        #  NOTE d): The steps in the process become:
        #    (d1) - Set up a Python list to hold the time data records
        #             extracted from the file
        #    (d2) - Loop over the dataframe derived from the input file,
        #             extracting the records we want
        #    (d3) - Make a Python dictionary from each row in the
        #             dataframe
        #    (d4) - Append that to the timedata list
        #    (d5) - Use the list of dictionaries to make a new dataframe
        #             (pandas has a convenient method)
        #    (d6) - Use the new dataframe to create database records
        #            (again pandas has a convenient method)
        #
    log_string = (
        f'\n'
        f'  Setting up a python list of python dictionaries for the'
        f' time data ...\n'
        )
            ############################################################
            #    (d1) - Set up a Python list to hold the time data
            #             records extracted from the file
            #
    timedata_list = list()
        ################################################################
        #    (d2) - Loop over the dataframe derived from the input file,
        #             extracting the records we want
        #
    log_string += (
        f'    ... extracting time data for each record\n\n'
        )
    loopcount = 0
    try:
        for index, row in dataframe.iterrows():
            as_datetime = pd.to_datetime(row["ts"], unit="ms")
                ########################################################
                #    (d3) - Make a Python dictionary from each row in
                #             the dataframe
                #
            time_dict = dict()
            time_dict["start_time"] = as_datetime
            time_dict["hour"] = as_datetime.hour
            time_dict["day"] = as_datetime.day
            time_dict["week"] = as_datetime.week
            time_dict["month"] = as_datetime.month
            time_dict["year"] = as_datetime.year
            time_dict["weekday"] = as_datetime.day_name()
                #
                # Add to the log_string for each record processed
                #
            log_string += (
                f'Time (in ms): {row["ts"]}  '
                f'Start time: {time_dict["start_time"]}  '
                f'Hour: {time_dict["hour"]}  '
                f'Day: {time_dict["day"]}  '
                f'Week number: {time_dict["week"]}  '
                f'Month: {time_dict["month"]}  '
                f'Year: {time_dict["year"]}  '
                f'Weekday: {time_dict["weekday"]}  '
                f'\n'
                )
            loopcount += 1
                ########################################################
                #    (d4) - Append that to the timedata list
                #
                # !! Dangerous Python indentation ... easy to get this
                #      outside the loop!!
                #
            timedata_list.append(time_dict)
        log_string += (
            f'\n  Number of records processed: {loopcount}\n'
            f'\n  Number of records added to timedata_list: '
            f'{len(timedata_list)}\n'
            f'{UNDERLINE_3}'
            )
        logging.debug(UNDERLINE_3)
    except:
        handled_errors += 1
        logging.error(
            f'\n'
            f'  An error occurred when extracting data to the '
            f'time_dict:\n'
            f'{UNDERLINE_3}')
            ############################################################
            #    (d5) - Use the list of dictionaries to make a new
            #             dataframe (pandas has a convenient method)
            #
    try:
        time_dataframe = pd.DataFrame.from_records(timedata_list)
        logging.debug(
            f'\n'
            f'Time data dataframe ...\n'
            f'{time_dataframe.to_string()}\n'
            f'{UNDERLINE_3}'
            )
    except:
        handled_errors += 1
        logging.error(
            f'\n'
            f'  An error occurred when converting time_dict to a '
            f'dataframe:\n'
            f'{UNDERLINE_3}'
            )
        ################################################################
        #    (d6) - Use the new dataframe to create database records
        #              (again pandas has a convenient method)
        #
    loopcount = 0
    log_string = (
        f'\n'
        f'  Inserting time data rows into database ...\n\n'
        )
    for i, row in time_dataframe.iterrows():
        try:
            log_string += (
                f'(Time) Row {i} list(row) is: {list(row)}\n'
                )
            cursor.execute(time_table_insert, list(row))
            loopcount += 1
        except psycopg2.Error as e:
            handled_errors += 1
            logging.error(
                f'\n'
                f'  Error saving time data record\n {e}'
                f'{UNDERLINE_3}'
                )
    log_string += (
        f'\n\n  '
        f'{loopcount} records added to the database\n'
        f'{UNDERLINE_3}'
        )
    logging.info(log_string)
        ################################################################
        # Task #4: Populate User Table
        # ============================
        #
        # Load user table ... try loading just the required columns from
        #   the file ... more time than working from the full dataset,
        #   but less memory!
        #
        # WARNING!!! - This works using a csv file, but not with JSON
        #   ... shrug??
        #
        # So, create a new user_dataframe from the next_song_rows
        #   dataframe
        #
        #  NOTE: You need to be logged in to request a next-song, so
        #          we'll still get all the users.
        #
    try:
        user_dataframe = next_song_rows[['userId', 'firstName',
                                    'lastName', 'gender', 'level']]
        logging.info (
            f'\n'
            f'  Extracting a user-dataframe from file: '
            f'{os.path.basename(filepath)}\n\n'
            f'  Number of lines read: {len(user_dataframe.index)}\n\n'
            f'User data fields (head):\n'
            f'{user_dataframe.head().to_string()}\n\n'
            f'User data fields (tail)\n'
            f'{user_dataframe.tail().to_string()}\n\n'
            f'{UNDERLINE_3}'
            )
#23456789_123456789_123456789_123456789_123456789_123456789_123456789_12
    except OSError as ose:
        handled_errors += 1
        logging.error(
            f'\n'
            f'  Something went wrong creating a sub-dataframe for '
            f'users.'
            f'\nOS returned:\n\n{ose}\n\n'
            f'{UNDERLINE_3}'
            )
    except ValueError as ve:
        handled_errors += 1
        logging.error(
            f'\n'
            f'  A ValueError occurred creating a sub-dataframe for '
            f'users.'
            f'\nError message is:\n\n{ve}\n\n'
            f'{UNDERLINE_3}'
            )
    except NameError as ne:
        handled_errors += 1
        logging.error(
            f'\n'
            f'  A NameError occurred creating a sub-dataframe for '
            f'users.'
            f'\nError message is:\n\n{ne}\n\n'
            f'{UNDERLINE_3}'
            )
    except:
        handled_errors += 1
        logging.error(
            f'\n'
            f'  Some error occurred creating a sub-dataframe for '
            f'users.\n\n'
            f'{UNDERLINE_3}'
            )
        ################################################################
        # Insert user records once again using the iterrows method
        #   provided by pandas
        #
    log_string = (
        f'\n'
        f'  Inserting user data rows into database ...\n\n'
        )
    for i, row in user_dataframe.iterrows():
        try:
            log_string += (
                f'(User) Row {i} list(row) is: {list(row)}\n'
                )
            cursor.execute(user_table_insert, list(row))
        except psycopg2.Error as e:
            handled_errors += 1
            logging.error(
                f'\n'
                f'  Error saving user data record\n {e}'
                )
    log_string += f'{UNDERLINE_3}'
    logging.info(log_string)
        ################################################################
        # Task #5: Populate 'songplays' Table
        # ===================================
        #
        #   FROM etl.ipynb ...
        #   ===================
        #     This one is a little more complicated since information
        #     from the songs table, artists table, and original log file
        #     are all needed for the songplays table. Since the log file
        #     does not specify an ID for either the song or the artist,
        #     you'll need to get the song ID and artist ID by querying
        #     the songs and artists tables to find matches based on song
        #     title, artist name, and song duration time.
        #
        ################################################################
        # For each row in the dataframe (the dataframe derived from
        #   log files!!)
        #
    log_string = (
        f'\n'
        f'Select all songs, just to make sure that there are some ...'
        )
    cursor.execute("SELECT song_id, title FROM songs")
    for row in cursor:
        log_string += (
            f'\n'
            f'{cursor.fetchone()}\n'
            )
    log_string += (UNDERLINE_2)
    logging.info(log_string)
        #
    log_string = (
        f'\n'
        f'  Retrieving song and artist IDs using an SQL query ...\n\n'
        )
            # REMEMBER it's the next_song_rows dataframe !!!
    for index, row in next_song_rows.iterrows():
            ############################################################
            # Get songid and artistid from song and artist tables
            #
            #   Remeber:
            #     song_select = 'SELECT song_id, artist_id FROM songs
            #                    JOIN artists ON artist_id
            #                    WHERE (title = %s,
            #                           name = %s,
            #                           duration = %s);'
            #
        query_values = tuple([row.song])
        logging.debug(
            f'\n'
            f'  Composed SQL query is: '
            f'{cursor.mogrify(song_select, query_values)}'
            f'{UNDERLINE_3}'
            )
        cursor.execute(song_select, query_values)
        test = cursor.fetchone()
        if (test is not None):
            log_string += (
                f'Song title: {row.song}, Artist ID {row.artist}'
                f' => {test}\n')
            ############################################################
            # Insert songplay record
            #   REMEMBER: - Table 'songplays' has fields:
            #     songplay_id, start_time, user_id, level, song_id,
            #     artist_id, session_id, location, user_agent
            #
        #songplay_data =
        #cur.execute(songplay_table_insert, songplay_data)
    log_string += f'{UNDERLINE_3}'

    logging.info(log_string)
    logging.info(
        f'\n'
        f'  process_log_file complete for file: '
        f'{os.path.basename(filepath)} \n'
        f'{UNDERLINE_3}'
        )

def process_data(cursor, connection, filepath, func):
    global handled_errors
#23456789_123456789_123456789_123456789_123456789_123456789_123456789_12
    logging.info(
        f'\n'
        f'  Entering process_data for path: {filepath} with: {func}\n'
        f'{UNDERLINE_3}'
        )
    logging.debug(
        f'\n'
        f'  Absolute path is:\n  {os.path.abspath(filepath)}\n'
        f'{UNDERLINE_3}'
        )
#
            ############################################################
            # Get all files matching extension from directory using os
            #   and glob
            #
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for file in files :
            all_files.append(os.path.abspath(file))
        ################################################################
        # get total number of files found
        #
    num_files = len(all_files)
    logging.info(
        f'\n'
        f'  {num_files} files found in path: '
        f'{os.path.basename(filepath)}\n'
        f'{UNDERLINE_2}'
        )
            ############################################################
            # iterate over files and process
            #
    for i, datafile in enumerate(all_files, 1):
        func(cursor, datafile)
        logging.info(
            f'\n'
            f'  {i}/{num_files} files processed.\n'
            f'{UNDERLINE_2}'
            )
    logging.info(
        f'\n'
        f'  Leaving process_data for path: '
        f'{os.path.basename(filepath)} with: {func}\n'
        f'{UNDERLINE_1}'
        )

def main():
    global handled_errors
    logging.warning(
        f'\n'
        f'  We\'re at the beginning ...\n'
        f'{UNDERLINE_3}'
        )
            ############################################################
            # Create and connect to the sparkifydb
            # Returns the connection and cursor to sparkifydb
            #
            # Create a connection to postgreSQL
            #
    try:
#23456789_123456789_123456789_123456789_123456789_123456789_123456789_12
        sparkify_connection = psycopg2.connect(
            "host=127.0.0.1 dbname=sparkify user=ajb password=hsc1857"
            )
        sparkify_connection.set_session(autocommit=True)
        logging.info(
            f'\n'
            f'  Connection open\n  {sparkify_connection}\n'
            f'{UNDERLINE_3}'
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
        logging.info(f'\n  Sparkify cursor active\n{UNDERLINE_1}')
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
          ##############################################################
          # Specify the root path for the song data files
          #
    song_datapath = 'data/song_data'
    process_data(sparkify_cursor,
        sparkify_connection,
        filepath=song_datapath,
        func=process_song_file
        )
    try:
        sql = 'SELECT count(*) from songs'
        sparkify_cursor.execute(sql)
        logging.info(
            f'\n'
            f'  Number of songs in the database: '
            f'{sparkify_cursor.fetchone()}\n'
            f'{UNDERLINE_3}'
            )
    except psycopg2.Error as e:
        logging.error(
            f'\n'
            f'  Something went wrong while trying to test the the '
            f'number of song records\n'
            f'  Error raied is: \n'
            f'    {e}\n'
            f'{UNDERLINE_1}'
            )
    try:
        sql = 'SELECT count(*) from artists'
        sparkify_cursor.execute(sql)
        logging.info(
            f'\n'
            f'  Number of artists in the database: '
            f'{sparkify_cursor.fetchone()}\n'
            f'{UNDERLINE_3}'
            )
    except psycopg2.Error as e:
        logging.error(
            f'\n'
            f'  Something went wrong while trying to test the '
            f'number of artist records\n'
            f'  Error raied is: \n'
            f'    {e}\n'
            f'{UNDERLINE_1}'
            )
          ##############################################################
          # Specify the root path for the log data files
          #
    logs_datapath = 'data/log_data'
    process_data(sparkify_cursor,
        sparkify_connection,
        filepath=logs_datapath,
        func=process_log_file
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
            f'{UNDERLINE_3}'
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
            f'{UNDERLINE_3}'
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
        f'  Handled errors encountered: {handled_errors}\n\n'
        f'  We\'ve got to the end!!\n\n'
        f'{UNDERLINE_2}\n\n'
        )

if __name__ == "__main__":
    main()
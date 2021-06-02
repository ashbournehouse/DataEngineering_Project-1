#########################################################################
# 21/05/2021 - This is the 'official' version of a script to populate
#  the songs and artists table ... completed after experimentation in
#   ./Exp[eriments/print_dataframe.py
#
#########################################################################
    #
    #########################################################################
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
    #########################################################################
    # 'Standardise' some underlining and use wit some 'debugging print'
    #   statements to help understand what's going on as we develop code
    #
UNDERLINE_1 = "===================================================================================================================="
UNDERLINE_2 = "--------------------------------------------------------------------------------------------------------------------"
UNDERLINE_3 = "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
    #########################################################################
    # LOGGING
    # =======
    # Handling so much data, especially when in a learning/development
    #   environment I'm realising that I need a more capable logger to
    #   help with debugging and learnig than just using 'debug print'
    #   statements where the amount of information becomes as hard to sort
    #   through as the original data. And Python has a capable logger so
    #   let's learn to use it.
    #
    #   Acknowledging online turorials from Corey Scaher, starting here:
    #      https://www.youtube.com/watch?v=-ARI4Cz-awo
    #
    # REMEMBER:
    #   There are five (Python) logging levels provided (in proiority order):
    #       - DEBUG ...... detailed info, typically only of interest when diagnosing problems
    #       - INFO ....... confirmation that things are working as expected
    #       - WARNING .... an indication that something unexpected happened
    #       - ERROR ...... a serious problem, the software has not been able to do something
    #       - CRITICAL ... a serious error where the software may be unable to continue
    #
    #   It might be a good idea to comment out the 'debug print' stuff
    #     as a learning reminder. Using trile quotes style comments might
    #     make this more obvious as we scan through the code.
    #
    #   Keeping the old 'debug print' statements as comments will also highlight
    #     any difference in formatting when using logging over 'debug print'. For
    #     example copious use of underlining can help comprehension in 'debug print'
    #     but may be inappropriate when logging to a file, especially if further
    #     (perhaps autometed) log analysis is to be used
    #
# DEBUG = False      # Capitalise convention for CONSTANTS
import logging
logging.basicConfig(level=logging.INFO)
    #
    #########################################################################
    # Using a global variable to count handled errors is clunky but ...
    #   ... pragmatic here as it saves hunting through lots of output
    #   when there are no error messages to find.
    #
handled_errors = 0

def process_song_file(cursor, filepath):
    global handled_errors
        #########################################################################
        # This processes one song file ... I think ...
        #
        # - Use pandas to open the data file as requested ...
        #    ... though, I think that, you could just read the json)
        #
        #  NOTE: When using pandas.read_json with a single line of json data
        #    per file, then it is necessary to specify typ='series'
        #
    """
    print(f'Entering process_song_file for:\n {filepath}\n\n')
    """
    logging.info(f'\n  Entering process_song_file for:\n    {filepath}\n{UNDERLINE_3}')
    try:
        dataframe = pd.read_json(filepath, typ='series')
    except:
        handled_errors += 1
        """
        print(f'Something went wrong converting the file to a dataframe:\n{sys.exc_info()[0]}\n')
        print(UNDERLINE_1)
        """
        logging.error(f'Something went wrong converting the file to a dataframe:\n{sys.exc_info()[0]}\n')
        logging.error(UNDERLINE_1)
            #########################################################################
            # Task #1: Populate Songs Table
            #
            #   NOTE a): dataframe.values as recommended by the course notes is, sort-
            #     of, deprecated and dataframe.to_numpy() recommended instead. This
            #     retunrns an array of values, which I'll enumerate here for
            #     clarity!
            #
            #   NOTE b): single quotes are a problem in postgreSQL queries
            #     so replace them with two quotes where they're likely to occur
            #
            #   NOTE c): rather than a string containing the field values
            #     for the sng dat, we need to make a tuple, so use a cast
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
        """
        if (DEBUG == True):
            print(f'song_data tuple is: {song_data}')
        """
        logging.debug(f'\n  song_data tuple is: {song_data}\n{UNDERLINE_3}')

    except:
        handled_errors += 1
        """
        print(f'Something went wrong building the song_data tuple\n{sys.exc_info()[0]}\n')
        print(UNDERLINE_1)
        """
        logging.error(f'Something went wrong building the song_data tuple\n{sys.exc_info()[0]}\n')
        logging.error(UNDERLINE_1)
            #########################################################################
            # With everything prepared, insert the song data into the database
            #
    try:
        cursor.execute(song_table_insert, song_data)
        """
        if (DEBUG == True):
            print("Song data record saved to database")
            print(f'{UNDERLINE_3}\n')
        """
        logging.debug(f'\n  Song data record saved to database\n{UNDERLINE_3}')
    except psycopg2.Error as e:
        handled_errors += 1
        """
        print(f'Error saving song data record\n {e}')
        print(f'{UNDERLINE_1}\n')
        """
        logging.error(f'Error saving song data record\n {e}')
        logging.error(f'{UNDERLINE_1}\n')
            #########################################################################
            # Task #2: Populate Artists Table
            #
            #   Use the same basic techniques as used for the song data
            #
    try:
        artist_data = tuple([artist_id, artist_name, artist_location, artist_latitude, artist_longitude])
        """
        if (DEBUG == True):
            print(f'artist_data: {artist_data}')
        """
        logging.debug(f'\n  artist_data tuple is: {artist_data}\n{UNDERLINE_3}')
    except:
        handled_errors += 1
        """
        print(f'Something went wrong building the artist_data string\n{sys.exc_info()[0]}\n')
        print(UNDERLINE_1)
        """
        logging.error(f'Something went wrong building the artist_data string\n{sys.exc_info()[0]}\n')
        logging.error(UNDERLINE_1)
            #########################################################################
            # With everything prepared, insert the artist data into the database
            #
    try:
        cursor.execute(artist_table_insert, artist_data)
        """
        if (DEBUG == True):
            print("Artist data record saved to database")
            print(f'{UNDERLINE_3}\n')
        """
        logging.debug(f'\n  Artist data record saved to database\n{UNDERLINE_3}')
    except psycopg2.Error as e:
        handled_errors += 1
        """
        print(f'Error saving artist data record\n {e}')
        print(f'{UNDERLINE_1}\n')
        """
        logging.debug(f'Error saving artist data record\n {e}')
        logging.debug(f'{UNDERLINE_1}\n')
    """
    print('\n\nprocess_song_file complete')
    print(f'{UNDERLINE_1}\n{UNDERLINE_1}\n')
    """
    logging.debug(f'\n  process_song_file complete\n{UNDERLINE_3}')

def process_log_file(cursor, filepath):
    global handled_errors
        #########################################################################
        # This processes one log file ...
        #
        #   - Use pandas to open the data file as requested ...
        #
        #  NOTE: Each log file has many lines of user activity data
        #
        #  NOTE: When using pandas.read_json with a multiple lines of json data
        #    per file, then it is necessary to specify lines=True
        #
        #############################################################################################
        # Use pandas to open the log file ...
        #
        #
    """
    print(f'Entering process_log_file for:\n {filepath}\n\n')
    print(f'Reading log file\n')
    """
    logging.info(f'\n  Entering process_log_file for:\n {filepath}\n\n  >>>Reading log file\n{UNDERLINE_3}')
    try:
        dataframe = pd.read_json(filepath, lines=True)  #... works for a multi-line JSON data file ...?
        """
        if (DEBUG == True):
            print(f'File: \n {filepath}\n')
            print(f'Number of lines read: {len(dataframe.index)}\n')
            print(f'Data fields (head):\n')
            print(dataframe.head())
            print('\n... (tail)')
            print(dataframe.tail())
        """
        log_string = (f'\n  >>> File: {filepath}\n\n')
        log_string += (f'  >>> Number of lines read: {len(dataframe.index)}\n\n')
        log_string += (f'  >>> Data fields (head):\n')
        log_string += (f'{dataframe.head()}\n\n')
        log_string += ('  >>> Data fields (tail)\n')
        log_string += (f'{dataframe.tail()}\n\n')
        log_string += (f'{UNDERLINE_3}')
        logging.debug(log_string)
           ###################################################################################
            # This error handling migh be better spun out to a function
            #
    except OSError as ose:
        handled_errors += 1
        """
        print(f'Something went wrong converting the file to a dataframe:\n {filepath}')
        print(f'OS returned:\n\n{ose}\n\n')
        print(UNDERLINE_1)
        """
        logging.error(f'Something went wrong converting the file to a dataframe:\n {filepath}')
        logging.error(f'OS returned:\n\n{ose}\n\n')
        logging.error(UNDERLINE_1)
    except ValueError as ve:
        handled_errors += 1
        """
        print(f'A ValueError occurred converting the file to a dataframe:\n {filepath}')
        print(f'Error message is:\n\n{ve}\n\n')
        print(UNDERLINE_1)
        """
        logging.error(f'A ValueError occurred converting the file to a dataframe:\n {filepath}')
        logging.error(f'Error message is:\n\n{ve}\n\n')
        logging.error(UNDERLINE_1)
    except NameError as ne:
        handled_errors += 1
        """
        print(f'A NameError occurred converting the file to a dataframe:\n {filepath}')
        print(f'Error message is:\n\n{ve}\n\n')
        print(UNDERLINE_1)
        """
        logging.error(f'A NameError occurred converting the file to a dataframe:\n {filepath}')
        logging.error(f'Error message is:\n\n{ve}\n\n')
        logging.error(UNDERLINE_1)
    except:
        handled_errors += 1
        """
        print(f'Some other non-OSError occurred reading:\n{sys.exc_info()[0]}\n')
        """
        logging.error(f'Some other non-OSError occurred reading:\n{sys.exc_info()[0]}\n')
            #
    """
    print(f'Done\n{UNDERLINE_3}\n\n')
    """
        #############################################################################################
        # filter by NextSong action to remove unwanted data
        #
        # NOTE: This is placing quite a lot of data in memory, so perhaps expect problems
        #   if the log files are huge ...
        #
    logging.info(f'\n  Filtering records where page == NextSong\n{UNDERLINE_3}')
    try:
        next_song_rows = dataframe.loc[dataframe["page"] == "NextSong"]
        """
        if (DEBUG == True):
            print(f'File: \n {filepath}\n')
            print(f'Rows where "page" = "NextSong": {len(next_song_rows.index)}\n')
            print(f'Data fields (head):\n')
            print(next_song_rows.head())
            print('\n... (tail)')
            print(next_song_rows.tail())
        """
        log_string = (f'\n  >>> File: {filepath}\n\n')
        log_string += (f'  >>> Rows where "page" = "NextSong": {len(next_song_rows.index)}\n\n')
        log_string += (f'  >>> Data fields (head):\n')
        log_string += (f'{next_song_rows.head()}\n\n')
        log_string += ('  >>> Data fields (tail)\n')
        log_string += (f'{next_song_rows.tail()}\n\n')
        log_string += (f'{UNDERLINE_3}')
        logging.debug(log_string)
    except OSError as ose:
        handled_errors += 1
        """
        print(f'Something went wrong filtering rows in:\n {filepath}')
        print(f'OS returned:\n\n{ose}\n\n')
        print(UNDERLINE_1)
        """
        logging.error(f'Something went wrong filtering rows in:\n {filepath}')
        logging.error(f'OS returned:\n\n{ose}\n\n')
        logging.error(UNDERLINE_1)
    except ValueError as ve:
        handled_errors += 1
        """
        print(f'A ValueError occurred filtering rows in:\n {filepath}')
        print(f'Error message is:\n\n{ve}\n\n')
        print(UNDERLINE_1)
        """
        logging.error(f'A ValueError occurred filtering rows in:\n {filepath}')
        logging.error(f'Error message is:\n\n{ve}\n\n')
        logging.error(UNDERLINE_1)
    except NameError as ne:
        handled_errors += 1
        """
        print(f'A NameError occurred filtering rows in:\n {filepath}')
        print(f'Error message is:\n\n{ne}\n\n')
        print(UNDERLINE_1)
        """
        logging.error(f'A NameError occurred filtering rows in:\n {filepath}')
        logging.error(f'Error message is:\n\n{ne}\n\n')
        logging.error(UNDERLINE_1)
    except:
        handled_errors += 1
        """
        print(f'Some other non-OSError occurred filtering rows in:\n{sys.exc_info()[0]}\n')
        """
        logging.debug(f'Some other non-OSError occurred filtering rows in:\n{sys.exc_info()[0]}\n')
            #############################################################################################
            # DEBUG --- Check for sensible looking user table data ...
            #
    """
    if (DEBUG == True):
        for index, row in next_song_rows.iterrows():
            print(f'{index} --- User ID: {row["userId"]}, Name: {row["firstName"]} {row["lastName"]}, Gender: {row["gender"]}, Level: {row["level"]}')
    """
    log_string = '\n  Extracting required data fields (for information ONLY):\n\n'
    for index, row in next_song_rows.iterrows():
        log_string += (f'{index} --- User ID: {row["userId"]}, Name: {row["firstName"]} {row["lastName"]}, Gender: {row["gender"]}, Level: {row["level"]}\n')
    log_string += (f'{UNDERLINE_3}')
    logging.info(log_string)
    """
    print(f'Done\n{UNDERLINE_3}\n\n')
    """
        #############################################################################################
        # Task #3: Populate Time Table
        #
        #  NOTE a): Further reading ...
        #    Apparently, appending rows to a dataframe one at a time is incredibly slow which may be why
        #     the Jupyter notebook (eti.pynb) suggests making a dictionary of all the records first,
        #     then convert that to a dataframe and submit that to psycopg2.
        #
        #  NOTE b): Running some tests on diffeent ways of doing things seems to confirm this. Handling
        #    many dataframes each containing one record seems very slow.
        #
        #  NOTE c) So, look at the format that we need to submit the processed data to the database to
        #    decide a strategy. We'll be using:
        #
        #               for i, row in time_dataframe.iterrows():
        #                   logging.debug(f'Row {i} list(row) is: {list(row)}')
        #                   cursor.execute(time_table_insert, list(row))
        #
        #   We will want each row of our timedata_list to be a tuple of the values of the field names:
        #     i.e. start_time, hour, day, week, month, year, weekday --- as specified in sql_queries.py
        #
        #  NOTE d): The steps in the process become:
        #    (d1) - Set up a Python list to hold the time data records extracted from the file
        #    (d2) - Loop over the dataframe derived from the input file, extracting the records we want
        #    (d3) - Make a Python dictionary from each row in the dataframe
        #    (d4) - Append that to the timedata list
        #    (d5) - Use the list of dictionaries to make a new dataframe (pandas has a convenient method)
        #    (d6) - Use the new dataframe to create database records (again pandas has a convenient method)
        #
    """
    print(f'{UNDERLINE_3}')
    print(f'Setting up a python list of python dictionaries for the time data ...')
    """
    logging.debug(f'{UNDERLINE_3}')
    logging.debug(f'Setting up a python list of python dictionaries for the time data ...')
        #############################################################################################
        #    (d1) - Set up a Python list to hold the time data records extracted from the file
        #
    timedata_list = list()
        #############################################################################################
        #    (d2) - Loop over the dataframe derived from the input file, extracting the records we want
        #
    """
    print(f'... extracting time data')
    """
    logging.debug(f'... extracting time data')
    try:
        for index, row in dataframe.iterrows():
            as_datetime = pd.to_datetime(row["ts"], unit="ms")
                #############################################################################################
                #    (d3) - Make a Python dictionary from each row in the dataframe
                #
            time_dict = dict()
            time_dict["start_time"] = as_datetime
            time_dict["hour"] = as_datetime.hour
            time_dict["day"] = as_datetime.day
            time_dict["week"] = as_datetime.week
            time_dict["month"] = as_datetime.month
            time_dict["year"] = as_datetime.year
            time_dict["weekday"] = as_datetime.day_name()
            """
            if (DEBUG == True):
                print(f'Time (in ms): {row["ts"]}')
                print(f'Start time: {time_dict["start_time"]}')
                print(f'Hour: {time_dict["hour"]}')
                print(f'Day: {time_dict["day"]}')
                print(f'Week number: {time_dict["week"]}')
                print(f'Month: {time_dict["month"]}')
                print(f'Year: {time_dict["year"]}')
                print(f'Weekday: {time_dict["weekday"]}')
                print(f'{UNDERLINE_3}\n')
                #
            print(time_dict)
            """
            log_string = (f'{UNDERLINE_3}\n')
            log_string += (f'Time (in ms): {row["ts"]}')
            log_string += (f'Start time: {time_dict["start_time"]}')
            log_string += (f'Hour: {time_dict["hour"]}')
            log_string += (f'Day: {time_dict["day"]}')
            log_string += (f'Week number: {time_dict["week"]}')
            log_string += (f'Month: {time_dict["month"]}')
            log_string += (f'Year: {time_dict["year"]}')
            log_string += (f'Weekday: {time_dict["weekday"]}')
            log_string += (f'{UNDERLINE_3}\n')
                #
            logging.debug(log_string)

                #############################################################################################
                #    (d4) - Append that to the timedata lisy
                #
        timedata_list.append(time_dict)
    except:
        handled_errors += 1
        """
        print(f'An error occurred when extracting data to the time_dict:\n{sys.exc_info()[0]}\n')
        """
        logging.error(f'An error occurred when extracting data to the time_dict:\n{sys.exc_info()[0]}\n')
            #################################################################################################
            #    (d5) - Use the list of dictionaries to make a new dataframe (pandas has a convenient method)
            #
    try:
        time_dataframe = pd.DataFrame.from_records(timedata_list)
        """
        if (DEBUG == True):
            print(f'\nTime data dataframe ...\n{time_dataframe}\n')
        """
        logging.debug(f'\nTime data dataframe ...\n{time_dataframe}\n')
    except:
        handled_errors += 1
        """
        print(f'An error occurred when converting time_dict to a dataframe:\n{sys.exc_info()[0]}\n')
        """
        logging.error(f'An error occurred when converting time_dict to a dataframe:\n{sys.exc_info()[0]}\n')
            #############################################################################################
            #    (d6) - Use the new dataframe to create database records
            #              (again pandas has a convenient method)
            #
    """
    print(f'Inserting time data rows into database ...')
    """
    logging.debug(f'Inserting time data rows into database ...')
    for i, row in time_dataframe.iterrows():
        try:
            """
            if (DEBUG == True):
                print(f'(Time) Row {i} list(row) is: {list(row)}')
            """
            logging.debug(f'(Time) Row {i} list(row) is: {list(row)}')
            cursor.execute(time_table_insert, list(row))
        except psycopg2.Error as e:
            handled_errors += 1
            """
            print(f'Error saving time data record\n {e}')
            """
            logging.error(f'Error saving time data record\n {e}')
    """
    print(f'\nDone\n{UNDERLINE_3}\n\n')
    """
    logging.debug(f'\nDone\n{UNDERLINE_3}\n\n')
        #############################################################################################
        # Task #4: Populate User Table
        #############################################################################################
        #############################################################################################
        # Load user table ... try loading just the required columns from the file
        #   ... more time than working from the full dataset, but less memory!
        #
        # WARNING!!! - This works using a csv file, but not with JSON ... shrug??
        #
        # So, create a new user_dataframe from the original
        #
    """
    print(f'Extracting a user-data frame\n\n')
    """
    logging.debug(f'Extracting a user-data frame\n\n')
    try:
        user_dataframe = dataframe[['userId', 'firstName', 'lastName', 'gender', 'level']]
        """
        if (True):
            print(f'File: \n {filepath}\n')
            print(f'Number of lines read: {len(user_dataframe.index)}\n')
            print(f'User data fields (head):\n')
            print(user_dataframe.head())
            print('\n... (tail)')
            print(user_dataframe.tail())
        """
        log_string = (f'{UNDERLINE_3}\n')
        log_string += (f'File: \n {filepath}\n')
        log_string += (f'File: \n {filepath}\n')
        log_string += (f'Number of lines read: {len(user_dataframe.index)}\n')
        log_string += (f'User data fields (head):\n')
        log_string += (user_dataframe.head())
        log_string += ('\n... (tail)\n')
        log_string += (user_dataframe.tail())
        log_string += (f'{UNDERLINE_3}\n')
            #
        logging.debug(debug_string)
    except OSError as ose:
        handled_errors += 1
        """
        print(f'Something went wrong creating a sub-dataframe for users.')
        print(f'OS returned:\n\n{ose}\n\n')
        print(UNDERLINE_1)
        """
        logging.error(f'Something went wrong creating a sub-dataframe for users.')
        logging.error(f'OS returned:\n\n{ose}\n\n')
        logging.error(UNDERLINE_1)
    except ValueError as ve:
        handled_errors += 1
        """
        print(f'A ValueError occurred creating a sub-dataframe for users.')
        print(f'Error message is:\n\n{ve}\n\n')
        print(UNDERLINE_1)
        """
        logging.error(f'A ValueError occurred creating a sub-dataframe for users.')
        logging.error(f'Error message is:\n\n{ve}\n\n')
        logging.error(UNDERLINE_1)
    except NameError as ne:
        handled_errors += 1
        """
        print(f'A NameError occurred creating a sub-dataframe for users.')
        print(f'Error message is:\n\n{ne}\n\n')
        print(UNDERLINE_1)
        """
        logging.error(f'A NameError occurred creating a sub-dataframe for users.')
        logging.error(f'Error message is:\n\n{ne}\n\n')
        logging.error(UNDERLINE_1)
    except:
        handled_errors += 1
        """
        print(f'Some other non-OSError occurred creating a sub-dataframe for users.\n{sys.exc_info()[0]}\n')
        """
        logging.error(f'Some other non-OSError occurred creating a sub-dataframe for users.\n{sys.exc_info()[0]}\n')
            #
    """
    print(f'Done\n{UNDERLINE_3}\n\n')
    """
    logging.debug(f'Done\n{UNDERLINE_3}\n\n')
        #############################################################################################
        # Insert user records once again using the iterrows method provided by pandas
        #
    """
    print(f'Inserting user data rows into database ...')
    """
    logging.debug(f'Inserting user data rows into database ...')
    for i, row in user_dataframe.iterrows():
        try:
            """
            if (DEBUG = True):
                print(f'(User) Row {i} list(row) is: {list(row)}')
            """
            logging.debug(f'(User) Row {i} list(row) is: {list(row)}')
            cursor.execute(user_table_insert, list(row))
        except psycopg2.Error as e:
            handled_errors += 1
            """
            print(f'Error saving user data record\n {e}')
            """
            logging.debug(f'Error saving user data record\n {e}')
    """
    print(f'\nDone\n{UNDERLINE_3}\n\n')
    """
    logging.debug(f'\nDone\n{UNDERLINE_3}\n\n')

        #############################################################################################
        # Task #5: Populate 'songplays' Table
        #
        #   FROM etl.ipynb ...
        #   ===================
        #     This one is a little more complicated since information from the songs table, artists
        #     table, and original log file are all needed for the songplays table. Since the log file
        #     does not specify an ID for either the song or the artist, you'll need to get the song ID
        #     and artist ID by querying the songs and artists tables to find matches based on song
        #     title, artist name, and song duration time.
        #############################################################################################
        #############################################################################################
        # For each row in the dataframe (the dataframe derived from log files!!)
        #
    for index, row in dataframe.iterrows():
            #############################################################################################
            # Get songid and artistid from song and artist tables
            #
            #   Remeber: song_select = 'SELECT song_id, artist_id FROM songs JOIN artists ON artist_id
            #                                              WHERE (title = %s, name = %s, duration = %s);'
            #
        #cursor.execute(song_select, (row.song, row.artist, row.length))
        cursor.execute(song_select)
        """
        print(f'Song title: {row.song}, Artist ID {row.artist} => {cursor.fetchone()}')
        """
        logging.debug(f'Song title: {row.song}, Artist ID {row.artist} => {cursor.fetchone()}')
        """
        results = cursor.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
        """
            #############################################################################################
            # Insert songplay record
            #   REMEMBER: - Table 'songplays' has fields:
            #     songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
        #songplay_data =
        #cur.execute(songplay_table_insert, songplay_data)
    """
    print('\n\nprocess_log_file complete')
    print(f'{UNDERLINE_1}\n')
    """
    logging.debug('\n\nprocess_log_file complete')
    logging.debug(f'{UNDERLINE_1}\n')

def process_data(cursor, connection, filepath, func):
    global handled_errors
    logging.info(f'\n  Entering process_data for path: {filepath} with: {func}\n{UNDERLINE_3}')
        #############################################################################################
        # Get all files matching extension from directory using os and glob
        #
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for file in files :
            all_files.append(os.path.abspath(file))
        #############################################################################################
        # get total number of files found
        #
    num_files = len(all_files)
    """
    if DEBUG == True:
        print(f'{num_files} files found in {filepath}')
        print(f'{UNDERLINE_1}\n')
    """
    logging.info(f'\n  {num_files} files found in path: {filepath}\n{UNDERLINE_2}')

        #############################################################################################
        # iterate over files and process
        #
    for i, datafile in enumerate(all_files, 1):
        func(cursor, datafile)
        connection.commit()
        """
        if DEBUG == True:
            print(f'{i}/{num_files} files processed.')
            logging.debug(f'{UNDERLINE_1}\n')
        """
        logging.info(f'\n  {i}/{num_files} files processed.\n{UNDERLINE_2}')
    logging.info(f'\n  Leaving process_data for path: {filepath} with: {func}\n{UNDERLINE_1}')

def main():
    global handled_errors
    """
    print("\nWe're at the beginning ...\n\n")
    """
    logging.info(f'\n  We\'re at the beginning ...\n{UNDERLINE_3}')
        #############################################################################################
        # Create and connect to the sparkifydb
        # Returns the connection and cursor to sparkifydb
        #############################################################################################
        # Create a connection to postgreSQL
        #
    try:
        sparkify_connection = psycopg2.connect("host=127.0.0.1 dbname=sparkify user=ajb password=hsc1857")
        """
        print(f'Connection open\n{sparkify_connection}\n\n')
        """
        logging.info(f'\n  Connection open\n  {sparkify_connection}\n{UNDERLINE_3}')
    except psycopg2.Error as e:
        handled_errors += 1
        """
        print(f'Error trying to open connection\n {sparkify_connection}')   # NOTE: the use of string interpolation; 'f-strings' in Python
        print(e)
        print(f'{UNDERLINE_1}\n')
        """
        logging.error(f'Error trying to open connection\n {sparkify_connection}')   # NOTE: the use of string interpolation; 'f-strings' in Python
        logging.error(e)
        logging.error(f'{UNDERLINE_1}\n')
          #############################################################################################
          # Use that conection to get a 'cursor' that can be used to execute queries
          #
    try:
        sparkify_cursor = sparkify_connection.cursor()
        """
        print("Sparkify cursor active\n")
        """
        logging.info(f'\n  Sparkify cursor active\n{UNDERLINE_1}')
    except psycopg2.Error as e:
        handled_errors += 1
        """
        print(f'Error obtaining a cursor on connection\n {sparkify_connection}')
        print(e)
        """
        logging.debug(f'Error obtaining a cursor on connection\n {sparkify_connection}')
        logging.debug(e)
          #############################################################################################
          # Specify the root path for the song data files
          #

    process_data(sparkify_cursor, sparkify_connection, filepath='data/song_data', func=process_song_file)
    process_data(sparkify_cursor, sparkify_connection, filepath='data/log_data', func=process_log_file)

        #############################################################################################
        #
        # Do a clean shutdown of the cursor and connection
        #
    try:
        sparkify_cursor.close()
        """
        print("Cursor closed")
        """
        logging.info(f'\n  Sparkify cursor closed\n{UNDERLINE_3}')
    except psycopg2.Error as e:
        handled_errors += 1
        """
        print(f'Error when trying to close the cursor\n')
        print(e)
        print(f'{UNDERLINE_1}\n')
        """
        logging.error(f'Error when trying to close the cursor\n')
        logging.error(e)
        logging.error(f'{UNDERLINE_1}\n')
            #
            # Close the connection
            #
    try:
        sparkify_connection.close()
        """
        print("Connection closed\n")
        """
        logging.info(f'\n  Sparkify connection closed\n{UNDERLINE_3}')
    except psycopg2.Error as e:
        handled_errors += 1
        """
        print(f'Error when trying to close the connection\n')
        print(e)
        print(f'{UNDERLINE_1}\n')
        """
        logging.error(f'Error when trying to close the connection\n')
        logging.error(e)
        logging.error(f'{UNDERLINE_1}\n')

    """
    print(f'Handled errors encountered: {handled_errors}')
    print("We've got to the end!!\n\n")
    print(f'{UNDERLINE_2}\n\n')
    """
    logging.debug(f'Handled errors encountered: {handled_errors}')
    logging.debug("We've got to the end!!\n\n")
    logging.debug(f'{UNDERLINE_2}\n\n')


if __name__ == "__main__":
    main()
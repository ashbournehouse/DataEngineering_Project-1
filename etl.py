"""
21/05/2021 - Udacity Data Engineering - ETL Script
============================================================

07/07/2021 - Reviewed for submission

  Standalone version etl script.

  (ETL = 'Extract, Transform, Load')

  This script extracts data from the songs and logs files
    then uses that data to populate the following tables:
     - songs
     - artists
     - time
     - users
     - songplays

  Part of my submission for:
    Udacity Data Engineering - Project 1 'Modelling in Postgres'

"""

"""
Imports
=======

  os - provides 'directory walk' capabilities amongst other
  sys - can help in interpreting errors
  glob - allows pattern matching using wildcards
  psycopg2 - provided interaction with PostgreSQL
  json - allows conversion into and out of json object format
  pandas - python data analysis and manipulation tool
  numpy - provides 'scientific computing' capabilities
  sql-queries - local source of sql queries used here

"""

import os
import sys
import glob
import psycopg2
import json
import pandas as pd
import numpy as np
from sql_queries import *

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

  Handling so much data, especially when in a learning/development
    environment I'm realising that I need a capable logger to help
    with debugging and learning. The 'debug print' approach doesn't
    really cut it.

    Acknowledging online tutorials from Corey Scaher, starting here:
       https://www.youtube.com/watch?v=-ARI4Cz-awo

  REMEMBER:
    There are five (Python) logging levels provided (in proiority
        order):

    - DEBUG ...... detailed info, typically only of interest when
          diagnosing problems
    - INFO ....... confirmation that things are working as expected
    - WARNING .... an indication that something unexpected happened
    - ERROR ...... a serious problem, the software has not been able
          to do something
    - CRITICAL ... a serious error where the software may be unable
          to continue
"""

import logging
logging.basicConfig(level=logging.INFO)


"""
  Using a global variable to count handled errors is clunky but ...
  ... pragmatic here as it saves hunting through lots of output
  when there are no error messages to find.
"""

handled_errors = 0

songs_saved = 0
song_duplicates = 0

artists_saved = 0
artist_duplicates = 0

times_saved = 0
time_duplicates = 0

users_saved = 0
user_duplicates = 0

songplays_saved = 0
songplays_duplicates = 0

song_select_finds = 0
song_select_responses = []


def process_song_file(cursor, filepath):

    """
    Opens the song file specified in 'filepath' and converts it
      to a pandas dataframe

     - Obtains a 'cursor' to allow submission of queries
     - Perfoms a 'kill and rebuild' of the 'sparkify' database by:
       * Droping the 'sparkify' database if it exists.
       * Creating a new 'sparkify' database
       * Closing the connection to the default database
       * Obtaining a connection to the sparkify database
       * Obtaining a cursor to the sparkify database

    Parameters:

     - cursor: a cursor object to the database
     - filepath: the absolute path to the song file to be processed

    Returns: none

    """

    global handled_errors

    global songs_saved
    global song_duplicates

    global artists_saved
    global artist_duplicates

    """
      NOTE: When using pandas.read_json with a single line of json
        data per file, then it is necessary to specify typ='series'

      Open and process the data file using a Pandas data series. As
        there is one song per file, the returned dataframe has only
        one row so a single subscript is used to access values.

    """

    logging.debug(
        f'\n'
        f'  Entering process_song_file for: '
        f'{os.path.basename(filepath)}\n'
        f'{UNDERLINE_3}')
    try:
        dataseries = pd.read_json(filepath, typ='series')
    except Exception:       # recommedned by PEP8
        handled_errors += 1
        logging.error(
            f'\n'
            f'  Something went wrong converting to a dataseries for:\n'
            f'    {os.path.basename(filepath)}\n'
            f'{UNDERLINE_1}')

    """
      Task #1: Populate Songs Table
      =============================

        Copying the dataseries fields to named variables is not
          necessary but makes the code more comprehensible.

        NOTE a): single quotes are a problem in postgreSQL
          queries so replace them with two quotes where they're
          likely to occur

        NOTE b): rather than a string containing the field
          values for the song_data, we need to make a tuple, so
          use a cast
    """
    try:
        num_songs = dataseries.iloc[0]
        artist_id = dataseries.iloc[1]
        artist_latitude = dataseries.iloc[2]
        artist_longitude = dataseries.iloc[3]
        artist_location = dataseries.iloc[4].replace("'", "''")
        artist_name = dataseries.iloc[5].replace("'", "''")
        song_id = dataseries.iloc[6]
        title = dataseries.iloc[7].replace("'", "''")
        duration = dataseries.iloc[8]
        year = dataseries.iloc[9]

        song_data = tuple([song_id, title, artist_id, year, duration])
        logging.debug(f'\n  song_data tuple is: {song_data}')
    except Exception:       # recommedned by PEP8
        handled_errors += 1
        logging.error(
            f'\n'
            f'  Something went wrong building the song_data tuple\n'
            f'{UNDERLINE_1}')

    """
      With everything prepared, insert the song data into the database
    """
    try:
        cursor.execute(song_table_insert, song_data)
        songs_saved += 1
        logging.debug(
            f'\n'
            f'  Song data record saved to database')
    except psycopg2.Error as e:
        if ('duplicate key' in str(e)):
            song_duplicates += 1
            logging.warning(
                f'\n'
                f'Database reports a duplicate key for song: '
                f'{title}\n'
                f'{UNDERLINE_3}'
                )
        else:
            handled_errors += 1
            logging.error(
                f'\n'
                f'  Error saving song data record\n'
                f'    {e}\n'
                f'{UNDERLINE_3}'
                )
    """
      Task #2: Populate Artists Table
      ===============================

        Use the same basic techniques as used for the song data of
          building a tuple from sensibly named variables.

    """
    try:
        artist_data = tuple([artist_id, artist_name, artist_location,
            artist_latitude, artist_longitude])
        logging.debug(
            f'\n'
            f'  artist_data tuple is: {artist_data}')
    except Exception:       # recommedned by PEP8
        handled_errors += 1
        logging.error(
            f'\n'
            f'Something went wrong building the artist_data tuple\n'
            f'{UNDERLINE_1}')

    """
      With everything prepared, insert the song data into the database.

      QUESTION:
      =========
        Do we want duplicates in the artists table?

        If the answer is no, we could check to see if an artist
          already exists in the artists table, but with few
          'duplicate artists' expected it is sugfficient to place
          a 'unique' constraint on the 'artist_id' field in the
          database.
    """

    try:
        cursor.execute(artist_table_insert, artist_data)
        artists_saved += 1
        logging.debug(
            f'\n'
            f'  Artist data record saved to database')
    except psycopg2.Error as e:
        if ('duplicate key' in str(e)):
            artist_duplicates += 1
            logging.warning(
                f'\n'
                f'Database reports a duplicate key for artist: '
                f'{artist_name}\n'
                f'{UNDERLINE_3}'
                )
        else:
            handled_errors += 1
            logging.error(
                f'\n'
                f'  Error saving artist data record\n'
                f'    {e}\n'
                f'{UNDERLINE_3}'
                )
    logging.debug(
        f'\n'
        f'  process_song_file completefor: {os.path.basename(filepath)}\n'
        f'{UNDERLINE_3}\n')


def process_log_file(cursor, filepath):

    """
    This processes one log file using pandas to open, convert (to a
      dataframe) and process the contents.

    NOTE: Unlike the songs files each log file contains multiple
      lines of user activity data.

    NOTE: When using pandas.read_json with a multiple lines of
      json data per file, then it is necessary to specify
      lines=True

    Parameters:

     - cursor: a cursor object to the database
     - filepath: the absolute path to the log file to be processed

    Returns: none

    """

    global handled_errors

    global times_saved
    global time_duplicates

    global users_saved
    global user_duplicates

    global songplays_saved
    global songplays_duplicates

    global song_select_finds
    global song_select_responses


    """
      Use pandas to open the log file ...
    """

    logging.debug(f'\n\
      Entering process_log_file for: \
      {os.path.basename(filepath)}\n{UNDERLINE_3}')
    try:
        dataframe = pd.read_json(filepath, lines=True)
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
    except Exception:       # recommedned by PEP8
        handled_errors += 1
        logging.error(
            f'\n'
            f'  Some other non-OSError occurred reading:\n'
            f'    {os.path.basename(filepath)}'
            f'{UNDERLINE_1}\n')

    """
      Filter by NextSong action to remove unwanted data

      NOTE: This is placing quite a lot of data in memory, so
        perhaps expect problems if the log files are huge ...
    """

    try:
        next_song_rows = dataframe.loc[dataframe["page"] == "NextSong"]
        logging.debug(
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
    except Exception:       # recommedned by PEP8
        handled_errors += 1
        logging.error(
            f'\n'
            f'Some error occurred filtering rows in:\n'
            f'    dataframe from: {os.path.basename(filepath)}\n'
            f'{UNDERLINE_3}'
            )

    """
      DEVELOPEMNT DEBUG --- Check for sensible looking user table data
      vvvvvvvvvvvvvvvvv
    """
    """
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
    """
    """
      ^^^^^^^^^^^^^^^^^
      End of DEVELOPMENT DEBUG
    """

    """
      Task #3: Populate Time Table
      ============================

        NOTE a): Further reading ...
          Apparently, appending rows to a dataframe one at a time is
           incredibly slow which may be why the Jupyter notebook
           (eti.pynb) suggests making a dictionary of all the records
           first, then convert that to a dataframe and submit that to
           psycopg2.

        NOTE b): Running some tests on diffeent ways of doing things
          seems to confirm this. Handling many dataframes each
          containing one record seems very slow.

        NOTE c) So, look at the format that we need to submit the
          processed data to the database to decide a strategy.
          We'll be using (something like):

              for i, row in time_dataframe.iterrows():
                   logging.debug(f'Row {i} list(row) is: {list(row)}')
                   cursor.execute(time_table_insert, list(row))

         We will want each row of our timedata_list to be a tuple of
           the values of the field names:
             i.e. start_time, hour, day, week, month, year, weekday
                --- as specified in sql_queries.py

        NOTE d): The steps in the process become:
          (d1) - Set up a Python list to hold the time data records
                   extracted from the file
          (d2) - Loop over the dataframe derived from the input file,
                   extracting the records we want
          (d3) - Make a Python dictionary from each row in the
                   dataframe
          (d4) - Append that to the timedata list
          (d5) - Use the list of dictionaries to make a new dataframe
                   (pandas has a convenient method)
          (d6) - Use the new dataframe to create database records
                  (again pandas has a convenient method)

    """
    log_string = (
        f'\n'
        f'  Setting up a python list of python dictionaries for the'
        f' time data ...\n'
        )

    """
      (d1) - Set up a Python list to hold the time data
        records extracted from the file
    """
    timedata_list = list()

    """
      (d2) - Loop over the dataframe derived from the input file,
               extracting the records we want
    """

    log_string += (
        f'    ... extracting time data for each record\n\n'
        )
    loopcount = 0
    try:
        for index, row in dataframe.iterrows():
            as_datetime = pd.to_datetime(row["ts"], unit="ms")
            """
              (d3) - Make a Python dictionary from each row in
                the dataframe
            """
            time_dict = dict()
            time_dict["start_time"] = as_datetime
            time_dict["hour"] = as_datetime.hour
            time_dict["day"] = as_datetime.day
            time_dict["week"] = as_datetime.week
            time_dict["month"] = as_datetime.month
            time_dict["year"] = as_datetime.year
            time_dict["weekday"] = as_datetime.day_name()
            """
              Add to the log_string for each record processed
            """
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
            """
              (d4) - Append that to the timedata list

                !! Dangerous Python indentation ... easy to get the
                     next statement outside the loop!!
            """
            timedata_list.append(time_dict)
        log_string += (
            f'\n  Number of records processed: {loopcount}\n'
            f'\n  Number of records added to timedata_list: '
            f'{len(timedata_list)}\n'
            f'{UNDERLINE_3}'
            )
        logging.debug(UNDERLINE_3)
    except Exception:       # recommedned by PEP8
        handled_errors += 1
        logging.error(
            f'\n'
            f'  An error occurred when extracting data to the '
            f'time_dict:\n'
            f'{UNDERLINE_3}')
    """
      (d5) - Use the list of dictionaries to make a new
               dataframe (pandas has a convenient method)
    """
    try:
        time_dataframe = pd.DataFrame.from_records(timedata_list)
        logging.debug(
            f'\n'
            f'Time data dataframe ...\n'
            f'{time_dataframe.to_string()}\n'
            f'{UNDERLINE_3}'
            )
    except Exception:       # recommedned by PEP8
        handled_errors += 1
        logging.error(
            f'\n'
            f'  An error occurred when converting time_dict to a '
            f'dataframe:\n'
            f'{UNDERLINE_3}'
            )
    """"
      (d6) - Use the new dataframe to create database records
               (again pandas has a convenient method)
    """
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
            times_saved += 1
            loopcount += 1
        except psycopg2.Error as e:
            if ('duplicate key' in str(e)):
                time_duplicates += 1
                logging.warning(
                    f'\n'
                    f'Database reports a duplicate key for time: '
                    f'{list(row)[0]}\n'
                    f'{UNDERLINE_3}'
                    )
            else:
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
    logging.debug(log_string)

    """
      Task #4: Populate User Table
      ============================

        Load user table ... try loading just the required columns from
          the file ... more time than working from the full dataset,
          but less memory!

        WARNING!!! - This works using a csv file, but not with JSON
          ... this is as documented but I still wonder why??

        So, create a new user_dataframe from the next_song_rows
          dataframe

        NOTE: You need to be logged in to request a next-song, so
                we'll still get all the users.

    """
    try:
        """
          If we don't want duplicate user records in the database ...

          NOTE: This will ONLY stop us creating duplicate user
            records from a given file. We need a constraint on the
            database when we create it fully to prevent duplicates.

          Nonetheless: not attempting to save know duplicates
            will save resources.
        """
        user_dataframe = next_song_rows[['userId',
                                         'firstName',
                                         'lastName',
                                         'gender',
                                         'level']].drop_duplicates(
                                            subset=['userId'])
        logging.debug (
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
    except Exception:       # recommedned by PEP8
        handled_errors += 1
        logging.error(
            f'\n'
            f'  Some error occurred creating a sub-dataframe for '
            f'users.\n\n'
            f'{UNDERLINE_3}'
            )
    """
      Insert user records once again using the iterrows method
        provided by pandas
    """
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
            users_saved += 1
        except psycopg2.Error as e:
            if ('duplicate key' in str(e)):
                user_duplicates += 1
                logging.warning(
                    f'\n'
                    f'Database reports a duplicate key for user: '
                    f'{list(row)[0:3]}\n'
                    f'{UNDERLINE_3}'
                    )
            else:
                handled_errors += 1
                logging.error(
                    f'\n'
                    f'  Error saving user data record\n {e}'
                    f'{UNDERLINE_3}'
                    )
    logging.debug(log_string)

    """
      Task #5: Populate 'songplays' Table
      ===================================

        FROM etl.ipynb ...
        ===================
          This one is a little more complicated since information
          from the songs table, artists table, and original log file
          are all needed for the songplays table. Since the log file
          does not specify an ID for either the song or the artist,
          you'll need to get the song ID and artist ID by querying
          the songs and artists tables to find matches based on song
          title, artist name, and song duration time.

       For each row in the dataframe (the dataframe derived from
         log files!!)
    """
    log_string = (
        f'\n'
        f'Select all songs, just to make sure that there are some ...\n'
        )
    cursor.execute("SELECT song_id, title FROM songs")
    for row in cursor:
        log_string += (
            f'{cursor.fetchone()}\n'
            )
    log_string += (UNDERLINE_2)
    logging.info(log_string)

    logging.warning(
        f'\n'
        f'  next_song_rows dataframe looks like this: \n\n'
        f'{next_song_rows.head()}'
        f'{UNDERLINE_3}'
        )

    log_string = (
        f'\n'
        f'  Retrieving song and artist IDs using an SQL query ...\n\n'
        )
    """
      REMEMBER it's the next_song_rows dataframe !!!

        This has fields:
          "artist":
          "auth":
          "firstName":
          "gender":
          "itemInSession":
          "lastName":
          "length":
          "level":
          "location":
          "method"
          "page":
          "registration":
          "sessionId":
          "song":
          "status":
          "ts":
          "userAgent":
          "userId":
    """
    for index, row in next_song_rows.iterrows():

        """
          Get song_id and artist_id from song and artist tables

            Remember:
              song_select = 'SELECT song_id, artist_id
                             FROM songs JOIN artists
                             ON songs.artist_id = artists.artist_id
                             WHERE title = (%s)
                             AND name = (%s)
                             AND duration = (%s);
        """
        query_values = tuple([row.song, row.artist, row.length])
        """
          Check the composed sql query
        """
        logging.debug(
            f'\n'
            f'  Composed SQL query is: \n'
            f'{cursor.mogrify(song_select, query_values)}\n'
            f'{UNDERLINE_3}'
            )
        """
          Execute the query
        """
        try:
            cursor.execute(song_select, query_values)
            response = cursor.fetchone()
            if response:                  # == if response is not None
                logging.warning(
                    f'\n'
                    f'  For title: {row.song}, artist: {row.artist} IDs are: '
                    f'{response}\n'
                    f'{UNDERLINE_3}'
                    )
                song_select_finds += 1
                song_select_responses.append(response)

                song_id, artist_id = response
            else:
                song_id = None
                artist_id = None

        except psycopg2.Error as e:
            handled_errors += 1
            logging.error(
                f'\n'
                f'  Error running select query: \n'
                f'    {cursor.mogrify(song_select, query_values)}\n'
                f'  Error reurned by psycopg2: \n'
                f'    {e}\n'
                f'{UNDERLINE_3}'
                )

        """
          Insert songplay record
            REMEMBER: - Table 'songplays' has fields:
                 songplay_id, start_time, user_id, level, song_id,
                   artist_id, session_id, location, user_agent
        """
        songplay_data = tuple([
                            pd.to_datetime(row['ts']),
                            row['userId'],
                            row['level'],
                            song_id,
                            artist_id,
                            row['sessionId'],
                            row['location'],
                            row['userAgent']
                            ])
        try:
            cursor.execute(songplay_table_insert, songplay_data)
            songplays_saved += 1
        except psycopg2.Error as e:
            handled_errors += 1
            logging.error(
                f'\n'
                f'  Error saving songplay data record\n {e}'
                f'{UNDERLINE_3}'
                )
    logging.debug(
        f'\n'
        f'  process_log_file complete for file: '
        f'{os.path.basename(filepath)} \n'
        f'{UNDERLINE_3}'
        )


def process_data(cursor, connection, filepath, func):

    """
    Perform a directory walk on the specified filepath and
      for each file found, submit the absolute filepath to
      the specfied function.

    Parameters:

     - cursor: a cursor object to the database
     - connection: a connection object to the database
     - filepath: the absolute path to the song file to be processed
     - func: the function to be called by this function

    Returns: none

    """
    global handled_errors

    logging.debug(
        f'\n'
        f'  Entering process_data for path: {filepath} with: {func}\n'
        f'{UNDERLINE_3}'
        )
    logging.debug(
        f'\n'
        f'  Absolute path is:\n  {os.path.abspath(filepath)}\n'
        f'{UNDERLINE_3}'
        )
    """
      Get all files matching extension from directory using os
        and glob
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for file in files:
            all_files.append(os.path.abspath(file))
    """
      Get (and log) total number of files found
    """
    num_files = len(all_files)
    logging.info(
        f'\n'
        f'  {num_files} files found in path: '
        f'{os.path.basename(filepath)}\n'
        f'{UNDERLINE_2}'
        )
    """
      Iterate over files and process
    """
    for i, datafile in enumerate(all_files, 1):
        func(cursor, datafile)
        logging.info(
            f'\n'
            f'  {os.path.basename(datafile)} complete: {i}/{num_files} files processed.\n'
            f'{UNDERLINE_2}'
            )
    logging.info(
        f'\n'
        f'  Leaving process_data for path: '
        f'{os.path.basename(filepath)} with: {func}\n'
        f'{UNDERLINE_1}'
        )


def main():

    """
    Main is the entry point to the script. It performs a
      connection to the database and obtains a cursor. It
      then uses the 'process-data' function firstly to
      perform tasks 1 & 2 using the songs files as their
      data source, then uses 'process-data' again to perform
      tasks 3, 4 & 5 using the activity log files.

    After all the sourcefiles are porcessed and database
      tables updated, the cursor and database connections are
      closed.

    Parameters: none

    Returns: none

    """
    global handled_errors

    logging.info(
        f'\n'
        f'  We\'re at the beginning ...\n'
        f'{UNDERLINE_3}'
        )
    """
      Connect to the sparkify database and obtain a cursor
    """
    try:
        sparkify_connection = psycopg2.connect(
            'host=127.0.0.1 dbname=sparkify user=student password=student'
            )
        sparkify_connection.set_session(autocommit=True)
        logging.debug(
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
            f'  Error raised is: \n'
            f'    {e}\n'
            f'{UNDERLINE_1}'
            )
    try:
        sparkify_cursor = sparkify_connection.cursor()
        logging.debug(
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
            f'  Error raised is: \n'
            f'    {e}\n'
            f'{UNDERLINE_1}'
            )
    """
      TASKS #1 & #2
      =============
        Process song datafiles and populate songs table and
          artists table.
    """
    song_datapath = 'data/song_data'
    process_data(sparkify_cursor,
        sparkify_connection,
        filepath=song_datapath,
        func=process_song_file
        )
    try:
        sql = 'SELECT count(*) from songs'
        sparkify_cursor.execute(sql)
        logging.debug(
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
            f'  Error raised is: \n'
            f'    {e}\n'
            f'{UNDERLINE_1}'
            )
    try:
        sql = 'SELECT count(*) from artists'
        sparkify_cursor.execute(sql)
        logging.debug(
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
            f'  Error raised is: \n'
            f'    {e}\n'
            f'{UNDERLINE_1}'
            )
    """
      TASKS #3, #4 & #5
      =================
        Process log datafiles and populate time table, users table
          and songplays table.
    """
    logs_datapath = 'data/log_data'
    process_data(sparkify_cursor,
        sparkify_connection,
        filepath=logs_datapath,
        func=process_log_file
        )
    """
      Do a clean shutdown of the cursor and connection
    """
    try:
        sparkify_cursor.close()
        logging.debug(
            f'\n'
            f'  Sparkify cursor closed\n'
            f'{UNDERLINE_3}'
            )
    except psycopg2.Error as e:
        handled_errors += 1
        logging.error(
            f'\n'
            f'  Error when trying to close the cursor\n'
            f'  Error raised is: \n'
            f'    {e}\n'
            f'{UNDERLINE_1}'
            )
    try:
        sparkify_connection.close()
        logging.debug(
            f'\n'
            f'  Sparkify connection closed\n'
            f'{UNDERLINE_3}'
            )
    except psycopg2.Error as e:
        handled_errors += 1
        logging.error(
            f'\n'
            f'  Error when trying to close the connection\n'
            f'  Error raised is: \n'
            f'    {e}\n'
            f'{UNDERLINE_1}'
            )
    """
      Say goodbye and log summary warnings
    """
    logging.info(
        f'\n'
        f'  Song duplicates encountered: {song_duplicates}\n'
        f'  Artist duplicates encountered: {artist_duplicates}\n'
        f'  Time duplicates encountered: {time_duplicates}\n'
        f'  User duplicates encountered: {user_duplicates}\n'
        f'  Songplays duplicates encountered: {songplays_duplicates}\n\n'
        f'  \'not None\' returns from song select: {song_select_finds}\n\n'
        f'    {song_select_responses}\n\n'
        f'  Handled errors encountered: {handled_errors}\n\n'
        f'  Songs saved to database: {songs_saved}\n'
        f'  Artists saved to database: {artists_saved}\n'
        f'  Times saved to database: {times_saved}\n'
        f'  Users saved to database: {users_saved}\n'
        f'  Songplays saved to database: {songplays_saved}\n\n'
        f'  We\'ve got to the end!!\n\n'
        f'{UNDERLINE_2}\n\n'
        )


if __name__ == "__main__":
    main()
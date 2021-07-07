# Sparkify Data Modelling

***
> *This is a submission for the Udacity Data Engineering 'nanodegree' project entitled **'Data Modelling with Postgres'.***
***

## Purpose

Information on both the songs availaible on Sparkify and the users interactions
with Sparkify are available in two sets of data files:

* song data files
* log data files

Each of these stores data in [.json format](https://www.json.org/json-en.html).

The purpose of this data model is to extract relevant information from these source data files and arrange it in an SQL database (in this case PostgresSQL) using the *'fact and dimension'* tables paradigm. This makes the information contained in the source files more readily available for useful, commercial analysis.

***

## Fact and Dimension Tables

Two of the *'dimension'* tables are generated from the collection of song data files, these are:

### Songs Table

This comprises the following fields:

* song_id
* title
* artist_id
* year
* duration

The artist_id field is used as a foreign key to the artists table.

### Artists Table

This comprises the following fields:

* artist_id
* name
* location
* latitude
* longitude

***

The remaining two *'dimension'* tables are generated from the log data files, they are:

### Users Table

This comprises the following fields:

* user_id
* first_name
* last_name
* gender
* level

'level' is the subscription level (free or paid) of the user.

### Time Table

This comprises the following fields:

* start_time
* hour
* day
* month
* year
* weekday

All fields except the 'start_time' are technically redundant as all the information is carried the 'start_time', which is a timestamp. However, breaking out that information makes it more usable in queries. For example if you were to want to find the average number of songs played by users in each month and how it is changing, the inclusion of a month and year fields makes this already complex query easier to write ... and to read.

***

The remaining table is the only *'fact'* table included in this schema.

### Songplays Table

This comprises the following fields:

* songplay_id
* start_time
* user_id
* level
* song_id
* artist_id
* session_id
* location
* user_agent

#### songplay_id

There is no obvious data field in the source data files to use for the songplay_id but we can get PostgreSQL to generate one by using the [BIGSERIAL pseudo data type](https://www.postgresqltutorial.com/postgresql-serial/) in the create table query which will then begin like this:

>     CREATE TABLE IF NOT EXISTS songplays(songplay_id BIGSERIAL, start_time ...

And the insert query needs to call for the default value in this field, like this:

>     INSERT INTO songplays(songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s);

***

## Pandas

[Pandas](https://pandas.pydata.org) is:

>  a fast, powerful, flexible and easy to use open source data analysis and manipulation tool, built on top of the Python programming language.

In this application we make extensive use of [Pandas dataframes](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html) which are described as:

> Two-dimensional, size-mutable, potentially heterogeneous tabular data.
>
> Data structure also contains labeled axes (rows and columns). Arithmetic operations align on both row and column labels. Can be thought of as a dict-like container for Series objects. The primary pandas data structure.

Essentially Pandas dataframeas can be visualised as 'in memory' spreadsheet workbooks. They offer many facilites for manipulating data incuding row and column selection and iteration over rows. The row iteration facility makes it easy to execute an INSERT query for each row of a dataframe; once the dataframe has been filtered to include only those columns matching the corresponding database table definition.

***

## Source Data File Descriptions

### Song Data Files

Song data files adopt the same format as the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/) with one song per file. Filenames are unique but not instantly meaningful to a human reader.

When reading a single line .json file using pandas 'read_json' method, it is necessary to declare the datatype as 'series', like this:

    dataseries = pd.read_json(filepath, typ='series')

### Log Data Files

Log data files contain information on user interactions with the Sparkify system including log in and log ut interactions, as well as requests to play a song. There is one log data file for each day and they are named in the format:

> yyyy-mm-dd-events.json

When reading a multi-line .json file using pandas 'read_json' method, it is necessary to declare the datatype with 'lines=True', like this:

    dataframe = pd.read_json(filepath, lines=True)

***

## ETL Pipeline Summary

The [ETL](https://www.matillion.com/what-is-etl-the-ultimate-guide/) pipeline used is a multi-stage process that can be summarised as follows:

> * Connect to the Sparkify database and obtain a cursor.

#### Populate the *'dimension'* tables

> * Scan the data/song_data path and process each song files found:
>
>> *  Extract and order the fields required by songs table.
>> *  Insert these as a row into the sparkify database table 'songs'
>
>> * Extract and order the fields required by the artists table.
>> * Insert these as a row into the sparkify database table 'artists'.
>
> * Scan the data/log_data path and process each log file found:
>
>> * Create a pandas dataframe and filter for 'nextsong' records.
>
>> * Extract the timestamp data and transform to fill other time fields.
>> * Insert as a row to the sparkify database table 'time'.
>
>> * Extract the user data fields required by the users table.
>> * Insert as a row to the sparkify database table 'users'.

#### Populate the *'fact'* table

> * Run a QUERY on a JOIN between the songs and artists table to obtain the song_id and artist_id from the song title and artist name included in each log row (of type page=nextsong).
> * Add these data fields to the other required fields from the log data file.
> * Add a songplay_id field using the SERIAL pseudo-datatype in PostgreSQL.
> * Insert as a row to the sparkify database table 'songplays'.
>
>> **NOTE: Using the test/development dataset only one valid song_id, artist_id pair is returned in this process. For all other rows these fields are filled with 'None' values.**

***

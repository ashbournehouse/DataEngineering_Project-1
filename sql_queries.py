	#
	###################################################################################################################################
	# DROP TABLES
songplay_table_drop = f'DROP TABLE IF EXISTS songplays'
user_table_drop = f'DROP TABLE IF EXISTS users'
song_table_drop = f'DROP TABLE IF EXISTS songs'
artist_table_drop = f'DROP TABLE IF EXISTS artists'
time_table_drop = f'DROP TABLE IF EXISTS time'
	#
	###################################################################################################################################
	# CREATE TABLES
	#
	#   01 - Table 'songplays' has fields: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
	#
songplay_fields = ''
songplay_fields = songplay_fields + 'songplay_id int, '
songplay_fields = songplay_fields + 'start_time timestamp, '
songplay_fields = songplay_fields + 'user_id int, '
songplay_fields = songplay_fields + 'level varchar, '
songplay_fields = songplay_fields + 'song_id varchar, '
songplay_fields = songplay_fields + 'artist_id varchar, '
songplay_fields = songplay_fields + 'session_id int, '
songplay_fields = songplay_fields + 'location varchar, '
songplay_fields = songplay_fields + 'user_agent text'
	#
songplay_table_create = f'CREATE TABLE IF NOT EXISTS songplays ({songplay_fields})'
	#
	#   02 - Table 'users' has fields: user_id, first_name, last_name, gender, level
	#
user_fields = ''
user_fields = user_fields + 'user_id int, '
user_fields = user_fields + 'first_name varchar, '
user_fields = user_fields + 'last_name varchar, '
user_fields = user_fields + 'gender varchar, '
user_fields = user_fields + 'level varchar'
	#
user_table_create = f'CREATE TABLE IF NOT EXISTS users ({user_fields})'
	#
	#   03 - Table 'songs' has fields: song_id, title, artist_id, year, duration
	#
song_fields = ''
song_fields = song_fields + 'song_id varchar, '
song_fields = song_fields + 'title varchar, '
song_fields = song_fields + 'artist_id varchar, '
song_fields = song_fields + 'year int, '
song_fields = song_fields + 'duration NUMERIC(10,5)'      # NUMERIC(precision, scale)
	#
song_table_create = f'CREATE TABLE IF NOT EXISTS songs ({song_fields})'
	#
	#   04 - Table 'artists' has fields: artist_id, name, location, latitude, longitude
	#
artist_fields = ''
artist_fields = artist_fields + 'artist_id varchar, '
artist_fields = artist_fields + 'name varchar, '
artist_fields = artist_fields + 'location varchar, '
artist_fields = artist_fields + 'latitude NUMERIC(8,5), '      # NUMERIC(precision, scale)
artist_fields = artist_fields + 'longitude NUMERIC(8,5)'       # NUMERIC(precision, scale)
	#''
artist_table_create = f'CREATE TABLE IF NOT EXISTS artists ({artist_fields})'

	#
	#   05 - Table 'time' has fields: start_time, hour, day, week, month, year, weekday
	#
timestamp_fields = ''
timestamp_fields = timestamp_fields + 'start_time timestamp, '
timestamp_fields = timestamp_fields + 'hour int, '
timestamp_fields = timestamp_fields + 'day int, '
timestamp_fields = timestamp_fields + 'week int, '
timestamp_fields = timestamp_fields + 'month int, '
timestamp_fields = timestamp_fields + 'year int, '
timestamp_fields = timestamp_fields + 'weekday varchar'
	#
time_table_create = f'CREATE TABLE IF NOT EXISTS time ({timestamp_fields})'
	#
	###################################################################################################################################
	# INSERT RECORDS

songplay_table_insert = 'INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)'
songplay_table_insert += ' VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);'

user_table_insert = 'INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s);'

song_table_insert = "INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s)"

artist_table_insert = 'INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES (%s, %s, %s, %s, %s);'

time_table_insert = 'INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s, %s);'

# FIND SONGS  (row.song, row.artist, row.length))

song_select = 'SELECT songs.song_id, artists.artist_id FROM songs JOIN artists ON songs.artist_id = artists.artist_id;'

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
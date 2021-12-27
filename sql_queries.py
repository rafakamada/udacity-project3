import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

REGION = config.get('REGION', 'REGION')

LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSON_PATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

ARN = config.get('IAM_ROLE', 'ARN')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
  artist VARCHAR,
  auth VARCHAR,
  firstName VARCHAR,
  gender VARCHAR,
  itemInSession INT,
  lastName VARCHAR,
  length FLOAT,
  level VARCHAR,
  location VARCHAR,
  method VARCHAR,
  page VARCHAR,
  registration FLOAT,
  sessionID INT,
  song VARCHAR,
  status INT,
  ts BIGINT,
  userAgent VARCHAR,
  userID INT
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
  num_songs INT,
  artist_id VARCHAR,
  artist_latitude FLOAT,
  artist_longitude FLOAT,
  artist_location VARCHAR,
  artist_name VARCHAR,
  song_id VARCHAR,
  title VARCHAR,
  duration FLOAT,
  year INT
)
""")

songplay_table_create = (""" 
CREATE TABLE songplays (
  id INT IDENTITY(0,1),
  start_time TIMESTAMP,
  user_id INT,
  level VARCHAR,
  song_id VARCHAR,
  artist_id VARCHAR,
  session_id INT,
  location VARCHAR,
  user_agent VARCHAR,
  FOREIGN KEY (start_time) REFERENCES time(start_time),
  FOREIGN KEY (user_id) REFERENCES users(id),
  FOREIGN KEY (song_id) REFERENCES songs(id),
  FOREIGN KEY (artist_id) REFERENCES artists(id)
)
""")

user_table_create = ("""
CREATE TABLE users (
  id  INT PRIMARY KEY,
  first_name VARCHAR,
  last_name VARCHAR,
  gender VARCHAR,
  level VARCHAR
)
""")

song_table_create = ("""
CREATE TABLE songs (
  id VARCHAR PRIMARY KEY,
  title VARCHAR,
  artist_id VARCHAR,
  year INT,
  duration FLOAT
)
""")

artist_table_create = ("""
CREATE TABLE artists (
  id VARCHAR PRIMARY KEY,
  name VARCHAR,
  location VARCHAR,
  latitude FLOAT,
  longitude FLOAT
)
""")

time_table_create = ("""
CREATE TABLE time (
  start_time TIMESTAMP PRIMARY KEY,
  hour INT,
  day INT,
  week INT,
  month INT,
  year INT,
  weekday INT
)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
CREDENTIALS 'aws_iam_role={}'
JSON {}
REGION {};
""").format(LOG_DATA, ARN, LOG_JSON_PATH, REGION)

staging_songs_copy = ("""
COPY staging_songs FROM {}
CREDENTIALS 'aws_iam_role={}'
JSON 'auto'
REGION {};
""").format(SONG_DATA, ARN, REGION)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT TIMESTAMP 'epoch' + e.ts::BIGINT/1000 * INTERVAL '1 second',
       e.userid,
       e.level,
       s.id,
       s.artist_id,
       e.sessionid,
       e.location,
       e.useragent
FROM staging_events AS e
  INNER JOIN songs AS s ON e.song = s.title
                       AND e.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (id, first_name, last_name, gender, level)
SELECT userid,
       firstname,
       lastname,
       gender,
       level
FROM staging_events
WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (id, title, artist_id, year, duration)
SELECT song_id,
       title,
       artist_id,
       year,
       duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (id, name, location, latitude, longitude)
SELECT artist_id,
       artist_name,
       artist_location,
       artist_latitude,
       artist_longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT TIMESTAMP 'epoch' + ts::BIGINT/1000 * INTERVAL '1 second' AS epoch_to_timestamp,
       DATE_PART('hour', epoch_to_timestamp),
       DATE_PART('day', epoch_to_timestamp),
       DATE_PART('week', epoch_to_timestamp),
       DATE_PART('month', epoch_to_timestamp),
       DATE_PART('year', epoch_to_timestamp),
       DATE_PART('weekday', epoch_to_timestamp)
FROM staging_events
WHERE page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]

import configparser

config = configparser.ConfigParser()
config.read('dwh.cfg')

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

staging_events_table_create = ("""CREATE TABLE staging_events(
event_id INT IDENTITY(0,1),
artist_name VARCHAR(255),
auth VARCHAR(50),
user_first_name VARCHAR(255),
user_gender VARCHAR(1),
item_in_session	INTEGER,
user_last_name VARCHAR(255),
song_length	DOUBLE PRECISION,
user_level VARCHAR(50),
location VARCHAR(255),
method VARCHAR(25),
page VARCHAR(35),
registration VARCHAR(50),
session_id BIGINT,
song_title VARCHAR(255),
status INTEGER,
ts bigint,
user_agent TEXT,
user_id integer,
PRIMARY KEY (event_id))
""")

staging_songs_table_create = ("""CREATE TABLE staging_songs(
song_id VARCHAR(100) primary key,
num_songs INTEGER,
artist_id VARCHAR(100),
artist_latitude DOUBLE PRECISION,
artist_longitude DOUBLE PRECISION,
artist_location VARCHAR(255),
artist_name VARCHAR(255),
title VARCHAR(255),
duration DOUBLE PRECISION,
year INTEGER)
""")

songplay_table_create = ("""CREATE TABLE if not exists songplays(
songplay_id INT IDENTITY(0,1) primary key,
start_time TIMESTAMP REFERENCES time(start_time),
user_id INTEGER REFERENCES users(user_id),
level VARCHAR(50),
song_id VARCHAR(100) REFERENCES songs(song_id),
artist_id VARCHAR(100) REFERENCES artists(artist_id),
session_id BIGINT,
location VARCHAR(255),
user_agent TEXT)
""")

user_table_create = ("""CREATE TABLE users(
user_id INTEGER not null primary key,
first_name VARCHAR(255),
last_name VARCHAR(255),
gender VARCHAR(1),
level VARCHAR(50))
""")

song_table_create = ("""CREATE TABLE songs(
song_id VARCHAR(100) not null primary key,
title VARCHAR(255),
artist_id VARCHAR(100) NOT NULL,
year INTEGER,
duration DOUBLE PRECISION,
PRIMARY KEY (song_id))
""")

artist_table_create = ("""CREATE TABLE artists(
artist_id VARCHAR(100) not null primary key,
name VARCHAR(255),
location VARCHAR(255),
latitude DOUBLE PRECISION,
longitude DOUBLE PRECISION)
""")

time_table_create = ("""CREATE TABLE time(
start_time TIMESTAMP not null primary key,
hour INTEGER,
day INTEGER,
week INTEGER,
month INTEGER,
year INTEGER,
weekday INTEGER)
""")

staging_events_copy = ("""copy staging_events from '{}'
 credentials 'aws_iam_role={}'
 region 'us-west-2'
 COMPUPDATE OFF STATUPDATE OFF
 JSON '{}'""") \
    .format(config.get('S3', 'LOG_DATA'),
            config.get('IAM_ROLE', 'ARN'),
            config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""copy staging_songs from '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    COMPUPDATE OFF STATUPDATE OFF
    JSON 'auto'
    """).format(config.get('S3', 'SONG_DATA'),
                config.get('IAM_ROLE', 'ARN'))

songplay_table_insert = ("""INSERT INTO
songplays (start_time, user_id, level,
song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT
TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time,
e.user_id,
e.user_level,
s.song_id,
s.artist_id,
e.session_id,
e.location,
e.user_agent
FROM staging_events e, staging_songs s
WHERE e.page = 'NextSong'
AND e.song_title = s.title
AND user_id NOT IN (SELECT DISTINCT
s.user_id FROM songplays s WHERE s.user_id = user_id
AND s.start_time = start_time AND s.session_id = session_id)
""")

user_table_insert = ("""INSERT INTO
users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
user_id,
user_first_name,
user_last_name,
user_gender,
user_level
FROM staging_events
WHERE page = 'NextSong'
""")

song_table_insert = ("""INSERT INTO
songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT
song_id,
title,
artist_id,
year,
duration
FROM staging_songs
WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs)
""")

artist_table_insert = ("""INSERT INTO
artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
artist_id,
artist_name,
artist_location,
artist_latitude,
artist_longitude
FROM staging_songs
WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
""")

time_table_insert = ("""INSERT INTO
time (start_time, hour, day, week, month, year, weekday)
SELECT
start_time,
EXTRACT(hr from start_time) AS hour,
EXTRACT(d from start_time) AS day,
EXTRACT(w from start_time) AS week,
EXTRACT(mon from start_time) AS month,
EXTRACT(yr from start_time) AS year,
EXTRACT(weekday from start_time) AS weekday
FROM (SELECT DISTINCT
TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as
start_time FROM staging_events s)
""")

create_table_queries = [staging_events_table_create,
                        staging_songs_table_create,
                        user_table_create,
                        song_table_create,
                        artist_table_create,
                        time_table_create,
                        songplay_table_create]

drop_table_queries = [staging_events_table_drop,
                      staging_songs_table_drop,
                      songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop]

copy_table_queries = [staging_events_copy,
                      staging_songs_copy]
insert_table_queries = [songplay_table_insert,
                        user_table_insert,
                        song_table_insert,
                        artist_table_insert,
                        time_table_insert]

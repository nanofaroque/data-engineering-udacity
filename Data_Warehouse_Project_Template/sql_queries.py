import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create = ("""create table if not exists staging_events
(event_id integer IDENTITY(0,1) PRIMARY KEY,
artist text,
auth text,
first_name text,
gender text,
item_in_session text,
last_name text,
length text,
level text,
location text,
method text,
page text,
registration text,
session_id text,
song text,
status text,
start_time text,
user_agent text,
user_id text
);
""")

staging_songs_table_create = ("""create table if not exists staging_songs
(num_songs text,
artist_id text,
artist_latitude double precision,
artist_longitude double precision,
artist_location text,
artist_name text,
song_id text PRIMARY KEY,
title text,
duration double precision,
year integer
);
""")

songplay_table_create = ("""create table if not exists songplays
(songplay_id integer IDENTITY(0,1) PRIMARY KEY,
start_time timestamp REFERENCES time(start_time) sortkey,
user_id text NOT NULL REFERENCES users(user_id),
level text,
song_id text REFERENCES songs(song_id) distkey,
artist_id text REFERENCES artists(artist_id),
session_id text,
location text,
user_agent text);
""")

user_table_create = ("""create table if not exists users(
user_id text primary key,
first_name text,
last_name text,
gender text,
level text
);
""")

song_table_create = ("""create table if not exists songs(
song_id text primary key ,
title text,
artist_id text NOT NULL REFERENCES artists(artist_id),
year integer,
duration double precision
);
""")

artist_table_create = ("""create table if not exists artists(
artist_id text primary key ,
name text,
location text,
latitude double precision,
longitude double precision
);
""")

time_table_create = ("""create table if not exists time(
start_time timestamp primary key ,
hour integer,
day integer,
week integer,
month integer,
year integer,
weekday text
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM '{}'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
FORMAT AS JSON '{}';
""").format(config.get('S3', 'LOG_DATA'),
            config.get('IAM_ROLE', 'ARN'),
            config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""copy staging_songs
from '{}'
credentials 'aws_iam_role={}'
region 'us-west-2'
json 'auto';
""").format(config.get('S3', 'SONG_DATA'),
            config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]

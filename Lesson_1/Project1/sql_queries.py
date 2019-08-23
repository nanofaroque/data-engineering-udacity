# DROP TABLES
songplay_table_drop = "drop table songplays;"
user_table_drop = "drop table users;"
song_table_drop = "drop table songs"
artist_table_drop = "drop table artists"
time_table_drop = "drop table time"

# CREATE TABLES

songplay_table_create = ("""create table songplays(
songplay_id serial not null,
start_time timestamp(3) with time zone,
user_id int not null ,
level varchar(25),
song_id varchar(256) not null ,
artist_id varchar(256) not null ,
session_id int,
location varchar(256), 
user_agent varchar(256),
primary key (songplay_id));
""")

user_table_create = ("""create table users(
user_id varchar(256) not null,
first_name varchar(40),
last_name varchar(40),
gender varchar(10),
level varchar(25),
primary key (user_id));
""")

song_table_create = ("""create table songs(
song_id varchar(256) not null,
title varchar(256),
artist_id varchar(256) not null ,
year int,
duration DECIMAL,
primary key (song_id));
""")

artist_table_create = ("""create table artists(
artist_id varchar(256) not null ,
name varchar(256),
location varchar(256),
latitude decimal,
longitude decimal,
primary key (artist_id));
""")

time_table_create = ("""create table time(
start_time timestamp(3) with time zone,
hour int,
day int,
week int,
month int,
year int,
weekday int,
primary key (start_time));
""")

# INSERT RECORDS

songplay_table_insert = ("""insert into songplays(
start_time,
user_id,
level,
song_id,
artist_id ,
session_id,
location, 
user_agent
)values (%s,%s,%s,%s,%s,%s,%s,%s);
""")

user_table_insert = ("""INSERT INTO users(
user_id,
first_name,
last_name,
gender,
level
)VALUES(%s,%s,%s,%s,%s)
ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level;
""")

song_table_insert = ("""INSERT INTO songs(
song_id,
title,
artist_id,
year,
duration
)VALUES(%s,%s,%s,%s,%s)
ON CONFLICT (song_id)
DO NOTHING;
""")

artist_table_insert = ("""insert into artists(
artist_id,
name,
location,
latitude,
longitude
)values(%s,%s,%s,%s,%s)
ON CONFLICT (artist_id)
DO NOTHING;
""")

time_table_insert = ("""insert into time(
start_time,
hour,
day,
week,
month,
year,
weekday
)values(%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (start_time)
DO NOTHING;
""")

# FIND SONGS

song_select = ("""
SELECT artists.artist_id, songs.song_id
FROM artists 
INNER JOIN songs
ON artists.artist_id= songs.artist_id
where songs.title=%s and songs.duration=%s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create,
                        time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

insert_table_queries = [song_table_insert]

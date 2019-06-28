# DROP TABLES
songplay_table_drop = "drop table songplays;"
user_table_drop = "drop table users;"
song_table_drop = "drop table songs"
artist_table_drop = "drop table artists"
time_table_drop = "drop table time"

# CREATE TABLES

songplay_table_create = ("""create table songplays(
songplay_id int,
start_time TIMESTAMP,
user_id int,
level varchar(25),
song_id varchar(256),
artist_id varchar(256),
session_id int,
location varchar(256), 
user_agent varchar(256));
""")

user_table_create = ("""create table users(
user_id varchar(256),
first_name varchar(40),
last_name varchar(40),
gender varchar(10),
level varchar(25),
primary key (user_id));
""")

song_table_create = ("""create table songs(
song_id varchar(256),
title varchar(256),
artist_id varchar(256),
year int,
duration DECIMAL,
primary key (song_id));
""")

artist_table_create = ("""create table artists(
artist_id varchar(256),
name varchar(256),
location varchar(256),
latitude decimal,
longitude decimal,
primary key (artist_id));
""")

time_table_create = ("""create table time(
start_time timestamp,
hour int,
day int,
week int,
month int,
year int,
weekday int);
""")

# INSERT RECORDS

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""INSERT INTO songs(
song_id,
title,
artist_id,
year,
duration
)VALUES(%s,%s,%s,%s,%s);
""")

artist_table_insert = ("""insert into artists(
artist_id,
name,
location,
latitude,
longitude
)values(%s,%s,%s,%s,%s)
""")

time_table_insert = ("""
""")

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create,
                        time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

insert_table_queries = [song_table_insert]

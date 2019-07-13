import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import sys


def process_song_file(cur, filepath):
    """
    Description: This function can be used to read the file in the filepath (data/song_data)
    to get the song id, song title, artist, year of the release, and duration
    populate the song and artist tables.

    Arguments:
        cur: the cursor object.
        filepath: song data file path.

    Returns:
        None
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values
    # insert song record
    song_data = list(song_data[0])
    record_to_insert = (song_data[0], song_data[1], song_data[2], song_data[3], song_data[4])
    try:
        cur.execute(song_table_insert, record_to_insert)
    except TypeError as error:
        e = sys.exc_info()[0]
        print(e)
        print(error)

    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values
    artist_data = list(artist_data[0])
    artist_data_to_insert = (artist_data[0], artist_data[1], artist_data[2], artist_data[3], artist_data[4])
    cur.execute(artist_table_insert, artist_data_to_insert)


# filter by NextSong action
def my_filter_callback_by_next_song(row):
    return row.page == 'NextSong'


def process_log_file(cur, filepath):
    """
    Description: This function can be used to read the file in the filepath (data/log_data)
    to get the user and time info and used to populate the users and time dim tables.

    Arguments:
        cur: the cursor object.
        filepath: log data file path.

    Returns:
        None
    """
    # open log file
    df = pd.read_json(filepath, lines=True)
    df_next_song = df[df.apply(my_filter_callback_by_next_song, axis=1)]

    df_next_song['date_time'] = (pd.to_datetime(df_next_song['ts'], unit='ms'))
    # convert timestamp column to datetime
    time_data = [[]]
    t = df_next_song[['date_time', 'ts']]
    from datetime import datetime
    for index, row in t.iterrows():
        data = [datetime.fromtimestamp(t['ts'][index] / 1000).strftime('%Y-%m-%d %H:%M:%S.%f'),
                int(t['date_time'][index].hour),
                int(t['date_time'][index].day),
                int(t['date_time'][index].week),
                int(t['date_time'][index].month),
                int(t['date_time'][index].year),
                int(t['date_time'][index].dayofweek)
                ]
        time_data.append(data)
    # insert time data records
    time_data = (time_data)
    column_labels = ('timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        if i != 0:
            print(list(row))
            cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        cur.execute(song_select, (row.song, row.length))
        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = 0, 0

        # insert songplay record
        if row.userId == '':
            user_id = 0
        else:
            user_id = row.userId
        songplay_data = (datetime.fromtimestamp(row.ts / 1000).strftime('%Y-%m-%d %H:%M:%S.%f'), user_id, row.level,
                         songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=password")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    conn.close()


if __name__ == "__main__":
    main()

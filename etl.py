import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Extract information from a song file and populate the two dimension tables `songs` and `artists`.
    
    This function implicitly depends on the SQL statements :func:`~sql_queries.song_table_insert`
    and :func:`~sql_queries.artist_table_insert`.
    
    :param cur: Reference to the database cursor
    :param filepath: Path to the JSON file
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_cols = ['song_id', 'title', 'artist_id', 'year', 'duration']
    song_data = df[song_cols].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_cols = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artist_data = df[artist_cols].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Extract information from a log file and populate the dimension tables `time` and `users`
    as well as the facts table `songplays`.
    
    This function implicitly depends on the SQL statements :func:`~sql_queries.time_table_insert`,
    :func:`~sql_queries.user_table_insert` and :func:`~sql_queries.songplay_table_insert`.
    
    :param cur: Reference to the database cursor
    :param filepath: Path to the JSON file
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (
        df['ts'].values, 
        t.dt.hour.values, 
        t.dt.day.values, 
        t.dt.week.values, 
        t.dt.month.values, 
        t.dt.year.values, 
        t.dt.weekday.values
    )
    column_labels = ('timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            row.ts, 
            row.userId, 
            row.level, 
            songid, 
            artistid, 
            row.sessionId, 
            row.location, 
            row.userAgent
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Recursively finds all JSON files in a given path and calls a function on the
    found files.
    
    :param cur: Reference to the database cursor
    :param conn: Reference to the database connection
    :param filepath: Directory that should be recursively walked through
    :param func: Function that should be called for each JSON file. 
                 Arguments passed are the database cursor and the path to the JSON file.
    """
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
    """
    Main function that establishes a database connection to the PostgeSQL database
    and processes data in the `data/song_data` and `data/log_data` folders.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
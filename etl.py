import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Reads a json file containing details for a song and inserts these details into the 
    songs and artists tables in the database 

    Args:
        cur (psycopg2.connection.cursor): Cursor which communicates with the database
        filepath (str)
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # Remove leading and trailing white space and converting names to lower case to avoid problems matching
    df['title'] = df['title'].str.strip().str.lower()
    df['artist_name'] = df['artist_name'].str.strip().str.lower()

    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data =list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Reads a json file containing user activity details filters them for song plays, and updates the 
    songplays and time tables in the database. Also adds any new users to the users table.

    Args:
        cur (psycopg2.connection.cursor): Cursor which communicates with the database
        filepath (str)
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (t.astype(str), t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    # Note: Series.dt.week is soon to be depricated, so for newer versions of pandas, the following line is preferred:
    # time_data = (t.astype(str), t.dt.hour, t.dt.day, t.dt.isocalendar().week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('timestamp', 'hour', 'day', 'week_of_year', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(columns=column_labels)
    for i, col in enumerate(time_data):
        time_df[column_labels[i]] = col

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    df['song'] = df['song'].str.strip().str.lower()
    df['artist'] = df['artist'].str.strip().str.lower()
    df['length'] = df['length'].round()

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            # Uncomment to check for matches while loading
            # print("Song match found:", row.song, results)
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (time_df.timestamp[index], row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Reads the filepath, locates .json files, and passes them to func to be loaded into sparkifydb.

    Args:
        cur (psycopg2.connection.cursor): Cursor which communicates with the database
        conn (psycopg2.connection)
        filepath (str): Root directory of the search tree
        func (process_song_file / process_log_file): Choose the function to use
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
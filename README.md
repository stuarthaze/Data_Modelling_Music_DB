# Udacity Data Engineering Project #1
## Schema for song play analysis
***
## Overview 
This code creates a PostgreSQL song database called 'sparkifydb' optimized for queries on song plays.  The code itself is Python using the library psycopg2 to interact with the database.  
The database is build using a star schema, with one central fact table (songplays) which is linked to four dimension tables (users, songs, artists, time) which contain additional information that can be accessed by their primary keys:

### Fact table:
#### `songplays`
Column names:
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension tables:
#### `users`
user_id, first_name, last_name, gender, level

#### `songs`
song_id, title, artist_id, year, duration

#### `artists`
artist_id, name, location, latitude, longitude

#### `time`
start_time, hour, day, week, month, year, weekday

***
## Usage
1. Start PostgreSQL server
2. Create role 'student' and database 'studentdb' (Required to make an initial connection)
    - `CREATE ROLE student WITH LOGIN CREATEDB PASSWORD 'student'`
    - `CREATE DATABASE IF NOT EXISTS studentdb`
3. Run create_tables
    - `python create_tables.py`
4.  Perform the ETL (Extract Load Transform) process to fill in the database
    - `python etl.py`
5. Log in to the database. This can be done using psql as follows (default port setting)
    - `psql -h localhost -p 5432 -U student sparkifydb`
6. Check that the tables have been correctly loaded with the commands
    - `sparkifydb=> \d` - Print a description of the database
    - `sparkifydb=> \d <table name>` Print a description of a given table

7. Use the database to perform queries of interest. 
    - Example: Which users have played the most songs?
```
SELECT songplays.user_id, users.first_name, COUNT(*) 
    FROM songplays
    JOIN users ON songplays.user_id = users.user_id  
    GROUP BY songplays.user_id, users.first_name  
    ORDER BY count DESC
    LIMIT 10;
```

***
## File desctiptions
* `create_tables.py` 
    - Clears and recreates the tables for the sparkifydb database
* `etl.py`
    - Main program for reading the data from files and loading the data into the database
* `sql_queries.py` 
    - Contains the SQL queries used to create tables and insert data

***
### NOTE
There needs to be a lot more data added to the song library for more interesting queries since many of the songs played by the users are not contained in the song data. The data in this project was provided by Udacity and is a subset of the [Million song dataset](http://millionsongdataset.com/) and could easily be expanded.
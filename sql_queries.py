import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist varchar NOT NULL,
        auth varchar NOT NULL,
        firstName varchar NOT NULL,
        gender varchar(1) NOT NULL,
        itemInSession int NOT NULL,
        lastName varchar NOT NULL,
        length real NOT NULL,
        level varchar NOT NULL,
        location varchar NOT NULL,
        method varchar NOT NULL,
        page varchar NOT NULL,
        registration real NOT NULL,
        sessionId integer NOT NULL,
        song varchar NOT NULL,
        status smallint NOT NULL,
        ts bigint NOT NULL,
        userAgent varchar NOT NULL,
        userId int NOT NULL
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs int NOT NULL, 
        artist_id varchar NOT NULL, 
        artist_latitude real, 
        artist_longitude real, 
        artist_location varchar NOT NULL, 
        artist_name varchar NOT NULL, 
        song_id varchar NOT NULL, 
        title varchar NOT NULL, 
        duration real NOT NULL, 
        year smallint NOT NULL
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id int IDENTITY(0, 1) PRIMARY KEY,
        start_time bigint NOT NULL,
        user_id int NOT NULL,
        level varchar NOT NULL,
        song_id varchar,
        artist_id varchar, 
        session_id int NOT NULL,
        location varchar NOT NULL,
        user_agent varchar NOT NULL
    );
""")

user_table_create = user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id int PRIMARY KEY,
        first_name varchar NOT NULL,
        last_name varchar NOT NULL,
        gender varchar(1) NOT NULL,
        level varchar NOT NULL
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar PRIMARY KEY,
        title varchar NOT NULL,
        artist_id varchar NOT NULL,
        year smallint NOT NULL,
        duration real NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar PRIMARY KEY,
        name varchar NOT NULL,
        location varchar NOT NULL,
        latitude real,
        longitude real
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time bigint NOT NULL,
        hour smallint NOT NULL,
        day smallint NOT NULL,
        week smallint NOT NULL,
        month smallint NOT NULL,
        year smallint NOT NULL,
        weekday smallint NOT NULL
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    
""").format()

staging_songs_copy = ("""
""").format()

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

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

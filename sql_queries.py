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
        artist varchar,
        auth varchar NOT NULL,
        firstName varchar,
        gender varchar(1),
        itemInSession int NOT NULL,
        lastName varchar,
        length real,
        level varchar NOT NULL,
        location varchar,
        method varchar NOT NULL,
        page varchar NOT NULL,
        registration real,
        sessionId integer NOT NULL,
        song varchar,
        status smallint NOT NULL,
        ts timestamp NOT NULL,
        userAgent varchar,
        userId int
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
        start_time timestamp NOT NULL,
        user_id int,
        level varchar NOT NULL,
        song_id varchar,
        artist_id varchar, 
        session_id int NOT NULL,
        location varchar,
        user_agent varchar
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id int PRIMARY KEY,
        first_name varchar,
        last_name varchar,
        gender varchar(1),
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
        start_time timestamp NOT NULL,
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
    COPY staging_events FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    JSON '{}'
    TIMEFORMAT as 'epochmillisecs';
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY staging_songs FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    JSON 'auto';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) (
        SELECT staging_events.ts
             , staging_events.userId
             , staging_events.level
             , staging_songs.song_id
             , staging_songs.artist_id
             , staging_events.sessionId
             , staging_events.location
             , staging_events.userAgent
        FROM staging_events
        JOIN staging_songs 
            ON staging_events.artist = staging_songs.artist_name 
            AND staging_events.song = staging_songs.song_id
            AND staging_events.length = staging_songs.duration
        WHERE staging_events.page = 'NextSong'
    );
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId
                  , firstName
                  , lastName
                  , gender
                  , level
    FROM staging_events
    WHERE userId IS NOT NULL
    AND staging_events.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id
                  , title
                  , artist_id
                  , year
                  , duration
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id
                  , artist_name
                  , artist_location
                  , artist_longitude
                  , artist_latitude
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT ts
         , EXTRACT(HOUR FROM ts)
         , EXTRACT(DAY FROM ts)
         , EXTRACT(WEEK FROM ts)
         , EXTRACT(MONTH FROM ts)
         , EXTRACT(YEAR FROM ts)
         , EXTRACT(WEEKDAY FROM ts)
    FROM staging_events
    WHERE staging_events.page = 'NextSong';
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay;"
user_table_drop = "DROP TABLE IF EXISTS dim_users;"
song_table_drop = "DROP TABLE IF EXISTS dim_songs;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists;"
time_table_drop = "DROP TABLE IF EXISTS dim_time;"


# CREATE STAGING TABLES

staging_events_table_create= (""" 
    CREATE TABLE staging_events (
        artist varchar,
        auth varchar,
        firstname varchar,
        gender varchar,
        iteminsession int,
        lastname varchar,
        length float4,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration bigint,
        sessionid int,
        song varchar,
        status int,
        ts timestamp,
        useragent varchar,
        userid int
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        artist_id varchar,
        artist_latitude float4,
        artist_location varchar,
        artist_longitude float4,
        artist_name varchar,
        duration float4,
        num_songs int,
        song_id varchar,
        title varchar,
        year int
    );
""")


# CREATE FACT TABLE

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS fact_songplay (
        songplay_id bigint identity(0, 1) PRIMARY KEY NOT NULL, 
        start_time timestamp NOT NULL, 
        user_id int NOT NULL, 
        level varchar, 
        song_id varchar,
        artist_id varchar,
        session_id int, 
        location varchar,
        user_agent varchar);
""")


# CREATE DIMENSION TABLES

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_users (
        user_id int PRIMARY KEY NOT NULL,
        first_name varchar, 
        last_name varchar, 
        gender varchar,
        level varchar);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_songs (
        song_id varchar PRIMARY KEY NOT NULL,
        title varchar, 
        artist_id varchar, 
        year int,
        duration float);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_artists (
        artist_id varchar PRIMARY KEY NOT NULL,
        name varchar, 
        location varchar, 
        latitude float,
        longitude float);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_time (
        start_time timestamp PRIMARY KEY NOT NULL,
        hour int, 
        day int, 
        week int, 
        month int, 
        year int,
        weekday int);
""")


# POPULATE STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF 
    REGION 'us-west-2'
    TIMEFORMAT AS 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    JSON {}
    """).format(
        config.get('S3', 'LOG_DATA'), 
        config.get('IAM_ROLE', 'ARN'),
        config.get('S3', 'LOG_JSON_PATH')
        )                       
                       
staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF 
    REGION 'us-west-2'
    TIMEFORMAT AS 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    JSON 'auto ignorecase';
    """).format(
        config.get('S3', 'SONG_DATA'), 
        config.get('IAM_ROLE', 'ARN')
        )


# POPULATE FACT & DIMENSION TABLES

songplay_table_insert = ("""
    INSERT INTO fact_songplay (start_time, user_id, level, 
        song_id, artist_id, session_id, location, user_agent)       
        (SELECT se.ts, se.userid, se.level, ss.song_id, 
            ss.artist_id, se.sessionid, se.location, se.useragent
         FROM staging_events se JOIN staging_songs ss
             ON se.artist = ss.artist_name
             AND se.song = ss.title
             AND se.length = ss.duration
         WHERE se.page = 'NextSong'
             AND se.ts IS NOT NULL
             AND se.userid IS NOT NULL);        
""")

user_table_insert = ("""
    INSERT INTO dim_users (user_id, first_name, last_name,
        gender, level)
        (SELECT DISTINCT a.userid, a.firstname, a.lastname, a.gender, a.level
         FROM staging_events a
         WHERE a.userid IS NOT NULL
             AND a.ts = (SELECT MAX(b.ts) FROM staging_events b
                         WHERE b.userid = a.userid));
""")

song_table_insert = ("""
    INSERT INTO dim_songs (song_id, title, artist_id, year,
        duration)
        (SELECT DISTINCT song_id, title, artist_id, year, duration
         FROM staging_songs
         WHERE song_id IS NOT NULL
             AND song_id NOT IN (SELECT DISTINCT song_id FROM dim_songs));
""")
                       
artist_table_insert = ("""
    INSERT INTO dim_artists (artist_id, name, location, 
        latitude, longitude)
        (SELECT DISTINCT artist_id, artist_name, artist_location,
             artist_latitude, artist_longitude
         FROM staging_songs
         WHERE artist_id IS NOT NULL
             AND artist_id NOT IN (SELECT DISTINCT artist_id FROM dim_artists));
""")

time_table_insert = ("""
    INSERT INTO dim_time (start_time, hour, day, week, month, 
        year, weekday)        
        (SELECT DISTINCT a.ts AS start_time, EXTRACT(hour FROM start_time), 
            EXTRACT(day FROM start_time), EXTRACT(week FROM start_time), 
            EXTRACT(month FROM start_time), EXTRACT(year FROM start_time), 
            EXTRACT(dow FROM start_time)
         FROM staging_events a
         WHERE start_time IS NOT NULL
             AND start_time NOT IN (SELECT DISTINCT b.start_time FROM dim_time b));
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

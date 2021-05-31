# PURPOSE: 

This data warehouse has been set up to support easy-to-use optimized queries and data analysis for the Sparkify music streaming app, which stores all song and user activity data/metadata in JSON files. The AWS cloud-based ETL pipeline associated with this data warehouse uses AWS Redshift to extract the Sparkify data from these JSON files and place it in two staging tables. From there, Redshift is used to transform and load the staged data into a star schema useful for analytical queries. The overall table schema is defined as follows: 

## Staging Tables (2)
staging_events : keys={artist, auth, firstname, gender, iteminsession, lastname, length, level, location, method, 
                       page, registration, sessionid, song, status, ts, useragent, userid}
staging_songs : keys={artist_id, artist_latitude, artist_location, artist_longitude, artist_name, duration, 
                      num_songs, song_id, title, year}

## Fact Table: 
fact_songplay : keys={songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent}

## Dimension Tables (4) 
dim_users : keys={user_id, first_name, last_name, gender, level}
dim_songs : keys={song_id, title, artist_id, year, duration}
dim_artists : keys={artist_id, name, location, latitude, longitude}
dim_time : keys={start_time, hour, day, week, month, year, weekday}

The justification for staging in this manner is that it takes the data/metadata for song and user activity exactly as generated and logged by existing Sparkify business processes and provides an intermediate Redshift storage point from which further transformation can be performed to generate fact and dimension tables more suitable for analytical queries. 

The justifications for the choice of a star schema for the analytical queries include:

a) Being denormalized, the join logic required for queries is much simpler than with a normalized schema.
b) Simplified reporting logic for queries of business interest.
c) Optimized query performance (especially for aggregations).


# SCRIPTS PROVIDED

1) dwh.cfg : Configuration file specifying an AWS Redshift CLUSTER 
             (HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT),
             IAM_ROLE (ARN), and S3 bucket containing the JSON log
             data and path information (LOG_DATA, LOG_JSON_PATH, SONG_DATA)
             
             (NOTE: the S3 variables are populated in this file, but the
              Redshift cluster variables and ARN must be generated on AWS and 
              added to the file)


2) sql_queries.py : Contains definition strings for the AWS Redshift queries to: 
                        a) DROP all tables in the Sparkify data warehouse (if they exist).
                        b) CREATE all tables required for the data warehouse.
                        c) COPY song and user activity data/metadata into the staging tables.
                        d) Transform and INSERT staged data into the fact and dimension tables.

3) create_tables.py : Reads cluster parameters from 'dwh.cfg' and establishes connection to specified AWS 
                      Redshift cluster, runs the queries in sql_queries.py to DROP all the tables (if they exist),
                      runs the queries in sql_queries.py to CREATE all necessary tables, and then closes the
                      connection.
                          
4) etl.py : Reads cluster parameters from 'dwh.cfg' and establishes connection to specified AWS Redshift cluster,
            copies log data/metadata JSON files from S3 to staging tables using queries from sql_queries.py,
            transforms and INSERTS data from staging tables to fact and dimension tables using additional
            queries from sql_queries.py, and then closes the connection.
                      

# RUNNING THE PROVIDED SCRIPTS:

1) Drop any existing tables and create new (empty) tables:

From the terminal (in the directory containing the Python scripts), type: python create_tables.py

(NOTE: This script imports sql_queries.py and dwh.cfg, which must be in the same directory)

2) Extract the JSON files from S3 into staging tables, Transform and Load staged data into the Fact and Dimension Tables:

From the terminal, type: python etl.py

(NOTE: This script also imports sql_queries.py and dw.cfg, which must be in the same directory)
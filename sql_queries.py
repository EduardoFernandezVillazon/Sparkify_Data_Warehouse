import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

arn = config.get( 'IAM_ROLE', 'ARN')
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table;"
user_table_drop = "DROP TABLE IF EXISTS user_table;"
song_table_drop = "DROP TABLE IF EXISTS song_table;"
artist_table_drop = "DROP TABLE IF EXISTS artist_table;"
time_table_drop = "DROP TABLE IF EXISTS time_table;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events_table
(
    artist varchar(max) DISTKEY,
    auth varchar,
    firstName varchar,
    gender char(1),
    itemInSession int,
    lastName varchar,
    length numeric,
    level char(4),
    location varchar,
    method char (3),
    page varchar,
    registration float,
    sessionId int,
    song varchar,
    status int,
    ts bigint,
    userAgent varchar,
    userId int
    );
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs_table
(
    num_songs int,
    artist_id text,
    artist_latitude numeric, 
    artist_longitude numeric,
    artist_location varchar,
    artist_name varchar(max) DISTKEY,
    song_id text,
    title text,
    duration numeric,
    year int
    );
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays 
(
    songplay_id bigint IDENTITY(0,1) PRIMARY KEY, 
    start_time bigint NOT NULL, 
    user_id int NOT NULL, 
    level char(4), 
    song_id text NOT NULL, 
    artist_id text NOT NULL, 
    session_id int, 
    location varchar, 
    user_agent varchar
    );
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users 
(
    user_id int NOT NULL PRIMARY KEY, 
    first_name varchar, 
    last_name varchar, 
    gender char(1), 
    level char(4)
    );
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs 
(
    song_id text NOT NULL PRIMARY KEY, 
    title varchar, 
    artist_id text NOT NULL, 
    year int, 
    duration numeric
    );
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists 
(
    artist_id text NOT NULL PRIMARY KEY, 
    name varchar(max), 
    location varchar, 
    latitude numeric, 
    longitude numeric);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time 
(
    start_time timestamp NOT NULL PRIMARY KEY, 
    hour int, 
    day int, 
    week int, 
    month text, 
    year int, 
    weekday varchar
    );
""")

# STAGING TABLES

#(
#                            artist, auth, firstName, gender, 
#                            itemInSession, lastName, length, 
#                            level, location, method, page, 
#                            registration, sessionId, song, 
#                            status, ts, userAgent, userId 
#                            ) 

staging_events_copy = ("""

COPY staging_events_table 

FROM {} 

CREDENTIALS 'aws_iam_role={}' COMPUPDATE OFF 

JSON {};

""").format(config['S3']['LOG_DATA'], arn, config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""

COPY staging_songs_table ( 
                            num_songs, artist_id, artist_latitude, 
                            artist_longitude, artist_location, 
                            artist_name, song_id, title, duration, year
                            ) 
                            
FROM {} 

CREDENTIALS 'aws_iam_role={}' COMPUPDATE OFF FORMAT AS JSON 'auto';

""").format(config['S3']['SONG_DATA'],arn)

# FINAL TABLES

songplay_table_insert = ("""

INSERT INTO songplays (start_time , user_id , level , song_id, artist_id , session_id , location , user_agent ) 

SELECT DISTINCT e.ts , e.userId , e.level , s.song_id, s.artist_id , e.sessionId , e.location , e.userAgent 

FROM staging_events_table AS E LEFT JOIN staging_songs_table AS S 

ON (s.title=e.song) AND (e.artist=s.artist_name)

WHERE (e.ts IS NOT NULL) AND (e.userId IS NOT NULL) AND (s.song_id IS NOT NULL) AND (s.artist_id IS NOT NULL) AND (e.page = 'NextSong');

""")

user_table_insert = ("""

INSERT INTO users (user_id, first_name, last_name, gender, level) 

SELECT DISTINCT userId, firstName, lastName, gender, level 

FROM staging_events_table

WHERE (userId IS NOT NULL)

""")

song_table_insert = ("""

INSERT INTO songs (song_id, title, artist_id, year, duration) 

SELECT DISTINCT song_id, title, artist_id, year, duration 

FROM staging_songs_table

WHERE (song_id IS NOT NULL)

""")

artist_table_insert = ("""

INSERT INTO artists (artist_id, name , location , latitude, longitude) 

SELECT DISTINCT artist_id, artist_name , artist_location , artist_latitude, artist_longitude 

FROM staging_songs_table

WHERE (artist_id IS NOT NULL)

""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)

SELECT DISTINCT a.start_time,

EXTRACT (HOUR FROM a.start_time), EXTRACT (DAY FROM a.start_time),

EXTRACT (WEEK FROM a.start_time), EXTRACT (MONTH FROM a.start_time),

EXTRACT (YEAR FROM a.start_time), EXTRACT (WEEKDAY FROM a.start_time) FROM

(SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time FROM staging_events_table) a

WHERE a.start_time is not Null;

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

S3_LOG_DATA = config.get('S3', 'LOG_DATA')
S3_SONG_DATA = config.get('S3', 'SONG_DATA')
S3_LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
IAM_ROLE_ARN = config.get('IAM_ROLE', 'ARN')
GEO_REGION = config.get('GEO', 'REGION')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay;"
user_table_drop = "DROP TABLE IF EXISTS dim_user;"
song_table_drop = "DROP TABLE IF EXISTS dim_song;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist;"
time_table_drop = "DROP TABLE IF EXISTS dim_time;"

# CREATE TABLES
staging_events_table_create= ("""
CREATE TABLE staging_events (
    artist TEXT,
    auth TEXT,
    firstName TEXT,
    gender TEXT,
    itemInSession INT,
    lastName TEXT,
    length DOUBLE PRECISION,
    level TEXT,
    location TEXT,
    method TEXT,
    page TEXT,
    registration TEXT,
    sessionId INT,
    song TEXT,
    status INT,
    ts TIMESTAMP,
    userAgent TEXT,
    userId INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    song_id TEXT PRIMARY KEY,
    num_songs INT,
    artist_id TEXT,
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    artist_location TEXT,
    artist_name TEXT,
    title TEXT,
    duration DOUBLE PRECISION,
    year INT
);
""")

songplay_table_create = ("""
CREATE TABLE fact_songplay (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL REFERENCES dim_time(start_time) sortkey,
    user_id INT NOT NULL REFERENCES dim_user(user_id),
    level TEXT NOT NULL,
    song_id TEXT NOT NULL REFERENCES dim_song(song_id),
    artist_id TEXT NOT NULL REFERENCES dim_artist(artist_id) distkey,
    session_id INT NOT NULL,
    location TEXT NOT NULL,
    user_agent TEXT NOT NULL
);
""")

user_table_create = ("""
CREATE TABLE dim_user (
    user_id INT PRIMARY KEY sortkey,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    gender TEXT NOT NULL,
    level TEXT NOT NULL
) diststyle ALL;
""")

song_table_create = ("""
CREATE TABLE dim_song (
    song_id TEXT PRIMARY KEY sortkey,
    title TEXT NOT NULL,
    artist_id TEXT NOT NULL,
    year INT NOT NULL,
    duration DOUBLE PRECISION NOT NULL
) diststyle ALL;
""")

artist_table_create = ("""
CREATE TABLE dim_artist (
    artist_id TEXT PRIMARY KEY sortkey distkey,
    name TEXT NOT NULL,
    location TEXT NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
);
""")

time_table_create = ("""
CREATE TABLE dim_time (
    start_time TIMESTAMP PRIMARY KEY sortkey,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday INT NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {from_:}
    iam_role {iam_role:}
    region {region:}
FORMAT AS JSON {format:}
timeformat 'epochmillisecs';
""").format(**{
    'from_': S3_LOG_DATA,
    'iam_role': IAM_ROLE_ARN,
    'region': GEO_REGION,
    'format': S3_LOG_JSONPATH
})

staging_songs_copy = ("""
COPY staging_songs
FROM {from_:}
    iam_role {iam_role:}
    region {region:}
FORMAT AS JSON
'auto';
""").format(**{
    'from_': S3_SONG_DATA,
    'iam_role': IAM_ROLE_ARN,
    'region': GEO_REGION
})

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO fact_songplay (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent 
)
SELECT DISTINCT 
    se.ts,
    se.userId,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionId,
    se.location,
    se.userAgent
FROM staging_events se
INNER JOIN staging_songs ss
    ON se.song = ss.title
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO dim_user (
    user_id,
    first_name,
    last_name,
    gender,
    level
)
SELECT DISTINCT
    se.userId,
    se.firstName,
    se.lastName,
    se.gender,
    se.level
FROM staging_events se
WHERE se.userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO dim_song (
    song_id,
    title,
    artist_id,
    year,
    duration
)
SELECT DISTINCT
    ss.song_id,
    ss.title,
    ss.artist_id,
    ss.year,
    ss.duration
FROM staging_songs ss;
""")

artist_table_insert = ("""
INSERT INTO dim_artist (
    artist_id,
    name,
    latitude,
    longitude,
    location
)
SELECT DISTINCT
    ss.artist_id,
    ss.artist_name,
    ss.artist_latitude,
    ss.artist_longitude,
    CASE WHEN 
        ss.artist_location IS NOT NULL 
        AND ss.artist_location != '' 
    THEN ss.artist_location else 'N/A' END 
FROM staging_songs ss
WHERE ss.artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO dim_time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
SELECT DISTINCT
    se.ts,
    CAST(DATE_PART('hour',  se.ts) as INT),
    CAST(DATE_PART('day',   se.ts) as INT),
    CAST(DATE_PART('week',  se.ts) as INT),
    CAST(DATE_PART('month', se.ts) as INT),
    CAST(DATE_PART('year',  se.ts) as INT),
    CAST(DATE_PART('dow',   se.ts) as INT)
FROM staging_events se
WHERE se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, artist_table_drop, time_table_drop, song_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
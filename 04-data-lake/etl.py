import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import logging
import sys
import argparse

logger = logging.getLogger('etl')

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'SECRET_ACCESS_KEY')


def create_spark_session():
    """
    Creates a new Spark session with the hadoop-aws plugin loaded.

    :return: Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Uses a given Spark session to process song data.

    :param input_data: Path to a folder or S3 bucket, where the input data lives.
    :param output_data: Path to a folder or S3 bucket, where the output should be stored.
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data', '*', '*', '*', "*.json")
    
    # read song data file
    logger.debug(f"Read data from {song_data}")
    df = spark.read.json(song_data)
    logger.debug("Auto detected JSON schema")
    df.printSchema()

    # extract columns to create songs table
    # Columns: song_id, title, artist_id, year, duration
    songs_table = df.selectExpr([
        "song_id",
        "title",
        "artist_id",
        "year",
        "duration"
    ]).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table_fp = os.path.join(output_data, 'dim_song.parquet')
    logger.debug(f"Write song table: {songs_table_fp}")
    songs_table.write.parquet(songs_table_fp, mode='overwrite', partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    # Columns: artist_id, name, location, lattitude, longitude
    artists_table = df.selectExpr([
        "artist_id",
        "artist_name as name",
        "artist_location as location",
        "artist_latitude as latitude",
        "artist_longitude as longitude"
    ]).dropDuplicates()
    
    # write artists table to parquet files
    artists_table_fp = os.path.join(output_data, 'dim_artist.parquet')
    logger.debug(f"Write artist table: {artists_table_fp}")
    artists_table.write.parquet(artists_table_fp, mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Uses a given Spark session to process log data.

    :param input_data: Path to a folder or S3 bucket, where the input data lives.
    :param output_data: Path to a folder or S3 bucket, where the output should be stored.
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data', '*.json')

    # read log data file
    logger.debug(f"Read data from {log_data}")
    df = spark.read.json(log_data)
    logger.debug('Auto detected JSON schema')
    df.printSchema()
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    # Columns: user_id, first_name, last_name, gender, level
    artists_table = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    users_table = df.selectExpr([
        "userId as user_id", 
        "firstName as first_name", 
        "lastName as last_name", 
        "gender", 
        "level"
    ]).dropDuplicates()
    
    # write users table to parquet file
    users_table_fp = os.path.join(output_data, 'dim_user.parquet')
    logger.debug(f"Write users table: {users_table_fp}")
    users_table.write.parquet(users_table_fp, mode='overwrite')

    # create timestamp column from original timestamp column
    df = df.withColumn("timestamp", F.to_timestamp(df.ts/1000))  # time is in millisecond
    
    # create datetime column from original timestamp column
    df = df.withColumn("datetime", F.to_date(df.timestamp))
    
    # extract columns to create time table
    # Columns: start_time, hour, day, week, month, year, weekday
    time_table = df.selectExpr([
        "timestamp as start_time",
        "hour(datetime) as hour",
        "dayofmonth(datetime) as day",
        "weekofyear(datetime) as week",
        "month(datetime) as month",
        "year(datetime) as year",
        "dayofweek(datetime) as weekday",
    ])
    
    # write time table to parquet files partitioned by year and month
    time_table_fp = os.path.join(output_data, 'dim_time.parquet')
    logger.debug(f"Write time table: {time_table_fp}")
    time_table.write.parquet(time_table_fp, mode='overwrite', partitionBy=["year", "month"])

    # read in song data to use for songplays table
    song_table_fp = os.path.join(output_data, 'dim_song.parquet')
    song_df = spark.read.parquet(song_table_fp)
    song_df = song_df\
        .selectExpr([
            'song_id as song_song_id',
            'artist_id as song_artist_id',
            'title as song_title'
        ])

    # read in artist data to use for songplays table
    artist_table_fp = os.path.join(output_data, 'dim_artist.parquet')
    artist_df = spark.read.parquet(artist_table_fp)
    artist_df = artist_df.selectExpr([
        'artist_id as artist_artist_id', 
        'name as artist_name'
    ])

    # extract columns from joined song and log datasets to create songplays table 
    # Columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    songplays_table = df.selectExpr([
            'timestamp as start_time',
            'userId as user_id',
            'level',
            'song',
            'artist',
            'sessionId as session_id',
            "location",
            'userAgent as user_agent'
        ])\
        .join(song_df, df.song==song_df.song_title, 'left_outer') \
        .join(artist_df, df.artist==artist_df.artist_name, 'left_outer') \
        .selectExpr([
            "start_time",
            "user_id", 
            "level",
            "song_song_id as song_id",
            "artist_artist_id as artist_id",
            "session_id",
            "location",
            "user_agent",
            "year(start_time) as year",
            "month(start_time) as month",
        ]) \
        .dropDuplicates() \
        .withColumn('songplay_id', F.monotonically_increasing_id())
        
    # write songplays table to parquet files partitioned by year and month
    songplays_table_fp = os.path.join(output_data, 'fact_songplay.parquet')
    logger.debug(f"Write songplay table: {songplays_table_fp}")
    songplays_table.write.parquet(songplays_table_fp, mode='overwrite', partitionBy=['year', 'month'])


def _dir_path(string):
    """
    Method to test if a given path is a directory.
    Idea from: https://stackoverflow.com/a/51212150

    :param string: Potential directory path.
    :raises NotADirectoryError: If given string is not a path to a directory
    """
    if os.path.isdir(string):
        return string
    else:
        raise NotADirectoryError(string)


def main():
    """
    Main program.
    """
    handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser('ETL pipeline in Spark')
    subparsers = parser.add_subparsers(dest="system")

    local_parser = subparsers.add_parser("local")
    local_parser.add_argument(
        '--source-dir',
        type=_dir_path,
        default=os.path.join(os.path.dirname(__file__), 'data'),
        help="Folder that contains the source files. Must be a local folder \
(without tailing slash)."
    )
    local_parser.add_argument(
        '--target-dir',
        type=_dir_path,
        default=os.path.join(os.path.dirname(__file__), 'output'),
        help="Folder in which the processed data should be stored. Must be a \
local folder (without tailing slash)."
    )

    remote_parser = subparsers.add_parser("remote")
    remote_parser.add_argument(
        '--s3-bucket-source',
        default="s3a://udacity-dend",
        help='Target bucket in which the processed data should be stored. \
Must have the following form: s3a://my-bucket (without tailing slash).'
    )
    remote_parser.add_argument(
        '--s3-bucket-target',
        required=True,
        help='Target bucket in which the processed data should be stored. \
Must have the following form: s3a://my-bucket (without tailing slash).'
    )
    cli_args = parser.parse_args()
    logger.debug(f"CLI args: {cli_args}")

    if cli_args.system == 'local':
        input_data = cli_args.source_dir
        output_data = cli_args.target_dir

        os.makedirs(cli_args.target_dir, exist_ok=True)
    else:
        input_data = cli_args.s3_bucket_source
        output_data = cli_args.s3_bucket_target

    logger.debug(f"Input data: {input_data}")
    logger.debug(f"Output data: {output_data}")

    spark = create_spark_session()
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

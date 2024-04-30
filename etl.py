import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import *


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create the SparkSession instance"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Extract Song data, process it with Spark, and write it back to S3"""
    # get filepath to song data file
    song_data = input_data + "/song_data/A/A/A/"

    # read song data file
    songSchema = StructType([
        StructField("num_songs", IntegerType()),
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_name", StringType()),
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("duration", DoubleType()),
        StructField("year", IntegerType())
    ])
    
    df = spark.read.json(song_data, schema=songSchema)

    # extract columns to create songs table
    df.createOrReplaceTempView("songs_view")
    songs_table = spark.sql("""
        SELECT song_id, title, artist_id, year, duration
        FROM songs_view
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table_path = output_data + "songs/songs_table.parquet"
    songs_table.write.partitionBy("year", "artist_id").parquet(songs_table_path)

    # extract columns to create artists table
    artists_table = spark.sql("""
        SELECT artist_id,
                artist_name AS name,
                artist_location AS location,
                artist_latitude AS latitude,
                artist_longitude AS longitude
        FROM songs_view
        LIMIT 5
    """)
    
    # write artists table to parquet files
    artists_table_path = output_data + "artists/artists_table.parquet"
    artists_table.write.parquet(artists_table_path)



def process_log_data(spark, input_data, output_data):
    """Extract Log data, process it with Spark, and write it back to S3"""
    
    # get filepath to log data file
    log_data = input_data + "log_data/2018/11/"

    # Spark is able to infer the schema in the case of the log data

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table   
    df.createOrReplaceTempView("logs_view")
    users_table = spark.sql("""
        SELECT DISTINCT userId AS user_id,
                        firstName AS first_name,
                        lastName AS last_name,
                        gender,
                        level
        FROM logs_view
        LIMIT 5
    """)
    
    # write users table to parquet files
    users_table_path = output_data + "users/users_table.parquet"
    users_table.write.parquet(users_table_path)

    # create timestamp column from original timestamp column
    @udf(TimestampType())
    def get_timestamp(ts):
        return datetime.fromtimestamp(ts / 1000.0)
    
    df = df.withColumn("timestamp", get_timestamp("ts"))
    
    
    # create datetime column from original timestamp column
    @udf(StringType())
    def get_datetime(ts):
        return datetime.fromtimestamp(ts / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
    
    df = df.withColumn("datetime", get_datetime("ts"))
    
    # extract columns to create time table
    df.createOrReplaceTempView("logs_view_2")
    time_table = spark.sql("""
        SELECT DISTINCT datetime AS start_time,
                        hour(timestamp) AS hour,
                        day(timestamp) AS day,
                        weekofyear(timestamp) AS week,
                        month(timestamp) AS month,
                        year(timestamp) AS year,
                        dayofweek(timestamp) AS weekday
        FROM logs_view_2
        LIMIT 5
    """)
    
    # write time table to parquet files partitioned by year and month
    time_table_path = output_data + "time/time_table.parquet"
    time_table.write.partitionBy("year", "month").parquet(time_table_path)

    # read in song data to use for songplays table
    song_data = input_data + "/song_data/A/A/A/"
    
    songSchema = StructType([
        StructField("num_songs", IntegerType()),
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_name", StringType()),
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("duration", DoubleType()),
        StructField("year", IntegerType())
    ])
    
    df_song = spark.read.json(song_data, schema=songSchema)
    df_song.createOrReplaceTempView("songs_view")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
        SELECT sessionID AS songplay_id, 
                timestamp AS start_time,
                year(timestamp) AS year,
                month(timestamp) AS month,
                userId AS user_id,
                level,
                song_id,
                artist_id,
                sessionId AS session_id,
                location,
                userAgent AS user_agent
        FROM logs_view_2 
        JOIN songs_view ON songs_view.title = logs_view_2.song
        LIMIT 5
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table_path = output_data + "songplays/songplays_table.parquet"
    songplays_table.write.partitionBy("year", "month").parquet(songplays_table_path)


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = config['LOCAL']['S3']
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

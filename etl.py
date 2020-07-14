import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

# AWS loading credentials from config file dl.cfg 
os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

# Create spark session with hadoop-aws package
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

'''
Load and process song data, create songs and artist tables, write parquet files
'''

def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data' + '/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # create temp view for song and artist table sql queries
    df.createOrReplaceTempView('songs_data')
    
    # extract columns to create songs table
    songs_table = spark.sql("""
    select distinct song_id, title, artist_id, year, duration
    from songs_data where song_id is not null""")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data + 'songs_table')       

    # extract columns to create artists table
    artists_table = spark.sql("""
    select distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    from songs_data where artist_id is not null""") 
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists_table')

'''
Load and process log data, filter and create: songplays, users and time tables, write parquet files
'''

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data' + '/*/*/*.json'

    # read log data file
    df_log = spark.read.json(log_data) 
    
    # filter by actions for song plays
    df_log = df_log.filter(df_log.page == 'NextSong')

    # create temp view for user table sql queries
    df_log.createOrReplaceTempView('logs_data')
    
    # extract columns for users table    
    users_table = spark.sql("""
    select distinct userId, firstName, lastName, gender, level
    from logs_data where userId is not nul""")
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + "users_table")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.fromtimestamp(x/1000.0).strftime('%Y-%m-%d %H:%M:%S'))

    df_log = df_log.withColumn('timestamp', get_timestamp(df_log['ts']))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df_log = df_log.withColumn('datetime', get_datetime(df_log['ts']))
    
    # create temp view for time_table sql queries
    df_log.createOrReplaceTempView('time_data')
    
    # extract columns to create time table
    time_table = spark.sql("""
    select distinct datetime as start_time,
        hour(datetime) as hour,
        day(datetime) as day,
        weekofyear(datetime) as week,
        month(datetime) as month,
        year(datetime) as year,
        dayofweek(datetime) as weekday
        from time_data""") 
    
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'time_table')

    # read in song data to use for songplays table
    song_df = spark.read.json(song_data)
    
    #create temp log and song tables
    df_log.createOrReplaceTempView('logs_data')
    song_df.createOrReplaceTempView('songs_data') 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
    select monotonically_increasing_id() as songplay_id,
        to_timestamp(l.ts/1000) as start_time,
        month(to_timestamp(l.ts/1000)) as month,
        year(to_timestamp(l.ts/1000)) as year,
        l.userId as user_id,
        l.level as level,
        s.song_id as song_id,
        s.artist_id as artist_id,
        l.sessionId as session_id,
        l.location as location,
        l.userAgent as user_agent
    from logs_data l join songs_data s 
    on l.artist = s.artist_name and l.song = s.title """) 

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'songplays_table')                     


'''
Create spark session, process song and log data to fact and dimension tables, write parquet file to S3, stop spark session
'''    
    
def main():
    spark = create_spark_session()
    input_data = "s3://udacity-dend/"
    output_data = "s3://data-lake-project/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    spark.stop()


if __name__ == "__main__":
    main()

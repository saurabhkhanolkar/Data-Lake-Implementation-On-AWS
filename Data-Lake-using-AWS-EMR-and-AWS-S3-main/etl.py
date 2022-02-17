import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType, DateType

config = configparser.ConfigParser()
config.read('dl.cfg')

#Giving IAM role key and Password
os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')

#Creating a Spark Session
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ Load song data from S3, transform the data
    and write it as partitioned parquet file
    """
    # get filepath to song data file
    song_data =input_data+ 'song_data/A/A/A/TRAAAAK128F9318786.json'#'song_data/*/*/*/*.json' 
    
    # read song data file
    df = spark.read.json(song_data)
    
    df.limit(10).toPandas()

    ## extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    songs_table.createOrReplaceTempView("song_data_table")

    songs_table = spark.sql("""
                            SELECT DISTINCT song_id, 
                            title,
                            artist_id,
                            year,
                            duration
                            FROM song_data_table 
                            WHERE song_id IS NOT NULL
                        """)

    songs_table.createOrReplaceTempView("songs")
    ## write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode('overwrite').parquet(os.path.join(output_data, 'songs'))

    ## extract columns to create artists table
    artists_table=df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
    artists_table=artists_table.withColumnRenamed("artist_name","name").withColumnRenamed("artist_location","location").withColumnRenamed("artist_latitude","lattitude").withColumnRenamed("artist_longitude","longitude")

    artists_table.createOrReplaceTempView("artist_data_table")

    artists_table = spark.sql("""
                                SELECT DISTINCT artist_id, 
                                name,
                                location,
                                lattitude,
                                longitude
                                FROM artist_data_table 
                                WHERE artist_id IS NOT NULL
                            """)

    
    ## write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(os.path.join(output_data, 'artists'))


def process_log_data(spark, input_data, output_data):
     """ Load events data from S3, transform the data
    and write it as partitioned parquet file
    """
    # get filepath to log data file
    log_data =input_data+ 'log_data/2018/11/2018-11-13-events.json'#'log_data/2018/11/*.json' 

    # read log data file
    df = spark.read.json(log_data)
    

    # filter by actions for song plays
    df  = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select("userid", "firstName", "lastName", "gender", "level").withColumnRenamed("userid","user_id").withColumnRenamed("firstName","first_name").withColumnRenamed("lastName","last_name")
    
    users_table.createOrReplaceTempView("users_data_table")

    # extract columns for users table    
    users_table = spark.sql("""
                        SELECT DISTINCT user_id, 
                        first_name,
                        last_name,
                        gender,
                        level
                        FROM users_data_table 
                        WHERE user_id IS NOT NULL """)
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    get_time = udf(lambda x: datetime.fromtimestamp(x / 1000.0), TimestampType())
    df = df.withColumn("start_time", get_time("ts"))
    
    # # create datetime column from original timestamp column
    get_date = udf(lambda x: datetime.fromtimestamp(x / 1000.0), DateType())
    df = df.withColumn("date", get_date("ts"))
    
    # extract columns to create time table
    # columns = start_time, hour, day, week, month, year, weekday
    time_table = df.select("start_time", 
                           hour("date").alias("hour"), 
                           dayofmonth("date").alias("day"), 
                           weekofyear("date").alias("week"), 
                           month("date").alias("month"),
                           year("date").alias("year"),
                           dayofweek("date").alias("weekday")
                        ).distinct() 
        
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode('overwrite').parquet(os.path.join(output_data, 'time'))


    
    song_df = df.select("song", "artist",
                             "length", 
                             "page", 
                             "start_time",
                             "userId", 
                             "level", 
                             "sessionId",
                             "location", 
                             "userAgent",
                            month("date").alias("month"),
                            year("date").alias("year"),
                            )
    
    song_df.createOrReplaceTempView("logs")
    
    songplays_table = spark.sql(
                """
                SELECT row_number() OVER (PARTITION BY start_time ORDER BY start_time) as songplay_id,
                       e.start_time, 
                       e.userId AS user_id, 
                       e.level AS level, 
                       s.song_id AS song_id, 
                       s.artist_id AS artist_id, 
                       e.sessionId AS session_id, 
                       e.location AS location, 
                       e.userAgent AS user_agent,
                       e.year,
                       e.month
                FROM logs e
                LEFT JOIN songs s 
                       ON e.song=s.title
                LEFT JOIN artist_data_table a 
                       ON e.artist=a.name
                """
            )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode('overwrite').parquet(os.path.join(output_data, 'songplays'))




def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://output0033/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

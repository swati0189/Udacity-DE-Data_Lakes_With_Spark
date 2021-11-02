"""
Import all necessary python modules/packages necessary for the ETL process. 
"""

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as f
from pyspark.sql import types as t

def create_spark_session():
    """ 
    This function creates a Spark Sesson and includes necessary Jar and adoop packages in the configuration. 
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function process the set of Song Json files from S3, extracts Song and Artist data, save them in dataframes and finally write the dataframes back to S3 as parquet file. 
    """
        
    # get filepath to song data file
    
    song_data = input_data + "song-data/*/*/*/*.json"
    

    # read song data file    
    df = spark.read.json(song_data)
    
    df.createOrReplaceTempView('songs')

    # extract columns to create songs table
    songs_table = df.select(df.song_id, \
                            df.title, \
                            df.artist_id, \
                            df.year, \
                            df.duration) \
                    .dropDuplicates(["song_id"])
    
    
    


   # write songs table to parquet files partitioned by year and artist
     
    songs_table.write.parquet(output_data+'songs/'+'songs.parquet', partitionBy=['year','artist_id'])
 

    # extract columns to create artists table
    artists_table = df.select(df.artist_id,\
                             df.artist_name,\
                             df.artist_location,\
                             df.artist_latitude,\
                             df.artist_longitude)\
                            .dropDuplicates(["artist_id"])
    
    
    
    
    
    artists_table.createOrReplaceTempView('artists')
        
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/' + 'artists.parquet', partitionBy=['artist_id'] )
    

def process_log_data(spark, input_data, output_data):
    
    """
    This function process the set of Log Json files from S3, extracts Users, Time data, extracts the Songplays data by joining Song and Log Files, save them in dataframes and finally write the dataframes back to S3 as parquet file. 
    """
        
    # get filepath to log data file
    
    
    log_data = input_data + "{}log-data/*/*/*.json"
    
    
    
    # read log data file
    df =  spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df['page'] == "NextSong")
    
    users_table = df.select(df.userId,\
                           df.firstName,\
                           df.lastName,\
                           df.gender,\
                           df.level)\
                          .dropDuplicates(["userId"])
    
    
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/' + 'users.parquet', partitionBy = ['userId'])
    
    
    # create datetime column from original timestamp column
    get_timestamp = udf(lambda x : datetime.fromtimestamp(x/1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    df.createOrReplaceTempView("logs")
     
    
    # extract columns to create time table
    time_table = df.select(
                    f.col("timestamp").alias("start_time"),
                    f.hour("timestamp").alias('hour'),
                    f.dayofmonth("timestamp").alias('day'),
                    f.weekofyear("timestamp").alias('week'),
                    f.month("timestamp").alias('month'), 
                    f.year("timestamp").alias('year'), 
                    f.date_format(f.col("timestamp"), "E").alias("weekday")
                ).sort(df.timestamp).dropDuplicates()
    
    
    time_table.createOrReplaceTempView("time")
    
    # write time table to parquet files partitioned by year and month
   
    time_table.write.parquet(output_data + 'time/' + 'time.parquet', partitionBy=['year','month'])
   

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table =  spark.sql("""
                                 SELECT 
                                 row_number() over (order by l.ts) as songplay_id,
                                 l.ts as start_time,
                                 year(l.timestamp) year,
                                 month(l.timestamp) month,
                                 l.userId,
                                 l.level,
                                 s.song_id,
                                 s.artist_id,
                                 l.sessionId,
                                 l.location,
                                 l.userAgent
                                 FROM logs l 
                                 JOIN songs s
                                 ON l.artist=s.artist_name 
                                 AND l.song= s.title
                                 and l.length = s.duration
                                """)
    

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + 'songplays/' + 'songplays.parquet',partitionBy=['year','month'])
    
   

def main():
    """
    The Main Function calls 3 different functions to create a spark session, to process the Song Data and to process the Log Data and assign aws credentials to process the data. 
        """

    # Load AWS credentials as env vars
    config = configparser.ConfigParser()
    config.read_file(open('dl.cfg'))
    
    os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']
    
    # Set input path and output path
    input_data = "s3a://udacity-dend/"
    output_data  = "s3a://udacity-de-sparkify-datalake/" 
        
    # Create a Spark Session
    spark = create_spark_session()
    
    # Run the ETL to Process song dataset and log dataset
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
       
    
if __name__ == "__main__":
    main()
    
    


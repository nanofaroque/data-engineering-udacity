import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
#config.read('dl.cfg')

#os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "./data/song_data/A/*/*"
    
    # read song data file
    df = spark.read.json(song_data)
    print('Number of data: '+ str(df.count()))

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id',
                            'year', 'duration').dropDuplicates()
    print(songs_table.describe())

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(f'./output_data',mode='overwrite',partitionBy=['year','artist_id'])
    '''

    # extract columns to create artists table
    artists_table = 
    
    # write artists table to parquet files
    artists_table
'''

'''
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data =

    # read log data file
    df = 
    
    # filter by actions for song plays
    df = 

    # extract columns for users table    
    artists_table = 
    
    # write users table to parquet files
    artists_table

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = 
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = 
    
    # extract columns to create time table
    time_table = 
    
    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table'''


def main():
    spark = create_spark_session()
    #input_data = "s3a://udacity-dend/"
    input_data=""
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    #process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

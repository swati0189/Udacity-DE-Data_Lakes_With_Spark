## Project: Building an ETL pipeline for Data Lake hosted on Amazon S3 using Apache Spark

### Project Introduction 

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As data engineer,the project is building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

### Project Datasets
This project uses two datasets from AWS S3:

**Song data:** s3://udacity-dend/song_data

**Log data:** s3://udacity-dend/log_data


***Song Dataset***

The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

***song_data/A/B/C/TRABCEI128F424C983.json***

***song_data/A/A/B/TRAABJL12903CDCF1A.json***

***Log Dataset***

The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

The log files in the dataset are partitioned by year and month.For example, here are filepaths to two files in this dataset.

***log_data/2018/11/2018-11-12-events.json***

***log_data/2018/11/2018-11-13-events.json***

### Project Files
The project template includes the following files:

|File Name | Description
|-----|----|
| **etl.py** | loads data from S3, use Apache Spark to extract and then save to S3 as Parquet |
|**dl.cfg** | configuration file to pass aws credential to run ETL process on spark|
|**data/song-data.zip** | Sample song dataset for development|
|**data/log-data.zip** | Sample log dataset for development|
|**sparkify_on_Spark.ipynb**| Jupyter Notebook to test the Code |


### Schema

The project is to do extract data available in JSON format from Amazon S3 service and then transform it and load into star schema with fact and dimension tables in parquet format.




*Fact Table*

|Table Name | Description
|-------|------|
|songplays| Records in event data associated with song plays i.e. records with page NextSong, it stores following attributes songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent and table sonplay is partitioned on artist_id   |

*Dimension Tables*


|Table Name | Description
|-------|------|
|users | Table stores information related to users and holds following attributes user_id, first_name, last_name, gender, level and is partitioned on year & month|
|songs | Table stores information related to songs and holds following attributes song_id, title, artist_id, year, duration and is partitioned on year & artist_id|
|artists |Table stores information related to artists and holds following attributes art ist_id, name, location, lattitude, longitude and is partitioned on artist_id| 
|time | Table stores information related to timestamps of records in songplay and holds following attributes start_time, hour, day, week, month, year, weekday and is partitioned on year & month |

### Deployment

1. Configure dl.cfg with AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY , This are the AWS creedentials that have read and write access to S3. 
2. Set input_path as s3a://udacity-dend/ for Udacity data 
3. Set outptu_path to a S3 bucket to which you have access.
4. ETL code written etl.py file which also access above two steps
5. Set IAM roles and run the above code on the EMR cluster 

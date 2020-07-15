### Data Lake project with Spark, AWS EMR and S3

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project we will do an ETL process for song and log data files to make dimension and fact tables and store them in S3 bucket, 
that Sparkify's analytics can use these tables for their analysis.



### ETL Pipeline

1. Create spark session with hadoop-aws package 
2. Load and read JSON files from AWS S3
3. Load and process song and log data, create tables:
- songs 
- artist 
- users 
- time
- songplays
4. Write these tables above as parquet files and load them back to S3


### How to run

1. Create an AWS S3 bucket (with your own path to output_data)
2. Add (or create a new IAM and add) the required AWS login credentials to the dl.cfg file
3. In terminal type python etl.py

Or log in AWS console and set up AWS EMR cluster with Hadoop, Spark, Hive, Livy and run ETL-for-AWS-S3 -notebook in Jupyter notebook's environment


## Author
Vesa Jaakola

## Acknowledgements
Must give credit to Udacity

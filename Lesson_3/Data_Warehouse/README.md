# Building ETL pipeline
This project is to build a ETL pipeline by leveraging AmazonRedshift. There are lots of 
ETL tools outside, Amazon Redshift can be used as one of them as a high performent and highly 
scalable. ETL means Extract, Transform and Load. ETL is required for serving OLTP request. 

### What are achieving here?
We will be achieving OLTP by using Redshift.

Extract - We will read the data from S3 buckets by using COPY 
command for faster copying and load into staging table.

Transform - We will clean up the data by finding valuable column

Load - For loading phase, we will keep inside the redshift but in real world you can store any storage or S3

Here is how it looks like: 
![](https://github.com/nanofaroque/data-engineering-udacity/blob/master/Data_Warehouse_Project_Template/desing_screen.png)

### Project structure details
* All the sql query reside inside the ```sql_queries.py``` file
* For creating the table, we have to run ```create_tables.py``` file
* Full ETL job can be run by running ```etl.py```
* All the configuration would be stored in inside ```dwh.cfg```
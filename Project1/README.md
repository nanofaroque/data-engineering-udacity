#Data Modeling with Postgres
This project is a sample data pipeline project. 
It reads some json log and song data file from the directory
called 'data' and process it or transform it. Finally it stores 
those data into the postgres database. 

### Repository details
*data* directory contains the log files for log data called *log_data*
and song data contained inside the *song_data* directory. 
* create_tables.py has been used to drop the existing table and 
create the new table by reading the sql_queries.py file. 
* etl.ipynb is just nothing but interactive mode of the whole pipeline
* etl.py is the starting point for running the application to run the pipeline
* test.ipynb makes sure that all the table has been created properly. 
#### Create table
You can create table by running the script below
```
python create_tables.py
```

#### Run the ETL pipeline
```.env
python etl.py
```

### Database Schema design
 ![Image](https://github.com/nanofaroque/data-engineering-udacity/blob/master/Project1/project1.png)

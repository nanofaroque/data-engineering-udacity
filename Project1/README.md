#Data Modeling with Postgres
This project is a sample data pipeline project. 
It reads some json log and song data file from the directory
called 'data' and process it or transform it. Finally it stores 
those data into the postgres database. 
###Create table
You can create table by running the script below
```
python create_tables.py
```

### Run the ETL pipeline
```.env
python etl.py
```
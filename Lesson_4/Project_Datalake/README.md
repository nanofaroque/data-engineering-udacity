## Project Data Lake

This is a demo project for utilizing the power of datalake on AWS platform. 

### What is a data lake?
A data lake is a centralized repository that allows you to store all 
your structured and unstructured data at any scale. You can store your 
data as-is, without having to first structure the data, and run different
types of analytics—from dashboards and visualizations to big data
processing, real-time analytics, and machine learning to guide better 
decisions.
 
<img src="https://github.com/nanofaroque/data-engineering-udacity/blob/master/Lesson_4/Project_Datalake/data_lake.png" width="100%"/>
 
### Data format 
We have a directory called data, contains log data and song data.  Here is the 
data in the song_data: 
```
{"num_songs": 1, "artist_id": "ARD7TVE1187B99BFB1", "artist_latitude": null, "artist_longitude": null, "artist_location": "California - LA", "artist_name": "Casual", "song_id": "SOMZWCG12A8C13C480", "title": "I Didn't Mean To", "duration": 218.93179, "year": 0}
```
Data from the log_data: 
```
{"artist":null,"auth":"Logged In","firstName":"Walter","gender":"M","itemInSession":0,"lastName":"Frye","length":null,"level":"free","location":"San Francisco-Oakland-Hayward, CA","method":"GET","page":"Home","registration":1540919166796.0,"sessionId":38,"song":null,"status":200,"ts":1541105830796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"39"}
```

### Star Schema
We are reading those json file and creating star schema like below

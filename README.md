# Spark Streaming integration with Kafka and Hive
This project created by using Apache Spark Direct Stream to read web requests logs data as messages 
from Apache Kafka and store them inside Apache Hive table  

 
## Sample of the data

10.131.2.1,30/Nov/2017:15:50:53,GET /details.php?id=43 HTTP/1.1,200

10.131.2.1,30/Nov/2017:15:34:56,POST /process.php HTTP/1.1,302

10.131.2.1,02/Dec/2017:18:35:48,GET /fonts/fontawesome-webfont.woff2?v=4.6.3 HTTP/1.1,304

10.129.2.1,14/Nov/2017:02:54:51,GET /robots.txt HTTP/1.1,404

10.130.2.1,22/Nov/2017:23:21:04,POST /process.php HTTP/1.1,302

## Project goals

This goal is to read the streaming logs of web requests from Kafka and store them inside apache hive table and the 
Columns are IP, DateTime, RequestType, Page , ResponseStatus generated from for each web request log .


Also you can find the data on Kaggle web site using the below URL 

* https://www.kaggle.com/shawon10/web-log-dataset
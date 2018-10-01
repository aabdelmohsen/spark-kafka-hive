# Spark Streaming integration with Kafka and Hive
This project helps you to understand how to create Spark Direct Stream to stream log data messages 
from Apache Kafka and save the messages as records inside Hive table  

## About the data

This dataset has 16008 rows and 4 columns. Columns are IP, Time, URL, Response Status for each web request
and you can find the data on Kaggle web site using this URL 

* https://www.kaggle.com/shawon10/web-log-dataset
 
## Sample of the data

10.131.2.1,30/Nov/2017:15:50:53,GET /details.php?id=43 HTTP/1.1,200
10.131.2.1,30/Nov/2017:15:34:56,POST /process.php HTTP/1.1,302

10.128.2.1,30/Nov/2017:15:35:15,GET /contestproblem.php?name=RUET%20OJ%20Server%20Testing%20Contest HTTP/1.1,200

10.131.2.1,02/Dec/2017:18:35:48,GET /fonts/fontawesome-webfont.woff2?v=4.6.3 HTTP/1.1,304

10.129.2.1,14/Nov/2017:02:54:51,GET /robots.txt HTTP/1.1,404

10.130.2.1,22/Nov/2017:23:21:04,POST /process.php HTTP/1.1,302


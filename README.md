# Building-ETL-pipeline-with-Apache-Airflow
Building ETL pipeline with Apache Airflow
Write a DAG named ETL_Server_Access_Log_Processing
Download task:
download task must download the server access log file 
Extract task:
The server access log file contains these fields.

a. timestamp - TIMESTAMP

b. latitude - float

c. longitude - float

d. visitorid - char(37)

e. accessed_from_mobile - boolean

f. browser_code - int

The extract task  extract the fields timestamp and visitorid
Transform task:
The transform task capitalize the visitorid
Load task:
The load task compress the extracted and transformed data.

The pipeline block schedule the task in the order listed below:
download
extract
transform
load

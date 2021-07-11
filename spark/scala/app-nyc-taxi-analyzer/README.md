# app-nyc-taxi-analyzer

## Intoduction
Application "app-nyc-taxi-analyzer" is a data pipeline developed using Apache Spark and Scala.It reads csv files from a location (either HDFS or 
local file system), performs data quality, data enrichment and stores the enriched data to another location (either HDFS or
local file system).

## Data Sources
There are two data source files which are used in this application. Below are the details for the same.

1) **NYC TLC Trip Record Data** : The dataset include fields capturing pick-up and drop-off dates/times, pick-up and drop-off locations, trip distances, 
   itemized fares, rate types, payment types, and driver-reported passenger counts. More information about the dataset could be 
   found here https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

   **_Schema of the data:_**

   field_name | date
   ---        | ---  
   data_type  | date 
 

2) **Weather Data Set** : The dataset contains information about weather in NewYork for a specified year. More information about the dataset could be 
   found here https://www.kaggle.com/mathijs/weather-data-in-new-york-city-2016
   
    **_Schema of the data:_**

    field_name | date | maximumtemperature |  minimumtemperature | averagetemperature | precipitation | snowfall | snowdepth
    ---        | ---  | ---  | ---  | ---  | ---  | ---  | ---  
    data_type  | date | double | double | double | double | double | double
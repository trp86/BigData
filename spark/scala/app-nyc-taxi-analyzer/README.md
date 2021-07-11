# app-nyc-taxi-analyzer

## Intoduction
Application "app-nyc-taxi-analyzer" is a data pipeline developed using Apache Spark and Scala.It reads csv files from a location (either HDFS or 
local file system), performs data quality, data enrichment and stores the enriched data to another location (either HDFS or
local file system).

## Data Ingestion
There are two data source files which are used in this application. Below are the details for the same.

1) **NYC TLC Trip Record Data** : The dataset include fields capturing pick-up and drop-off dates/times, pick-up and drop-off locations, trip distances, 
   itemized fares, rate types, payment types, and driver-reported passenger counts. More information about the dataset could be 
   found here https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

   **_Schema of the data:_**

   field_name | vendor_id | pickup_datetime | dropoff_datetime | passenger_count | trip_distance | pickup_longitude | pickup_latitude | rate_code | store_and_fwd_flag | dropoff_longitude | dropoff_latitude | payment_type | fare_amount | surcharge | mta_tax | tip_amount | tolls_amount | total_amount
   ---        | ---  | ---  | ---  | ---  | ---  | ---  | ---  | ---  | ---  | ---  | ---  | ---  | ---  | ---  | ---  | ---  | ---  | ---  
   data_type  | string | timestamp | timestamp | int | double | string | string | int | string | string | string | string | double | double | double | double | double | double
 

2) **Weather Data Set** : The dataset contains information about weather in NewYork for a specified year. More information about the dataset could be 
   found here https://www.kaggle.com/mathijs/weather-data-in-new-york-city-2016
   
    **_Schema of the data:_**

    field_name | date | maximumtemperature |  minimumtemperature | averagetemperature | precipitation | snowfall | snowdepth
    ---        | ---  | ---  | ---  | ---  | ---  | ---  | ---  
    data_type  | date | double | double | double | double | double | double

A subset of the **NYC TLC Trip Record Data** (year 2014) data and **Weather Data Set** for all the months of 2014 was used. CSV file format is used for this
analysis.

## Data Preparation and cleaning

Below are the tasks that are performed during this phase.

1) **Header Check** : As the file format used is CSV hence both the data files will have a header. Hence, for both the datasets header validation is done. If header matches then the files are processed further or else it is rejected. 
   
2) **Column Name Check** : If any of the columns have a reserved keyword then the column names are modified. Only one column name is renamed here. In weather dataset there is a column named '**date**'. This column is renamed to '**weather_date**'.
   
3) **Negative Value Check** : Few columns should never have negative values. If these columns have negative value then records are rejected. For trip data these columns undergo this check ```trip_distance,fare_amount,surcharge,mta_tax,tip_amount,
   tolls_amount,total_amount,passenger_count```  and for weather data these columns undergo this check ```precipitation,snowfall,snowdepth```
   
4) **Date Or Timestamp column Check** : The date and timestamp columns should have valid dates and timestamp. If not then the records are rejected. For trip data ```pickup_datetime,dropoff_datetime```  columns undergo this check.  

5) **Assign proper data type** : Most appropriate data types are assigned to the columns. Below are the details for trip data.

field_name | vendor_id | pickup_datetime | dropoff_datetime | passenger_count | trip_distance | pickup_longitude | pickup_latitude | rate_code | store_and_fwd_flag | dropoff_longitude | dropoff_latitude | payment_type | fare_amount | surcharge | mta_tax | tip_amount | tolls_amount | total_amount
   ---     | ---  | ---  | ---  | ---  | ---  | ---  | ---  | ---  | ---  | ---  | ---  | ---  | ---  | ---  | ---  | ---  | ---  | ---  
data_type  | string | timestamp | timestamp | int | double | string | string | int | string | string | string | string | double | double | double | double | double | double

Below are the details for weather data.

field_name | weather_date | maximumtemperature |  minimumtemperature | averagetemperature | precipitation | snowfall | snowdepth
---        | ---  | ---  | ---  | ---  | ---  | ---  | ---  
data_type  | date | decimal:(14,4) | decimal:(14,4) | decimal:(14,4) | decimal:(14,4) | decimal:(14,4) | decimal:(14,4)

6) **Column or Value Compare** : Few columns are usually present which have certain limit of ceiling value. If the values of these columns exceed 
   than ceiling value then the records are rejected. 
   For trip data below checks are done.
   - pickup_datetime < dropoff_datetime
   - trip_distance <= 100 

    For weather data below checks are done.
    - minimumtemperature < maximumtemperature

7) **Adding additional columns** : For trip data below columns are added to original data.
   - **_trip_date_**
   - **_trip_hour_**
   - **_trip_day_of_week_**
   To derive these columns ```pickup_datetime``` column is used
  
   For weather data below columns are added.
    - **_temperature_condition_**
    
    Condition               | value
    ---                     | ---- 
    averagetemperature < 32 | verycold
    averagetemperature >= 32 && averagetemperature < 59 | cold
    averagetemperature >= 59 && averagetemperature < 77 | normal
    averagetemperature >= 77 && averagetemperature < 95 | hot
    averagetemperature > 95 | veryhot
   
    **NOTE** : All temparatures are in Fahrenheit

    - **_snowfall_condition_**

     Condition               | value
     ---                     | ---- 
     snowfall < 0.0001 | nosnow
     snowfall >= 0.0001 && snowfall < 4 | moderate
     snowfall >= 4 && snowfall < 15 | heavy
     snowfall >= 15 | violent 

    - **_snowdepth_condition_**

    Condition               | value
     ---                     | ---- 
     snowdepth < 0.0001 | nosnow
     snowdepth >= 0.0001 && snowdepth < 4 | moderate
     snowdepth >= 4 && snowdepth < 15 | heavy
     snowdepth >= 15 | violent 

    - **_rain_condition_**

    Condition               | value
    ---                     | ---- 
    precipitation <= 0 | norain
    precipitation > 0 && precipitation < 0.3 | moderate
    precipitation >= 0.3 && precipitation < 2 | heavy
    precipitation >= 2 | violent



   
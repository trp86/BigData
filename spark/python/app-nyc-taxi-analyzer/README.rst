.. image:: https://img.shields.io/badge/python-3.8-blue
    :target: https://img.shields.io/badge/python-3.8-blue
    :alt: Python version

.. image:: https://img.shields.io/badge/spark-3.0-orange
    :target: https://img.shields.io/badge/spark-3.0-orange
    :alt: Spark version

=====================
app-nyc-taxi-analyzer
=====================

Introduction
============

Application "app-nyc-taxi-analyzer" is a data pipeline developed using Apache Spark and Python.
It reads csv files from a location (either HDFS or local file system), performs data quality, 
data enrichment and stores the enriched data to another location (either HDFS or local file system). Below
are the steps or processes which are performed.

- **Data Ingestion**
- **Data Preparation and cleaning**
- **Data Processing**
- **Technology Used**
- **Best Practices**

**Data Ingestion**
==================

There are two data source files which are used in this application. Below are the details for the same.

- **NYC TLC Trip Record Data** :  The dataset include fields capturing pick-up and drop-off dates/times, pick-up 
  and drop-off locations, trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts.
  More information about the dataset could be found here https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

  *Schema of the data:*

  ==================  ==========
  field_name          data_type
  ==================  ==========
  vendor_id             string
  pickup_datetime       timestamp
  dropoff_datetime	    timestamp
  passenger_count		int
  trip_distance		    double
  pickup_longitude	    string
  pickup_latitude		string
  rate_code			    int
  store_and_fwd_flag    string 
  dropoff_longitude     string
  dropoff_latitude      string
  payment_type          string
  fare_amount           double
  surcharge             double
  mta_tax				double
  tip_amount            double
  tolls_amount          double
  total_amount          double
  ==================  ==========

- **Weather Data Set** : The dataset contains information about weather in NewYork for a specified year.More information 
  about the dataset could be found here https://www.kaggle.com/mathijs/weather-data-in-new-york-city-2016

  *Schema of the data:*

  ==================  ==========
  field_name          data_type
  ==================  ==========
  date                  date
  maximumtemperature    double
  minimumtemperature	double
  averagetemperature    double
  precipitation		    double
  snowfall	            double
  snowdepth		        double
  ==================  ==========


**Data Preparation and cleaning**
=================================
Below are the tasks that are performed during this phase.

- **Header Check** : As the file format used is CSV hence both the data files will have a header. Hence, for both the datasets header validation is done. If header matches then the files are processed further or else it is rejected.
- **Column Name Check** : If any of the columns have a reserved keyword then the column names are modified. Only one column name is renamed here. In weather dataset there is a column named 'date'. This column is renamed to 'weather_date'.
- **Negative Value Check** : Few columns should never have negative values. If these columns have negative value then records are rejected.For trip data,columns named *trip_distance,fare_amount,surcharge,mta_tax,tip_amount,tolls_amount,total_amount,passenger_count* undergo this check and for weather data,columns named *precipitation,snowfall,snowdepth* undergo this check. 
- **Date Or Timestamp column Check** : The date and timestamp columns should have valid dates and timestamp. If not then the records are rejected. For trip data *pickup_datetime,dropoff_datetime* columns undergo this check.
- **Assign proper data type** : Most appropriate data types are assigned to the columns. Below are the details for trip data.
- **Column or Value Compare** : Few columns are usually present which have certain limit of ceiling value. If the values of these columns exceed than ceiling value then the records are rejected. For trip data below checks are done.
     
- **Replace Values**: In the weather dataset there are few columns of decimal data type which has values as 'T'. This value indicates that value is negligible but not 0. Hence, these values are replaced by 0.0001. Below are the columns which undergo this transformation.
         - maximumtemperature
         - minimumtemperature
         - averagetemperature
         - precipitation
         - snowfall
         - snowdepth
- **Adding additional columns**: For trip data below columns are added to original data set.
         - trip_date
         - trip_hour
         - trip_day_of_week
  
  To derive these columns *pickup_datetime* column is used.
  
  For weather data below columns are added.
          - temperature_condition
            
            **NOTE** : All temparatures are in Fahrenheit
			      
            ====================================================        =========
             Condition                                                  value 
            ====================================================        =========
             averagetemperature <32                                     verycold
             averagetemperature >= 32 && averagetemperature < 59        cold
             averagetemperature >= 59 && averagetemperature < 77        normal
             averagetemperature >= 77 && averagetemperature < 95        hot
             averagetemperature > 95                                    veryhot
            ====================================================        =========    

          - snowfall_condition
            
            ====================================================        =========
             Condition                                                  value 
            ====================================================        =========
             snowfall < 0.0001                                          nosnow
             snowfall >= 0.0001 && snowfall < 4                         moderate
             snowfall >= 4 && snowfall < 15                             heavy 
             snowfall >= 15                                             violent
            ====================================================        =========    

          - snowdepth_condition
            
            ====================================================        =========
             Condition                                                  value 
            ====================================================        =========
             snowdepth < 0.0001                                         nosnow
             snowdepth >= 0.0001 && snowdepth < 4                       moderate
             snowdepth >= 4 && snowdepth < 15                           heavy 
             snowdepth >= 15                                            violent
            ====================================================        =========    

          - rain_condition
            
            ====================================================        =========
             Condition                                                  value 
            ====================================================        =========
             precipitation <= 0                                         norain
             precipitation > 0 && precipitation < 0.3                   moderate
             precipitation >= 0.3 && precipitation < 2                  heavy 
             precipitation >= 2                                         violent
            ====================================================        =========    

  Records which fail to satisfy the above rules are marked as error records with a reject reason. Schema of the error records have one extra column rejectReason.  


**Data Processing**
=====================
Weather and trip data (only success records) undergo a left outer join and final dataframes are created. Columns used for join are trip_date (trip dataset) and weather_date (weather dataset).
 
*Schema of the data:*

=====================   ==========
field_name              data_type
=====================   ==========
vendor_id               string
pickup_datetime         timestamp
dropoff_datetime	      timestamp
passenger_count         int
trip_distance 	        double
pickup_longitude 	      int
pickup_latitude 	      int
rate_code 	            int
store_and_fwd_flag 	    string
dropoff_longitude 	    int
payment_type 	          string
fare_amount 	          int
surcharge 	            int
mta_tax 	              int
tip_amount 	            int
tolls_amount 	          int
total_amount 	          int
trip_date 	            date
trip_hour 	            int
trip_day_of_week 	      int
weather_date 	          date
maximumtemperature 	    decimal(14,4)
minimumtemperature 	    decimal(14,4)
averagetemperature 	    decimal(14,4)
precipitation 	        decimal(14,4)
snowfall 	              decimal(14,4)
snowdepth 	            decimal(14,4)
temperature_condition 	string
snowfall_condition 	    string
snowdepth_condition 	  string
rain_condition 	        string
=====================   ==========

Processed data are persisted in partitioned format. Columns used for partitioning are weather_date. Error records are also persisted in partitioned format. Columns used for partitioning are rejectReason.


**Technology Used**
===================
- Python
- Sphinx
- Flake8
- pytest
- pyspark
- pandas
- poetry 
  
**NOTE** : More details about the dependencies and version can be found in **pyproject.toml**


**Best Practices**
===================

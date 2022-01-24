.. image:: https://img.shields.io/badge/python-3.8-blue
    :target: https://img.shields.io/badge/python-3.8-blue
    :alt: Python version

=====================
app-nyc-taxi-analyzer
=====================

Introduction
============

Application "app-nyc-taxi-analyzer" is a data pipeline developed using Apache Spark and Python.
It reads csv files from a location (either HDFS or local file system), performs data quality, 
data enrichment and stores the enriched data to another location (either HDFS or local file system). Below
are the steps or processes which are performed.
- Data Ingestion
- Data Preparation and cleaning
- Data Processing
- Technology Used
- Best Practices

Data Ingestion
--------------

There are two data source files which are used in this application. Below are the details for the same.

- **NYC TLC Trip Record Data** :  The dataset include fields capturing pick-up and drop-off dates/times, pick-up 
  and drop-off locations, trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts.
  More information about the dataset could be found here https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

**_Schema of the data:_**

==================  ==========
field_name          data_type
==================  ==========
vendor_id           string
pickup_datetime     timestamp
dropoff_datetime	timestamp
passenger_count		int
trip_distance		double
pickup_longitude	string
pickup_latitude		string
rate_code			int
store_and_fwd_flag  string 
dropoff_longitude   string
dropoff_latitude    string
payment_type        string
fare_amount         double
surcharge           double
mta_tax				double
tip_amount          double
tolls_amount        double
total_amount        double
==================  ==========
- Are intuitive

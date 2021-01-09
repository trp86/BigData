# app-ts-generate-sessionid

Application "app-ts-generate-sessionid" will help us to generate sessionid column and activity time for a timeseries data.

Given below a time series data which is a click stream of user activity.

click_ts | user_id 
--- | --- 
2018-01-01 11:00:00 | u1 
2018-01-01 12:10:00 | u1
2018-01-01 13:00:00 | u1
2018-01-01 13:25:00 | u1
2018-01-01 14:40:00 | u1
2018-01-01 15:10:00 | u1
2018-01-01 16:20:00 | u1
2018-01-01 16:50:00 | u1
2018-01-01 11:00:00 | u2
2018-01-02 11:00:00 | u2

Below are the columns description for the data:-

###### click_ts:- Contains eventtimestamp
###### user_id:- Contains user id

"app-ts-generate-sessionid" will enrich the data with sessionId.
######  Session Definition:- Session expires after inactivity of 60 minutes,because of inactivity no clickstream record will be generated.It remains active for total duration of 2 hours.

"app-ts-generate-sessionid" will output the data in the below way.

user_id | sessionId | click_time | activity_time
--- | --- | --- | --- 
u1 | c2938586ac2cf10843b5b337750f4b45 | 2018-01-01 11:00:00 | 0
u1 | c60a5f434a9356346239d9bc701bdb52 | 2018-01-01 12:10:00 | 0
u1 | c60a5f434a9356346239d9bc701bdb52 | 2018-01-01 13:00:00 | 3000
u1 | c60a5f434a9356346239d9bc701bdb52 | 2018-01-01 13:25:00 | 1500
u1 | ca897b641448ea9c8ce43fe2caf858d2 | 2018-01-01 14:40:00 | 0
u1 | ca897b641448ea9c8ce43fe2caf858d2 | 2018-01-01 15:10:00 | 1800
u1 | d457823da32b648321f881d9ae3b9d65 | 2018-01-01 16:20:00 | 0
u1 | d457823da32b648321f881d9ae3b9d65 | 2018-01-01 16:50:00 | 1800
u2 | 9ed03b43a1092f43c4f30b399c1c4df5 | 2018-01-01 11:00:00 | 0
u2 | 6597e108764db28de7fa2e086640cc11 | 2018-01-02 11:00:00 | 0

Below are the column description for the output data by app-ts-generate-sessionid:-

###### user_id:- unique id for a user
###### sessionId:- unique id for a session
###### click_time:- event timestamp
###### activity_time:- total time a user was active
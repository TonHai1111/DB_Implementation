#!/usr/bin/python

#TODO:
# 1. Pre-process data sets
# 2. Insert data into postgres database

#Query:
# select id, cab_type_id, vendor_id, trip_distance, fare_amount from trips t where t.id in (select id from trips limit 10000) order by id;
# select * from trips t where t.id in (select id from trips limit 10000) order by id;
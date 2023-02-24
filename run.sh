#!/bin/bash

# Run local cluster
# source ~/${PROJECT_FOLDER}/spark/setup/local_cluster.sh

# Execute homework.py
MASTER_URL=spark://vm-main.asia-southeast1-a.c.dtc-de-course-375301.internal:7077
spark-submit \
    --master="${MASTER_URL}" \
    ~/pyspark/homework.py \
        --input_path="./dataset/raw/fhvhv_tripdata_2021-06.csv.gz" \
        --zones_input_path="./dataset/raw/taxi_zone_lookup.csv" \
        --output_path="./dataset/pq/fhvhv/2021/06" \
        --zones_output_path="./dataset/pq/zones/" 

# Question 1
# 23/02/24 01:49:06 INFO StandaloneAppClient$ClientEndpoint: Executor updated: app-20230224014906-0001/0 is now RUNNING
# 23/02/24 01:49:06 INFO StandaloneSchedulerBackend: SchedulerBackend is ready for scheduling beginning after reached minRegisteredResourcesRatio: 0.0
# 3.3.2
# 23/02/24 01:49:07 INFO SparkContext: Invoking stop() from shutdown hook

# Question 2
# ls -lh ~/pyspark/dataset/pq/fhvhv/2021/06
# total 284M
# -rw-r--r-- 1 irfanfadh43 irfanfadh43   0 Feb 24 03:10 _SUCCESS

# Question 3
# 23/02/24 02:23:34 INFO CodeGenerator: Code generated in 45.364664 ms
# +-------------------+-----------+
# |          start_day|total_trips|
# +-------------------+-----------+
# |2021-06-15 00:00:00|     450872|
# +-------------------+-----------+

# Question 4
# 23/02/24 03:28:29 INFO CodeGenerator: Code generated in 20.6293 ms
# +-----------------+
# |max_trip_duration|
# +-----------------+
# |66.87888888888666|
# +-----------------+

# Question 6
# 23/02/24 03:46:48 INFO CodeGenerator: Code generated in 28.5334 ms
# +-------------------+---------------------+
# |               Zone|total_pickup_per_zone|
# +-------------------+---------------------+
# |Crown Heights North|               231279|
# |       East Village|               221244|
# |        JFK Airport|               188867|
# |     Bushwick South|               187929|
# |      East New York|               186780|
# +-------------------+---------------------+

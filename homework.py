import argparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import functions as F

# Add Args
parser = argparse.ArgumentParser()
parser.add_argument('--input_path', required=True)    # "dataset/raw/fhvhv_tripdata_2021-06.csv.gz"
parser.add_argument('--zones_input_path', required=True)    # "dataset/raw/taxi_zone_lookup.csv"
parser.add_argument('--output_path', required=True)   # "dataset/pq/fhvhv/2021/06/"
parser.add_argument('--zones_output_path', required=True)   # "dataset/pq/zones/"

args = parser.parse_args()
input_path = args.input_path 
zones_input_path = args.zones_path 
output_path = args.output_path 
zones_output_path = args.zones_output_path 

# Create spark session
spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

# HVFHV Taxi Tripdata
hvfhv_schema = types.StructType([
    types.StructField('dispatching_base_num', types.StringType(), True), 
    types.StructField('pickup_datetime', types.TimestampType(), True), 
    types.StructField('dropoff_datetime', types.TimestampType(), True), 
    types.StructField('PULocationID', types.IntegerType(), True), 
    types.StructField('DOLocationID', types.IntegerType(), True), 
    types.StructField('SR_Flag', types.IntegerType(), True),
    types.StructField('Affiliated_base_number', types.StringType(), True),
])
# Read CSV file for HVFHV June 2021
df = spark.read \
    .option("header", "true") \
    .schema(hvfhv_schema) \
    .csv(input_path)
df = df.repartition(12)
df.write.parquet(output_path, mode="overwrite")

# Taxi Zones
zones_schema = types.StructType([
    types.StructField("LocationID", types.IntegerType(), True),
    types.StructField("Borough", types.StringType(), True),
    types.StructField("Zone", types.StringType(), True),
    types.StructField("service_zone", types.StringType(), True),
]) 
# Read CSV file for Taxi Zones
df_zones = spark.read \
    .option("header", "true") \
    .schema(zones_schema) \
    .csv(zones_input_path) 
df_zones.write.parquet(zones_output_path, mode="overwrite")

# Read parquet file
df_hvfhv = spark.read.parquet(output_path)
df_zones = spark.read.parquet(zones_output_path)

# Set as sql table
df_hvfhv.createOrReplaceTempView('hvfhv_tripdata')

# Count of taxi trips on June 15
df_result_q3 = spark.sql("""
SELECT
    date_trunc('day', pickup_datetime) AS start_day
    ,COUNT(1) AS total_trips
FROM
    hvfhv_tripdata
WHERE
    date_trunc('day', pickup_datetime) == '2021-06-15'
GROUP BY
    start_day
""")
# df_result_q3.show()

# Longest trip for each day
# https://stackoverflow.com/questions/43406887/spark-dataframe-how-to-add-a-index-column-aka-distributed-data-index
from pyspark.sql.functions import desc, row_number, monotonically_increasing_id
from pyspark.sql.window import Window

df_hvfhv_with_id = df_hvfhv \
    .withColumn(
        'trip_id', 
        row_number().over(Window.orderBy(monotonically_increasing_id()))
    )

df_hvfhv_with_id.createOrReplaceTempView('hvfhv_tripdata_with_id')

df_result_q4 = spark.sql("""
WITH tmp_trip_duration AS (
    SELECT 
        trip_id
        ,pickup_datetime
        ,dropoff_datetime
        ,EXTRACT(day FROM (dropoff_datetime - pickup_datetime)) AS days
        ,EXTRACT(hour FROM (dropoff_datetime - pickup_datetime)) AS hours
        ,EXTRACT(minute FROM (dropoff_datetime - pickup_datetime)) AS minutes
        ,EXTRACT(second FROM (dropoff_datetime - pickup_datetime)) AS seconds
    FROM 
        hvfhv_tripdata_with_id 
)
SELECT 
    MAX(duration_in_hours) AS max_trip_duration
FROM (
    SELECT
        (days * 24) + hours + (minutes / 60) + (seconds / 3600) AS duration_in_hours
    FROM
        tmp_trip_duration
)
""")
df_result_q4.show()

# Most frequent pickup location zone
df_join = df_hvfhv.join(df_zones, df_hvfhv.PULocationID == df_zones.LocationID)

df_join.createOrReplaceTempView('tripdata')

df_result_q6 = spark.sql("""
SELECT 
    Zone
    ,COUNT(Zone) AS total_pickup_per_zone
FROM
    tripdata
GROUP BY
    Zone
ORDER BY
    total_pickup_per_zone DESC
LIMIT
    5
""")
df_result_q6.show()
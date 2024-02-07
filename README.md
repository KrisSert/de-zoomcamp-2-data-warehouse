THE datapipeline was build with mage to load the data to GCS bucket: pipeline name: "green_taxi_etl"

-- SETUP
-- create the materialized table based on external table: (external table was created via bigquery studio browser ui)
CREATE OR REPLACE TABLE `de-zoomcamp-411619.green_taxi_data.green_taxi_data`
AS SELECT * FROM `de-zoomcamp-411619.green_taxi_data.green_taxi_data_external`;

-- Q1.
-- count all records loaded in the dataset
SELECT count(*) FROM `de-zoomcamp-411619.green_taxi_data.green_taxi_data`;
-- result: 840402

-- Q2.
-- count the distinct number of PULocationIDs for the entire dataset on both the tables.
SELECT count(distinct pulocation_id)
FROM `de-zoomcamp-411619.green_taxi_data.green_taxi_data`;
-- result: 258, Bytes processed 6.41 MB, estimated: 6.41 MB
SELECT count(distinct pulocation_id)
FROM `de-zoomcamp-411619.green_taxi_data.green_taxi_data_external`;
-- restult: 258, Bytes processed 6.41 MB, estimated: 0 MB

-- Q3.
-- How many records have a fare_amount of 0?
select count(*) FROM `de-zoomcamp-411619.green_taxi_data.green_taxi_data` WHERE fare_amount=0;
-- result: 1622
-- double check: 
select fare_amount, count(*) as cnt 
FROM  `de-zoomcamp-411619.green_taxi_data.green_taxi_data` 
WHERE fare_amount<1 and fare_amount>-1 
group by fare_amount order by fare_amount asc;

-- Q4.
-- partition by lpep_pickup_datetime, cluster on pulocation_id
CREATE OR REPLACE TABLE `de-zoomcamp-411619.green_taxi_data.green_taxi_data_part_clust`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY pulocation_id
AS SELECT * FROM `de-zoomcamp-411619.green_taxi_data.green_taxi_data`;

-- Q5.
SELECT DISTINCT pulocation_id
FROM `de-zoomcamp-411619.green_taxi_data.green_taxi_data`
WHERE lpep_pickup_datetime BETWEEN TIMESTAMP('2022-06-01 00:00:00') AND TIMESTAMP('2022-06-30 23:59:59');
-- 12.82 MB
SELECT DISTINCT pulocation_id
FROM `de-zoomcamp-411619.green_taxi_data.green_taxi_data_part_clust`
WHERE lpep_pickup_datetime BETWEEN TIMESTAMP('2022-06-01 00:00:00') AND TIMESTAMP('2022-06-30 23:59:59');
-- 1.12 MB

-- Q6.
--gcpbucket

-- Q7.
--false. 
-- For small tables, the benefits of clustering might be negligible, and the overhead of clustering might not be justified.
-- If your data is infrequently queried or if your queries don't involve filtering or grouping, the impact of clustering might be minimal.
-- If your queries are dynamic and don't consistently filter or group by the same columns, clustering might not provide significant benefits.

-- Q8.
SELECT *
FROM `de-zoomcamp-411619.green_taxi_data.green_taxi_data`;
-- 114.63 bytes, because it will read the whole table with all of its columns
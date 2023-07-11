-- This archive describes some requests to query in this BigQuery Data Engineering Project.

-- SELECT VendorID, SUM(fare_amount)
FROM uber-proyectanalytics.uber_data_enginering.fact_table
GROUP BY VendorID;


SELECT b.payment_type_name, SUM(a.tip_amount) as sum_of_tip
FROM uber-proyectanalytics.uber_data_enginering.fact_table a
JOIN uber-proyectanalytics.uber_data_enginering.payment_type_dim b
ON a.payment_type_id = b.payment_type_id
GROUP BY b.payment_type_name
ORDER BY sum_of_tip DESC

-- Find the 10 pickup locations

SELECT pickup_location_id, count(pickup_location_id) as NumberOfTimes
FROM uber-proyectanalytics.uber_data_enginering.pickup_location_dim
GROUP BY pickup_location_id
ORDER BY NumberOfTimes DESC


-- Find the total number of unique trips

SELECT COUNT(DISTINCT(trip_distance))
FROM uber-proyectanalytics.uber_data_enginering.tbl_analytics


-- Find the average fare amount by payment type

SELECT AVG(fare_amount) AS AVGAmount, payment_type_name AS PaymentType
FROM uber-proyectanalytics.uber_data_enginering.tbl_analytics
GROUP BY paymentType
ORDER BY AVGAmount DESC

-- Creating the data Analysis table

CREATE OR REPLACE TABLE uber-proyectanalytics.uber_data_enginering.tbl_analytics AS (
SELECT f.VendorID, d.tpep_pickup_datetime, d.tpep_dropoff_datetime, p.passenger_count, t.trip_distance, r.rate_code_name, pick.pickup_latitude, pick.pickup_longitude, drp.dropoff_latitude, drp.dropoff_longitude, pay.payment_type_name, f.extra, f.mta_tax, f.tip_amount, f.tolls_amount, f.improvement_surcharge, f.total_amount

FROM uber-proyectanalytics.uber_data_enginering.fact_table f
JOIN uber-proyectanalytics.uber_data_enginering.datetime_dim d
ON f.datetime_id = d.datetime_id
JOIN uber-proyectanalytics.uber_data_enginering.passenger_count_dim p
ON p.passenger_count_id = f.passenger_count_id
JOIN uber-proyectanalytics.uber_data_enginering.trip_distance_dim t
ON t.trip_distance_id = f.trip_distance_id
JOIN uber-proyectanalytics.uber_data_enginering.rate_code_dim r
ON r.rate_code_id = f.rate_code_id
JOIN uber-proyectanalytics.uber_data_enginering.pickup_location_dim pick 
ON pick.pickup_location_id = f.pickup_location_id
JOIN uber-proyectanalytics.uber_data_enginering.dropoff_location_dim drp 
ON drp.dropoff_location_id = f.dropoff_location_id
JOIN uber-proyectanalytics.uber_data_enginering.payment_type_dim pay 
ON pay.payment_type_id = f.payment_type_id)
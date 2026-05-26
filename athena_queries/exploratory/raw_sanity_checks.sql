-- raw_sanity_checks.sql
-- Quick read-only checks to confirm the raw table is queryable after the
-- crawler runs. Run these in Athena (workgroup + output location required).
--
-- COST NOTE: Athena bills $5 per TB scanned. The raw month is ~48 MB, so each
-- of these scans well under a cent. Still, prefer LIMIT and column projection
-- out of habit -- the discipline matters once tables hit terabytes.

-- 1. Does the table resolve and return rows?
SELECT *
FROM nyc_taxi_raw.raw_yellow_tripdata
LIMIT 10;

-- 2. Row count for the loaded month (expect ~2.96M for 2024-01).
SELECT count(*) AS trip_count
FROM nyc_taxi_raw.raw_yellow_tripdata;

-- 3. Date range sanity -- pickups should fall inside Jan 2024
--    (TLC files always contain a few stray out-of-range timestamps).
SELECT
  min(tpep_pickup_datetime) AS earliest_pickup,
  max(tpep_pickup_datetime) AS latest_pickup
FROM nyc_taxi_raw.raw_yellow_tripdata;

-- 4. Cheap data-quality smell test: how many rows have impossible values?
SELECT
  count_if(passenger_count = 0)        AS zero_passenger_trips,
  count_if(trip_distance <= 0)         AS nonpositive_distance,
  count_if(fare_amount < 0)            AS negative_fare,
  count_if(total_amount < 0)           AS negative_total
FROM nyc_taxi_raw.raw_yellow_tripdata;

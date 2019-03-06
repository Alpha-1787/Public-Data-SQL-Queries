# grab for hire vehicle trip count and avg duration by month for each borough/zone
WITH fhv_trips AS 
	(SELECT
		"FHV" AS trip_type,
		DATETIME_TRUNC(DATETIME(TIMESTAMP(tfp.pickup_datetime, 'US/Eastern')), MONTH) AS date_month,
		tfp.borough AS pickup_borough,
		tfp.zone AS pickup_zone,
		tfp.dropoff_borough,
		tfp.dropoff_zone,
		COUNT(*) AS trip_count,
		ROUND(AVG(DATETIME_DIFF(tfp.dropoff_datetime, tfp.pickup_datetime, MINUTE)), 2) AS avg_trip_duration_min
	FROM
		`bigquery-public-data.new_york_taxi_trips.tlc_fhv_trips_2017` tfp
	WHERE 
		tfp.dropoff_borough != ''
		AND tfp.dropoff_zone != ''
	GROUP BY
		trip_type,
		date_month,
		pickup_borough,
		pickup_zone,
		tfp.dropoff_borough,
		tfp.dropoff_zone
	HAVING
		trip_count > 100),
# grab green taxi trip count and avg duration by month for each borough/zone
green_trips AS
	(SELECT
		"Green Taxi" AS trip_type,
		DATETIME_TRUNC(DATETIME(TIMESTAMP(tgt.pickup_datetime, 'US/Eastern')), MONTH) AS date_month,
		zp.borough AS pickup_borough,
		zp.zone_name AS pickup_zone,
		zd.borough AS dropoff_borough,
		zd.zone_name AS dropoff_zone,
		COUNT(*) AS trip_count,
		ROUND(AVG(DATETIME_DIFF(tgt.dropoff_datetime, tgt.pickup_datetime, MINUTE)), 2) AS avg_trip_duration_min
	FROM
		`bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2017` tgt
	LEFT JOIN 
		`bigquery-public-data.new_york_taxi_trips.taxi_zone_geom` zp
		ON 
			zp.zone_id = tgt.pickup_location_id
	LEFT JOIN 
		`bigquery-public-data.new_york_taxi_trips.taxi_zone_geom` zd 
		ON 
			zd.zone_id = tgt.dropoff_location_id
	WHERE 
		zp.borough != ''
		AND zp.zone_name != ''
	GROUP BY
		trip_type,
		date_month,
		pickup_borough,
		pickup_zone,
		dropoff_borough,
		dropoff_zone
	HAVING
		trip_count > 100),
# grab yellow taxi trip count and avg duration by month for each borough/zone
yellow_trips AS 
	(SELECT
		"Yelow Taxi" AS trip_type,
		DATETIME_TRUNC(DATETIME(TIMESTAMP(tyt.pickup_datetime, 'US/Eastern')), MONTH) AS date_month,
		zp.borough AS pickup_borough,
		zp.zone_name AS pickup_zone,
		zd.borough AS dropoff_borough,
		zd.zone_name AS dropoff_zone,
		COUNT(*) AS trip_count,
		ROUND(AVG(DATETIME_DIFF(tyt.dropoff_datetime, tyt.pickup_datetime, MINUTE)), 2) AS avg_trip_duration_min
	FROM
		`bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2017` tyt
	LEFT JOIN 
		`bigquery-public-data.new_york_taxi_trips.taxi_zone_geom` zp
		ON 
			zp.zone_id = tyt.pickup_location_id
	LEFT JOIN 
		`bigquery-public-data.new_york_taxi_trips.taxi_zone_geom` zd 
		ON 
			zd.zone_id = tyt.dropoff_location_id
	WHERE 
		zp.borough != ''
		AND zp.zone_name != ''
	GROUP BY
		trip_type,
		date_month,
		pickup_borough,
		pickup_zone,
		dropoff_borough,
		dropoff_zone
	HAVING
		trip_count > 100)
# combine the results onto one and sort it
SELECT 
  *
FROM 
	(SELECT * FROM fhv_trips UNION ALL
	SELECT * FROM green_trips UNION ALL 
	SELECT * FROM yellow_trips) tt
ORDER BY 
	tt.date_month,
	tt.trip_type,
	tt.pickup_borough,
	tt.pickup_zone,
	tt.dropoff_borough,
	tt.dropoff_zone

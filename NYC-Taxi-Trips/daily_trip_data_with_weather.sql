/* daily ride data + weather */

# daily weather data
WITH daily_weather AS
	(SELECT	
	    DATETIME_TRUNC(DATETIME(TIMESTAMP(CONCAT(g.year,'-',g.mo,'-',g.da), 'US/Eastern')), DAY) AS date,
	    g.temp AS avg_temp,
	    g.max AS max_temp,
	    g.min AS min_temp,
	    g.prcp AS percip_inch,
	    CASE
	    	WHEN g.sndp = 999.9 THEN 0
	    	ELSE g.sndp END AS snow_depth,
	    g.fog,
	    g.rain_drizzle,
	    g.snow_ice_pellets,
	    g.hail,
	    g.thunder,
	    g.tornado_funnel_cloud, 
	    CAST(g.wdsp AS float64) AS wind_speed,
	    g.dewp,
	    g.visib
	FROM
		`bigquery-public-data.noaa_gsod.gsod2017` g
	WHERE
	    g.wban IN 
	      (SELECT 
	          st.wban
	      FROM 
	          `bigquery-public-data.noaa_gsod.stations` st
	      WHERE
	          st.country = 'US'
	          AND st.state = 'NY'
	          AND st.call = 'KNYC')),
# for hire trips
fhv_trips AS 
	(SELECT
		"FHV" AS trip_type,
		DATETIME_TRUNC(DATETIME(TIMESTAMP(tfp.pickup_datetime, 'US/Eastern')), DAY) AS date,
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
		date,
		pickup_borough,
		pickup_zone,
		tfp.dropoff_borough,
		tfp.dropoff_zone),
# grab green taxi trip count and avg duration by day for each borough/zone
green_trips AS
	(SELECT
		"Green Taxi" AS trip_type,
		DATETIME_TRUNC(DATETIME(TIMESTAMP(tgt.pickup_datetime, 'US/Eastern')), DAY) AS date,
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
		date,
		pickup_borough,
		pickup_zone,
		dropoff_borough,
		dropoff_zone),
# grab yellow taxi trip count and avg duration by day for each borough/zone
yellow_trips AS 
	(SELECT
		"Yelow Taxi" AS trip_type,
		DATETIME_TRUNC(DATETIME(TIMESTAMP(tyt.pickup_datetime, 'US/Eastern')), DAY) AS date,
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
		date,
		pickup_borough,
		pickup_zone,
		dropoff_borough,
		dropoff_zone)
SELECT 
	tt.*,
	dw.avg_temp,
	dw.max_temp,
	dw.min_temp,
	dw.percip_inch,
	dw.snow_depth,
	dw.fog,
	dw.rain_drizzle,
	dw.snow_ice_pellets,
	dw.hail,
	dw.thunder,
	dw.tornado_funnel_cloud,
	dw.wind_speed,
	dw.dewp,
	dw.visib
FROM 
	(SELECT * FROM fhv_trips UNION ALL
	SELECT * FROM green_trips UNION ALL 
	SELECT * FROM yellow_trips) tt
LEFT JOIN 
	daily_weather dw
	ON 
		dw.date = tt.date
WHERE
	tt.date >= '2017-01-01'
	AND tt.date < '2018-01-01'
ORDER BY 
	tt.date,
	tt.trip_type,
	tt.pickup_borough,
	tt.pickup_zone,
	tt.dropoff_borough,
	tt.dropoff_zone

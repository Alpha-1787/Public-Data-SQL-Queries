# Query for avg bike/dock availability, avg trip count, and duration for any given day, hour of the day per station
# Uses joined inline views

SELECT 
	sa.station_name,
	CASE 
    		WHEN sa.day_of_week = 1 THEN "SUNDAY"
		WHEN sa.day_of_week = 2 THEN "MONDAY"
		WHEN sa.day_of_week = 3 THEN "TUESDAY"
		WHEN sa.day_of_week = 4 THEN "WEDNESDAY"
		WHEN sa.day_of_week = 5 THEN "THURSDAY"
		WHEN sa.day_of_week = 6 THEN "FRIDAY"
		ELSE "SATURDAY" END AS day_of_week,
	sa.hour_of_day,
	sa.dock_count,
	IFNULL(sa.avg_bikes_available, 0) AS avg_bikes_available,
	IFNULL(sa.avg_docks_available, 0) AS avg_docks_available,
	IFNULL(tog.avg_trip_count, 0) AS avg_trip_count,
	IFNULL(tog.avg_trip_duration_min, 0) AS avg_trip_duration_min
FROM 
	(SELECT 
		bst.name AS station_name,
		bst.station_id,
		EXTRACT(DAYOFWEEK FROM bs.time AT TIME ZONE "America/Los_Angeles") AS day_of_week,  
		EXTRACT(HOUR FROM bs.time AT TIME ZONE "America/Los_Angeles") AS hour_of_day,
		AVG(bst.dockcount) AS dock_count,
		ROUND(AVG(bs.bikes_available), 0) AS avg_bikes_available,
		ROUND(AVG(bs.docks_available), 0) AS avg_docks_available
	FROM
		`bigquery-public-data.san_francisco.bikeshare_stations` bst 
	LEFT JOIN 
		`bigquery-public-data.san_francisco.bikeshare_status` bs
		ON 
			bs.station_id = bst.station_id
	GROUP BY
		station_name,
    bst.station_id,
		day_of_week,
		hour_of_day) sa
LEFT JOIN 
	(SELECT 
		bt.start_station_name,
		bt.start_station_id,
		EXTRACT(DAYOFWEEK FROM bt.start_date AT TIME ZONE "America/Los_Angeles") AS day_of_week,  
		EXTRACT(HOUR FROM bt.start_date AT TIME ZONE "America/Los_Angeles") AS hour_of_day,
		ROUND(COUNT(*)/COUNT(DISTINCT(EXTRACT(DATE from bt.start_date AT TIME ZONE "America/Los_Angeles"))),
          	  2) AS avg_trip_count,
		ROUND(AVG(bt.duration_sec/60)) AS avg_trip_duration_min
	FROM 
		`bigquery-public-data.san_francisco.bikeshare_trips` bt
	GROUP BY 
		bt.start_station_name,
		bt.start_station_id,
		day_of_week,
		hour_of_day) tog 
	ON 
		tog.start_station_id = sa.station_id
		AND tog.day_of_week = sa.day_of_week
		AND tog.hour_of_day = sa.hour_of_day
WHERE 
	sa.day_of_week IN (2, 3, 4)
ORDER BY 
	sa.station_name,
	sa.day_of_week,
	sa.hour_of_day

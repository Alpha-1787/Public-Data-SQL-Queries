# getting top 5 origin station by trip count for the top 5 destination station by trip count with inline view and subquery
# will return the same results as top_5_origin_for_top_5_destination_cte.sql

SELECT
	tdo.end_station_name,
	tdo.start_station_name,
	tdo.trip_count_rank,
	tdo.trip_count,
	tdo.avg_trip_duration_min 
FROM 
	(SELECT
		bt.end_station_name,
		bt.start_station_name,
		DENSE_RANK() OVER (PARTITION BY bt.end_station_name ORDER BY COUNT(*) DESC, bt.end_station_name) AS trip_count_rank,
		COUNT(*) AS trip_count,
		ROUND(AVG(bt.duration_sec)/60, 2) AS avg_trip_duration_min
	FROM
		`bigquery-public-data.san_francisco.bikeshare_trips` bt
	GROUP BY
		bt.start_station_name,
		bt.end_station_name) tdo
WHERE 
	tdo.trip_count_rank IN (1, 2, 3, 4, 5)
	AND tdo.end_station_name IN 
		(SELECT 
			td.end_station_name
		FROM 
			(SELECT
				bt.end_station_name,
				DENSE_RANK() OVER (ORDER BY COUNT(*) DESC, bt.end_station_name) AS trip_count_rank,
				COUNT(*) AS trip_count,
				ROUND(AVG(bt.duration_sec)/60, 2) AS avg_trip_duration_min
			FROM
				`bigquery-public-data.san_francisco.bikeshare_trips` bt
			GROUP BY
				bt.end_station_name) td
		WHERE
			td.trip_count_rank IN (1, 2, 3, 4, 5))

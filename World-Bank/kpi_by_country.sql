# grabs each country's most recent KPIs and the corresponding the global median to serve as comparison.

# used to narrow down the KPIs to 'Education: Outcomes' topic
WITH vars AS 
	(SELECT 
		'Education: Outcomes' AS topic),
global_summary AS
	(SELECT DISTINCT
		ss.topic,
		hnp.indicator_name,
		hnp.indicator_code,
		hnp.year,
		PERCENTILE_DISC(hnp.value, 0.5) OVER(PARTITION BY hnp.indicator_code, hnp.year) AS global_median_value
	FROM 
		`bigquery-public-data.world_bank_health_population.health_nutrition_population` hnp
	LEFT JOIN
		`bigquery-public-data.world_bank_health_population.series_summary` ss
	ON 
		ss.series_code = hnp.indicator_code
	WHERE
		ss.topic = (SELECT topic FROM vars)),
country_latest AS 
	(SELECT 
		hnp.country_name,
		hnp.year,
		ss.topic,
    	hnp.indicator_code,
		hnp.indicator_name,
		hnp.value AS country_value,
		ss.unit_of_measure
	FROM 
		`bigquery-public-data.world_bank_health_population.health_nutrition_population` hnp
	LEFT JOIN
		`bigquery-public-data.world_bank_health_population.series_summary` ss
		ON 
			ss.series_code = hnp.indicator_code
	WHERE
		hnp.year IN
			(SELECT -- correlated subquery to grab the latest value for each indicator for each country
	      		MAX(year)
	      	FROM
	      		`bigquery-public-data.world_bank_health_population.health_nutrition_population`
	     	WHERE 
	     		country_name = hnp.country_name
	     		AND indicator_name = hnp.indicator_name
	     		AND indicator_code = hnp.indicator_code
	     		AND country_code = hnp.country_code)
		AND ss.topic = (SELECT topic FROM vars))
SELECT 
	cl.country_name,
	cl.year,
	cl.topic,
	cl.indicator_name,
	cl.country_value,
	ws.global_median_value,
	cl.unit_of_measure
FROM 
	country_latest cl 
LEFT JOIN
	global_summary gs
	ON
		gs.topic = cl.topic
		AND gs.indicator_code = cl.indicator_code
		AND gs.year = cl.year
ORDER BY 
	cl.country_name,
	cl.topic,
	cl.indicator_name

/* daily ride data + weather */


# daily weather data
SELECT
	g.year,
 	g.mo,
 	g.da,
 	AVG(g.temp) AS avg_temperature
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
          AND st.call = 'KNYC')
GROUP BY
    g.year,
    g.mo,
    g.da
ORDER BY 
    g.mo,
    g.da

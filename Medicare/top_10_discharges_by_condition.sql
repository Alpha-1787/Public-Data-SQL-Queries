SELECT 
	drg_definition,
	total_discharges
FROM 
	(SELECT 
		ic.drg_definition,
		SUM(ic.total_discharges) AS total_discharges
	FROM 
	  `bigquery-public-data.cms_medicare.inpatient_charges_2015` ic
	GROUP BY 
		ic.drg_definition
  ORDER BY 
    SUM(ic.total_discharges) DESC) discharges_by_drg
LIMIT 10

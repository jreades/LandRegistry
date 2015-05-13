SET search_path TO landreg, os, public;

DROP MATERIALIZED VIEW IF EXISTS landreg.pd2_annual_fct; 
CREATE MATERIALIZED VIEW pd2_annual_fct AS 
SELECT 
	pd2,
	extract(year FROM completion_dt) AS completion_year,  
	count(price_int) as n,
	min(price_int) as min_price_int, 
	max(price_int) as max_price_int,
	sum(price_int) as sum_price_int, 
	avg(price_int) as avg_price_int, 
	quantile(price_int, 0.25) as lower_quartile, 
	quantile(price_int, 0.5) as median_price_int,
	quantile(price_int, 0.75) as upper_quartile, 
	stddev_samp(price_int) as sd_price_int, 
	var_samp(price_int) as var_price_int
FROM 
	price_paid_fct AS pf, 
	pc_mapping_dim AS pmd 
WHERE 
	pmd.pc=pf.pc 
GROUP BY 
	pd2, 
	property_type_cd, 
	completion_year 
ORDER BY 
	pd2, 
	property_type_cd, 
	completion_year
;

DROP MATERIALIZED VIEW IF EXISTS landreg.pd1_annual_fct; 
CREATE MATERIALIZED VIEW pd1_annual_fct AS 
SELECT 
	pd1,
	extract(year FROM completion_dt) AS completion_year,  
	count(price_int) as n,
	min(price_int) as min_price_int, 
	max(price_int) as max_price_int,
	sum(price_int) as sum_price_int, 
	avg(price_int) as avg_price_int, 
	quantile(price_int, 0.25) as lower_quartile, 
	quantile(price_int, 0.5) as median_price_int,
	quantile(price_int, 0.75) as upper_quartile, 
	stddev_samp(price_int) as sd_price_int, 
	var_samp(price_int) as var_price_int
FROM 
	price_paid_fct AS pf, 
	pc_mapping_dim AS pmd 
WHERE 
	pmd.pc=pf.pc 
GROUP BY 
	pd1, 
	property_type_cd, 
	completion_year 
ORDER BY 
	pd1, 
	property_type_cd, 
	completion_year
;

DROP MATERIALIZED VIEW IF EXISTS landreg.pd_annual_fct; 
CREATE MATERIALIZED VIEW pd_annual_fct AS 
SELECT 
	pd,
	extract(year FROM completion_dt) AS completion_year,  
	count(price_int) as n,
	min(price_int) as min_price_int, 
	max(price_int) as max_price_int,
	sum(price_int) as sum_price_int, 
	avg(price_int) as avg_price_int, 
	quantile(price_int, 0.25) as lower_quartile, 
	quantile(price_int, 0.5) as median_price_int,
	quantile(price_int, 0.75) as upper_quartile, 
	stddev_samp(price_int) as sd_price_int, 
	var_samp(price_int) as var_price_int
FROM 
	price_paid_fct AS pf, 
	pc_mapping_dim AS pmd 
WHERE 
	pmd.pc=pf.pc 
GROUP BY 
	pd, 
	property_type_cd, 
	completion_year 
ORDER BY 
	pd, 
	property_type_cd, 
	completion_year
;

DROP MATERIALIZED VIEW IF EXISTS landreg.pda_annual_fct; 
CREATE MATERIALIZED VIEW pda_annual_fct AS 
SELECT 
	pda,
	extract(year FROM completion_dt) AS completion_year,  
	count(price_int) as n,
	min(price_int) as min_price_int, 
	max(price_int) as max_price_int,
	sum(price_int) as sum_price_int, 
	avg(price_int) as avg_price_int, 
	quantile(price_int, 0.25) as lower_quartile, 
	quantile(price_int, 0.5) as median_price_int,
	quantile(price_int, 0.75) as upper_quartile, 
	stddev_samp(price_int) as sd_price_int, 
	var_samp(price_int) as var_price_int
FROM 
	price_paid_fct AS pf, 
	pc_mapping_dim AS pmd 
WHERE 
	pmd.pc=pf.pc 
GROUP BY 
	pda, 
	property_type_cd, 
	completion_year 
ORDER BY 
	pda, 
	property_type_cd, 
	completion_year
;
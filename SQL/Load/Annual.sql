SET search_path TO landreg, os, public;

-- Postcode
-- DROP MATERIALIZED VIEW IF EXISTS landreg.pc_annual_fct; 
-- CREATE MATERIALIZED VIEW pc_annual_fct AS 
-- SELECT 
-- 	pc, 
-- 	property_type_cd, 
-- 	extract(year FROM completion_dt) AS completion_year,  
-- 	count(price_int) as n,
-- 	min(price_int) as min_price_int, 
-- 	max(price_int) as max_price_int,
-- 	sum(price_int) as sum_price_int, 
-- 	avg(price_int) as avg_price_int, 
-- 	quantile(price_int, 0.25) as lower_quartile, 
-- 	quantile(price_int, 0.5) as median_price_int,
-- 	quantile(price_int, 0.75) as upper_quartile, 
-- 	stddev_samp(price_int) as sd_price_int, 
-- 	var_samp(price_int) as var_price_int
-- FROM 
-- 	price_paid_fct AS pf
-- GROUP BY 
-- 	pc, 
-- 	property_type_cd, 
-- 	completion_year 
-- ORDER BY 
-- 	pc, 
-- 	property_type_cd, 
-- 	completion_year
-- ;

-- Local Authority
DROP MATERIALIZED VIEW IF EXISTS landreg.la_annual_fct; 
CREATE MATERIALIZED VIEW la_annual_fct AS 
SELECT 
	initcap(authority_nm) as authority_nm, 
	property_type_cd, 
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
	price_paid_fct AS pf
GROUP BY 
	authority_nm, 
	property_type_cd, 
	completion_year 
ORDER BY 
	authority_nm, 
	property_type_cd, 
	completion_year
;

DROP MATERIALIZED VIEW IF EXISTS landreg.pd2_annual_fct; 
CREATE MATERIALIZED VIEW pd2_annual_fct AS 
SELECT 
	pd2, 
	property_type_cd, 
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
	property_type_cd, 
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
	property_type_cd, 
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

DROP MATERIALIZED VIEW IF EXISTS landreg.pca_annual_fct; 
CREATE MATERIALIZED VIEW pca_annual_fct AS 
SELECT 
	pca, 
	property_type_cd, 
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
	pca, 
	property_type_cd, 
	completion_year 
ORDER BY 
	pca, 
	property_type_cd, 
	completion_year
;

-- Region
DROP MATERIALIZED VIEW IF EXISTS landreg.region_annual_fct; 
CREATE MATERIALIZED VIEW region_annual_fct AS 
SELECT  
	region, 
	property_type_cd, 
	extract(year from completion_dt) as completion_year,   
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
from 
	price_paid_fct ppf, 
	pc_mapping_dim pmd 
where 
	ppf.pc=pmd.pc
group by 
	region, 
	property_type_cd, 
	completion_year
order by 
	region, 
	property_type_cd, 
	completion_year;

-- 
-- Now create the tables where we don't 
-- worry about the type of property
-- 

-- Postcode
-- DROP MATERIALIZED VIEW IF EXISTS landreg.pc_annual_change_fct; 
-- CREATE MATERIALIZED VIEW pc_annual_change_fct AS 
-- SELECT 
-- 	pc,
-- 	extract(year FROM completion_dt) AS completion_year,  
-- 	count(price_int) as n,
-- 	min(price_int) as min_price_int, 
-- 	max(price_int) as max_price_int,
-- 	sum(price_int) as sum_price_int, 
-- 	avg(price_int) as avg_price_int, 
-- 	quantile(price_int, 0.25) as lower_quartile, 
-- 	quantile(price_int, 0.5) as median_price_int,
-- 	quantile(price_int, 0.75) as upper_quartile, 
-- 	stddev_samp(price_int) as sd_price_int, 
-- 	var_samp(price_int) as var_price_int
-- FROM 
-- 	price_paid_fct AS pf
-- GROUP BY 
-- 	pc,  
-- 	completion_year 
-- ORDER BY 
-- 	pc,  
-- 	completion_year
-- ;

-- Local Authority
DROP MATERIALIZED VIEW IF EXISTS landreg.la_annual_change_fct; 
CREATE MATERIALIZED VIEW la_annual_change_fct AS 
SELECT 
	initcap(authority_nm) as authority_nm,
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
	price_paid_fct AS pf
GROUP BY 
	authority_nm,  
	completion_year 
ORDER BY 
	authority_nm,  
	completion_year
;

DROP MATERIALIZED VIEW IF EXISTS landreg.pd2_annual_change_fct; 
CREATE MATERIALIZED VIEW pd2_annual_change_fct AS 
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
	completion_year 
ORDER BY 
	pd2,  
	completion_year
;

DROP MATERIALIZED VIEW IF EXISTS landreg.pd1_annual_change_fct; 
CREATE MATERIALIZED VIEW pd1_annual_change_fct AS 
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
	completion_year 
ORDER BY 
	pd1, 
	completion_year
;

DROP MATERIALIZED VIEW IF EXISTS landreg.pd_annual_change_fct; 
CREATE MATERIALIZED VIEW pd_annual_change_fct AS 
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
	completion_year 
ORDER BY 
	pd, 
	completion_year
;

DROP MATERIALIZED VIEW IF EXISTS landreg.pca_annual_change_fct; 
CREATE MATERIALIZED VIEW pca_annual_change_fct AS 
SELECT 
	pca,
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
	pca, 
	completion_year 
ORDER BY 
	pca, 
	completion_year
;

-- Region
DROP MATERIALIZED VIEW IF EXISTS landreg.region_annual_change_fct; 
CREATE MATERIALIZED VIEW region_annual_change_fct AS 
SELECT  
	region,
	extract(year from completion_dt) as completion_year,  
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
from 
	price_paid_fct ppf, 
	pc_mapping_dim pmd 
where 
	ppf.pc=pmd.pc
group by 
	region, 
	completion_year
order by 
	region, 
	completion_year
;

-- 
-- And, finally, the tables where we put it 
-- all together into one column per year so 
-- it's easy to calculate change in one go.
-- 

SELECT 
	p.gid, 
	p.pca, 
	p.geom, 
	pf1.avg_price_int as avg1995, 
	pf1.median_price_int as med1995, 
	pf2.avg_price_int as avg1996, 
	pf2.median_price_int as med1996, 
	pf3.avg_price_int as avg1997, 
	pf3.median_price_int as med1997, 
	pf4.avg_price_int as avg1998, 
	pf4.median_price_int as med1998, 
	pf5.avg_price_int as avg1999, 
	pf5.median_price_int as med1999, 
	pf6.avg_price_int as avg2000, 
	pf6.median_price_int as med2000, 
	pf7.avg_price_int as avg2001, 
	pf7.median_price_int as med2001, 
	pf8.avg_price_int as avg2002, 
	pf8.median_price_int as med2002, 
	pf9.avg_price_int as avg2003, 
	pf9.median_price_int as med2003, 
	pf10.avg_price_int as avg2004, 
	pf10.median_price_int as med2004, 
	pf11.avg_price_int as avg2005, 
	pf11.median_price_int as med2005, 
	pf12.avg_price_int as avg2006, 
	pf12.median_price_int as med2006, 
	pf13.avg_price_int as avg2007, 
	pf13.median_price_int as med2007, 
	pf14.avg_price_int as avg2008, 
	pf14.median_price_int as med2008, 
	pf15.avg_price_int as avg2009, 
	pf15.median_price_int as med2009, 
	pf16.avg_price_int as avg2010, 
	pf16.median_price_int as med2010, 
	pf17.avg_price_int as avg2011, 
	pf17.median_price_int as med2011, 
	pf18.avg_price_int as avg2012, 
	pf18.median_price_int as med2012, 
	pf19.avg_price_int as avg2013, 
	pf19.median_price_int as med2013, 
	pf20.avg_price_int as avg2014, 
	pf20.median_price_int as med2014
FROM 
	os.pca_spa as p 
LEFT OUTER JOIN pca_annual_change_fct pf1  ON (p.pca=pf1.pca)  WHERE pf1.completion_year=1995 
LEFT OUTER JOIN pca_annual_change_fct pf2  ON (p.pca=pf2.pca)  WHERE pf2.completion_year=1996 
LEFT OUTER JOIN pca_annual_change_fct pf3  ON (p.pca=pf3.pca)  WHERE pf3.completion_year=1997 
LEFT OUTER JOIN pca_annual_change_fct pf4  ON (p.pca=pf4.pca)  WHERE pf4.completion_year=1998 
LEFT OUTER JOIN pca_annual_change_fct pf5  ON (p.pca=pf5.pca)  WHERE pf5.completion_year=1999 
LEFT OUTER JOIN pca_annual_change_fct pf6  ON (p.pca=pf6.pca)  WHERE pf6.completion_year=2000 
LEFT OUTER JOIN pca_annual_change_fct pf7  ON (p.pca=pf7.pca)  WHERE pf7.completion_year=2001 
LEFT OUTER JOIN pca_annual_change_fct pf8  ON (p.pca=pf8.pca)  WHERE pf8.completion_year=2002 
LEFT OUTER JOIN pca_annual_change_fct pf9  ON (p.pca=pf9.pca)  WHERE pf9.completion_year=2003 
LEFT OUTER JOIN pca_annual_change_fct pf10 ON (p.pca=pf10.pca) WHERE pf10.completion_year=2004 
LEFT OUTER JOIN pca_annual_change_fct pf11 ON (p.pca=pf11.pca) WHERE pf11.completion_year=2005 
LEFT OUTER JOIN pca_annual_change_fct pf12 ON (p.pca=pf12.pca) WHERE pf12.completion_year=2006 
LEFT OUTER JOIN pca_annual_change_fct pf13 ON (p.pca=pf13.pca) WHERE pf13.completion_year=2007 
LEFT OUTER JOIN pca_annual_change_fct pf14 ON (p.pca=pf14.pca) WHERE pf14.completion_year=2008 
LEFT OUTER JOIN pca_annual_change_fct pf15 ON (p.pca=pf15.pca) WHERE pf15.completion_year=2009 
LEFT OUTER JOIN pca_annual_change_fct pf16 ON (p.pca=pf16.pca) WHERE pf16.completion_year=2010 
LEFT OUTER JOIN pca_annual_change_fct pf17 ON (p.pca=pf17.pca) WHERE pf17.completion_year=2011 
LEFT OUTER JOIN pca_annual_change_fct pf18 ON (p.pca=pf18.pca) WHERE pf18.completion_year=2012 
LEFT OUTER JOIN pca_annual_change_fct pf19 ON (p.pca=pf19.pca) WHERE pf19.completion_year=2013 
LEFT OUTER JOIN pca_annual_change_fct pf20 ON (p.pca=pf20.pca) WHERE pf20.completion_year=2014 
SET search_path = landreg, os, public;

-- PC
-- DROP MATERIALIZED VIEW IF EXISTS landreg.pc_quarterly_fct; 
-- CREATE MATERIALIZED VIEW pc_quarterly_fct AS 
-- SELECT  
-- 	pc,
-- 	property_type_cd, 
-- 	extract(year from completion_dt) as completion_year, 
-- 	ceil(extract(month from completion_dt)/3.0) as quarter, 
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
-- from 
-- 	price_paid_fct ppf
-- group by 
-- 	pc, 
-- 	property_type_cd, 
-- 	completion_year, 
-- 	quarter
-- order by 
-- 	pc, 
-- 	property_type_cd, 
-- 	completion_year, 
-- 	quarter;
-- ALTER TABLE pd_quarterly_fct ADD CONSTRAINT pdq_pkey PRIMARY KEY(pd, property_type_cd, completion_year, quarter);
-- CREATE INDEX pdq_date_idx ON pd_quarterly_fct USING btree (completion_year, quarter);
-- CREATE INDEX pdq_n_idx ON pd_quarterly_fct USING btree (n);
-- CREATE INDEX pdq_pd_idx ON pd_quarterly_fct USING btree (pd COLLATE pg_catalog."default");
-- CREATE INDEX pdq_property_idx ON pd_quarterly_fct USING btree (property_type_cd COLLATE pg_catalog."default");

-- Local Authority
DROP MATERIALIZED VIEW IF EXISTS landreg.la_quarterly_fct; 
CREATE MATERIALIZED VIEW la_quarterly_fct AS 
SELECT  
	initcap(authority_nm) as authority_nm,
	property_type_cd, 
	extract(year from completion_dt) as completion_year, 
	ceil(extract(month from completion_dt)/3.0) as quarter,  
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
	authority_nm, 
	property_type_cd, 
	completion_year, 
	quarter
order by 
	authority_nm, 
	property_type_cd, 
	completion_year, 
	quarter;


-- PD
DROP MATERIALIZED VIEW IF EXISTS landreg.pd_quarterly_fct; 
CREATE MATERIALIZED VIEW pd_quarterly_fct AS 
SELECT  
	pd,
	property_type_cd, 
	extract(year from completion_dt) as completion_year, 
	ceil(extract(month from completion_dt)/3.0) as quarter, 
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
	pd, 
	property_type_cd, 
	completion_year, 
	quarter
order by 
	pd, 
	property_type_cd, 
	completion_year, 
	quarter;
-- ALTER TABLE pd_quarterly_fct ADD CONSTRAINT pdq_pkey PRIMARY KEY(pd, property_type_cd, completion_year, quarter);
-- CREATE INDEX pdq_date_idx ON pd_quarterly_fct USING btree (completion_year, quarter);
-- CREATE INDEX pdq_n_idx ON pd_quarterly_fct USING btree (n);
-- CREATE INDEX pdq_pd_idx ON pd_quarterly_fct USING btree (pd COLLATE pg_catalog."default");
-- CREATE INDEX pdq_property_idx ON pd_quarterly_fct USING btree (property_type_cd COLLATE pg_catalog."default");

-- PD1 
DROP MATERIALIZED VIEW IF EXISTS landreg.pd1_quarterly_fct; 
CREATE MATERIALIZED VIEW pd1_quarterly_fct AS 
SELECT  
	pd1,
	property_type_cd, 
	extract(year from completion_dt) as completion_year, 
	ceil(extract(month from completion_dt)/3.0) as quarter, 
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
	pd1, 
	property_type_cd, 
	completion_year, 
	quarter
order by 
	pd1, 
	property_type_cd, 
	completion_year, 
	quarter;
-- ALTER TABLE pd1_quarterly_fct ADD CONSTRAINT pd1q_pkey PRIMARY KEY(pd1, property_type_cd, completion_year, quarter);
-- CREATE INDEX pd1q_date_idx ON pd1_quarterly_fct USING btree (completion_year, quarter);
-- CREATE INDEX pd1q_n_idx ON pd1_quarterly_fct USING btree (n);
-- CREATE INDEX pd1q_pd_idx ON pd1_quarterly_fct USING btree (pd1 COLLATE pg_catalog."default");
-- CREATE INDEX pd1q_property_idx ON pd1_quarterly_fct USING btree (property_type_cd COLLATE pg_catalog."default");

-- PD2 
DROP MATERIALIZED VIEW IF EXISTS landreg.pd2_quarterly_fct; 
CREATE MATERIALIZED VIEW pd2_quarterly_fct AS 
SELECT  
	pd2,
	property_type_cd, 
	extract(year from completion_dt) as completion_year, 
	ceil(extract(month from completion_dt)/3.0) as quarter,  
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
	pd2, 
	property_type_cd, 
	completion_year, 
	quarter
order by 
	pd2, 
	property_type_cd, 
	completion_year, 
	quarter;
-- ALTER TABLE pd2_quarterly_fct ADD CONSTRAINT pd2q_pkey PRIMARY KEY(pd2, property_type_cd, completion_year, quarter);
-- CREATE INDEX pd2q_date_idx ON pd2_quarterly_fct USING btree (completion_year, quarter);
-- CREATE INDEX pd2q_n_idx ON pd2_quarterly_fct USING btree (n);
-- CREATE INDEX pd2q_pd_idx ON pd2_quarterly_fct USING btree (pd2 COLLATE pg_catalog."default");
-- CREATE INDEX pd2q_property_idx ON pd2_quarterly_fct USING btree (property_type_cd COLLATE pg_catalog."default");

-- PC Area
DROP MATERIALIZED VIEW IF EXISTS landreg.pca_quarterly_fct; 
CREATE MATERIALIZED VIEW pca_quarterly_fct AS 
SELECT  
	pca as pca,
	property_type_cd, 
	extract(year from completion_dt) as completion_year, 
	ceil(extract(month from completion_dt)/3.0) as quarter,  
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
	pca, 
	property_type_cd, 
	completion_year, 
	quarter
order by 
	pca, 
	property_type_cd, 
	completion_year, 
	quarter;
-- ALTER TABLE pca_quarterly_fct ADD CONSTRAINT pcaq_pkey PRIMARY KEY(pca, property_type_cd, completion_year, quarter);
-- CREATE INDEX pcaq_date_idx ON pca_quarterly_fct USING btree (completion_year, quarter);
-- CREATE INDEX pcaq_n_idx ON pca_quarterly_fct USING btree (n);
-- CREATE INDEX pcaq_pd_idx ON pca_quarterly_fct USING btree (pca COLLATE pg_catalog."default");
-- CREATE INDEX pcaq_property_idx ON pca_quarterly_fct USING btree (property_type_cd COLLATE pg_catalog."default");


-- Region
DROP MATERIALIZED VIEW IF EXISTS landreg.region_quarterly_fct; 
CREATE MATERIALIZED VIEW region_quarterly_fct AS 
SELECT  
	region,
	property_type_cd, 
	extract(year from completion_dt) as completion_year, 
	ceil(extract(month from completion_dt)/3.0) as quarter,  
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
	completion_year, 
	quarter
order by 
	region, 
	property_type_cd, 
	completion_year, 
	quarter;
-- ALTER TABLE region_quarterly_fct ADD CONSTRAINT regq_pkey PRIMARY KEY(region, property_type_cd, completion_year, quarter);
-- CREATE INDEX regq_date_idx ON region_quarterly_fct USING btree (completion_year, quarter);
-- CREATE INDEX regq_n_idx ON region_quarterly_fct USING btree (n);
-- CREATE INDEX regq_pd_idx ON region_quarterly_fct USING btree (region COLLATE pg_catalog."default");
-- CREATE INDEX regq_property_idx ON region_quarterly_fct USING btree (property_type_cd COLLATE pg_catalog."default");


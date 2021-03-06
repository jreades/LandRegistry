DROP MATERIALIZED VIEW IF EXISTS viz.hex{year}_{resolution}m; 
CREATE MATERIALIZED VIEW viz.hex{year}_{resolution}m AS 
SELECT 
	row_number() OVER() as id, 
	extract(year from pp.completion_dt) as yr,
	quantile(pp.price_int, 0.5) as median_price,
--	pp.property_type_cd as type,
	RPIConversion(lf.income_amt*52::numeric, 2012, {year}) AS median_income, 
	round(quantile(pp.price_int, 0.5)::numeric / RPIConversion(lf.income_amt*52::numeric, 2012, {year}), 2) AS affordability,
	h.gid, 
	h.geom
 FROM 
 	landreg.price_paid_fct pp,
	inflation.hh_income_fct lf, 
	osopen.hex_mapping_dim hd, 
	osopen.hex_{resolution}m h 
WHERE extract(year from pp.completion_dt) = {year} 
	AND lf.year = {year}
	AND lf.metric_nm::text = 'Median'::text 
	AND lf.region_nm::text = 'All households4'::text 
	AND pp.pc     = hd.pc 
	AND hd.h{resolution}m = h.gid 
GROUP BY yr, h.gid, lf.income_amt;
ALTER TABLE viz.hex{year}_{resolution}m 
  OWNER TO postgres;
GRANT SELECT ON TABLE viz.hex{year}_{resolution}m TO public;
GRANT ALL ON TABLE viz.hex{year}_{resolution}m TO postgres;

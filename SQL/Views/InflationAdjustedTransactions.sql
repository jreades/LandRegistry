DROP MATERIALIZED VIEW IF EXISTS viz.{reg}{year}; 
CREATE MATERIALIZED VIEW viz.{reg}{year} AS 
SELECT pp.uid,
	pp.yr,
	pp.tid,
	pp.price,
	pp.postcode,
	pp.type,
	RPIConversion(lf.income_amt*52::numeric, 2012, pp.yr) AS median_income, 
	round(pp.price::numeric / RPIConversion(lf.income_amt*52::numeric, 2012, pp.yr), 2) AS affordability,
	pp.geom
 FROM 
 	viz.pp_transaction_spa pp,
	inflation.hh_income_fct lf
WHERE pp.yr = {year} 
	AND pp.yr = lf.year 
	AND lf.metric_nm::text = 'Median'::text 
	AND lf.region_nm::text = '{region}'::text;
ALTER TABLE viz.{reg}{year}
  OWNER TO postgres;
GRANT SELECT ON TABLE viz.{reg}{year} TO public;
GRANT ALL ON TABLE viz.{reg}{year} TO postgres;
DROP MATERIALIZED VIEW IF EXISTS {reg}.viz{year}; 
CREATE MATERIALIZED VIEW {reg}.viz{year} AS 
SELECT pp.uid,
	pp.yr,
	pp.tid,
	pp.price,
	pp.pc,
	pp.type,
	inflation.RPIConversion(lf.income_amt*52::numeric, 2012, pp.yr::integer) AS median_income, 
	round(pp.price::numeric / inflation.RPIConversion(lf.income_amt*52::numeric, 2012, pp.yr::integer), 2) AS affordability,
	pp.geom
 FROM 
 	{reg}.pp_transaction_spa pp,
	inflation.hh_income_fct lf
WHERE pp.yr = {year} 
	AND pp.yr = lf.year 
	AND lf.metric_nm::text = 'Median'::text 
	AND lf.region_nm::text = '{region}'::text;
ALTER TABLE {reg}.viz{year}
  OWNER TO postgres;
GRANT SELECT ON TABLE {reg}.viz{year} TO public;
GRANT ALL ON TABLE {reg}.viz{year} TO postgres;
DROP MATERIALIZED VIEW viz.pd_annual_change_fct;
CREATE MATERIALIZED VIEW viz.pd_annual_change_fct AS 
SELECT 
	row_number() OVER () AS id, 
	pf1.pd, 
	pp.geom, 
	pf1.md as md1997, 
	pf2.md as md2012
FROM 
	landreg.pd_annual_fct AS pf1
LEFT JOIN landreg.pd_annual_fct AS pf2 ON pf1.pd=pf2.pd 
LEFT JOIN os.pd_spa AS pp ON pf1.pd=pp.pd 
WHERE
	pf1.yr=1997 
AND 
	pf2.yr=2012
;

-- And now calculate the change so we can display it on a map!

DROP MATERIALIZED VIEW viz.ldn_pd_affordability_change_fct;
CREATE MATERIALIZED VIEW viz.ldn_pd_affordability_change_fct AS 	
SELECT 
	row_number() OVER () as id, 
	pp.pd,
	pp.md1997, 
--	RPIConversion(hf1.income_amt*52::numeric, 2012, hf1.year) AS i1997, 
	round(pp.md1997::numeric / RPIConversion(hf1.income_amt*52::numeric, 2012, hf1.year), 2) AS a1997,
	pp.md2012, 
--	RPIConversion(hf2.income_amt*52::numeric, 2012, hf2.year) AS i2012, 
	round(pp.md2012::numeric / RPIConversion(hf2.income_amt*52::numeric, 2012, hf2.year), 2) AS a2012,
	round( (pp.md2012::numeric / RPIConversion(hf2.income_amt*52::numeric, 2012, hf2.year))-(pp.md1997::numeric / RPIConversion(hf1.income_amt*52::numeric, 2012, hf1.year)), 3) AS change, 
	pp.geom
FROM 
	viz.pd_annual_change_fct pp 
LEFT JOIN inflation.hh_income_fct hf1 ON 1=1
LEFT JOIN inflation.hh_income_fct hf2 ON 1=1
WHERE 
	pp.pd IN (SELECT DISTINCT pd FROM os.pc_mapping_dim WHERE region='London') 
AND 	
	hf1.region_nm='London' AND hf1.year=1997 AND hf1.metric_nm='Median'
AND 
	hf2.region_nm='London' AND hf2.year=2012 AND hf2.metric_nm='Median' 
;

SELECT 
    id, 
	pd2,
	md1997, 
	a1997,
	md2012, 
	a2012,
    (a2012-a1997) as abschg, 
	(a2012-a1997)/a1997 as pctchg, 
	geom
FROM 
    viz.ldn_pd2_affordability_change_fct
;
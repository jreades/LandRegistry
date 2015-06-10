SET search_path = {area}, landreg, os, osopen, public, pg_catalog;

-- Need to do this after loading in
-- the building data
CREATE INDEX {area}_feat_idx
  ON {area}.building_fct
  USING btree
  (featcode);
SELECT UpdateGeometrySRID('{area}','building_fct','geom',27700);
CREATE INDEX {area}_geom_gist 
  ON {area}.building_fct
  USING GIST (geom);

-- Find the x/y dimensions of the 
-- building fact table so that we 
-- can select a circle.
SELECT MAX(ST_XMax(geom)) FROM {area}.building_fct; -- 420576.33
SELECT MAX(ST_YMax(geom)) FROM {area}.building_fct; -- 435010.34
SELECT MIN(ST_XMin(geom)) FROM {area}.building_fct; -- 347504.12
SELECT MIN(ST_YMin(geom)) FROM {area}.building_fct; -- 361456.31
-- Diameter: 36536.11
-- Midpoint X: 384040.23
-- Midpoint Y: 398233.33

-- Select all postcodes that fall 
-- within the circle we've just 
-- calculated above.
CREATE TABLE {area}.pc_spa AS
SELECT * 
FROM os.pc_spa AS p
WHERE ST_Intersects(ST_Buffer(ST_GeomFromText('POINT({midx} {midy})',{diameter}),27700), p.geom);
ALTER TABLE {area}.pc_spa ADD CONSTRAINT {area}_pc_spa_pidx PRIMARY KEY(gid);
CREATE INDEX {area}_pc_spa_gix ON {area}.pc_spa USING GIST (geom);
CREATE INDEX {area}_pc_spa_idx ON {area}.pc_spa (pc);
VACUUM ANALYZE {area}.pc_spa;
CLUSTER {area}.pc_spa USING {area}_pc_spa_gix;
ANALYZE {area}.pc_spa;

-- Now we basically merge all of 
-- the buildings falling within a
-- postcode so that we can easily 
-- place transactions at some random
-- point inside them
CREATE TABLE {area}.pc_building_fct AS 
SELECT 
	pc.gid as gid, 
	pc.pc as pc, 
	ST_INTERSECTION(pc.geom, ST_Union(ST_Buffer(bf.geom,0.000001))) as intersect_geom
FROM 
	{area}.pc_spa as pc, 
	{area}.building_fct as bf
WHERE 
	ST_INTERSECTS(bf.geom, pc.geom)
GROUP BY 
	pc.gid, pc.pc;
ALTER TABLE {area}.pc_building_fct ADD CONSTRAINT {area}_pc_building_pidx PRIMARY KEY(gid);
CREATE INDEX {area}_pc_building_gix ON {area}.pc_building_fct USING GIST (geom);
CREATE INDEX {area}_pc_building_idx ON {area}.pc_building_fct (pc);
VACUUM ANALYZE {area}.pc_building_fct;
CLUSTER {area}.pc_building_fct USING {area}_pc_building_gix;
ANALYZE {area}.pc_building_fct;

-- Now we need to find out how 
-- many transactions there were 
-- in each year so that we can 
-- create the right number of 
-- random points.
DROP TABLE IF EXISTS {area}.pc_transaction_cnt;
create table {area}.pc_transaction_cnt as (
select pc.pc as pc, 
extract(year from ppf.completion_dt) as transaction_yr, 
count(*) as transaction_cnt 
from 
	landreg.price_paid_fct as ppf, 
	{area}.pc_spa as pc 
where pc.pc=ppf.pc 
group by pc.pc, transaction_yr 
order by 2 desc);
ALTER TABLE {area}.pc_transaction_cnt
  ADD CONSTRAINT pc_transaction_cnt_pidx PRIMARY KEY(pc,transaction_yr);

-- And create the random points that 
-- we now want to show. 
DROP TABLE IF EXISTS {area}.pc_transaction_spa;
CREATE TABLE {area}.pc_transaction_spa AS 
SELECT 
	ROW_NUMBER() OVER () AS uid, 
	extract(year from ppf.completion_dt) as yr, 
	ppf.transaction_id AS tid, 
	ppf.price_int AS price, 
	ppf.pc, 
	ppf.property_type_cd AS type, 
	viz.RandomPointsInPolygon(pbf.geom, 1::integer) as geom
FROM 
	landreg.price_paid_fct AS ppf, 
	{area}.pc_building_fct AS pbf 
WHERE 
	ppf.pc=pbf.pc;
ALTER TABLE {area}.pc_transaction_spa ADD CONSTRAINT {area}_pc_transaction_spa_pidx PRIMARY KEY(uid);
CREATE INDEX {area}_pc_transaction_spa_gix ON {area}.pc_transaction_spa USING GIST (geom);
CREATE INDEX {area}_pc_transaction_spa_idx ON {area}.pc_transaction_spa (pc);
ANALYZE {area}.pc_transaction_spa;
CLUSTER {area}.pc_transaction_spa USING {area}_pc_transaction_spa_gix;
VACUUM ANALYZE {area}.pc_transaction_spa_gix;

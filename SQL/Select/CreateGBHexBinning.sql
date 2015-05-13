-- CREATE TABLE public.hex_750m AS
-- SELECT g.gid, g.geom 
-- FROM public.grid_750m as g 
-- WHERE ST_Intersects(g.geom, (SELECT geom FROM os.uk WHERE name='GB'));
-- SELECT UpdateGeometrySRID('public','hex_750m', 'geom',27700);
-- CREATE INDEX hex_750m_gix ON public.hex_750m USING gist (geom);

WITH latest_map AS (
	select p.pc as postcode, h.gid as geomid 
	from osopen.hex_750m as h, os.pc_points as p 
	where ST_WITHIN(p.geom, h.geom)
)
UPDATE osopen.hex_mapping_dim  
SET h750m = latest_map.geomid 
FROM latest_map  
WHERE pc = latest_map.postcode
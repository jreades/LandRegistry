CREATE TABLE public.hex_750m AS
SELECT g.gid, g.geom 
FROM public.grid_750m as g 
WHERE ST_Intersects(g.geom, (SELECT geom FROM os.uk WHERE name='GB'));
SELECT UpdateGeometrySRID('public','hex_750m', 'geom',27700);
CREATE INDEX hex_750m_gix ON public.hex_750m USING gist (geom);
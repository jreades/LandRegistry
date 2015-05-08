CREATE OR REPLACE FUNCTION RPIConversion (
                i_price float8,
                i integer, 
                j integer
        )
        RETURNS numeric
        AS $$
DECLARE
        adjusted numeric := 0; -- total area
BEGIN
        SELECT i_price * (SELECT rpi_idx FROM inflation.rpi_fct WHERE yr=j)/(SELECT rpi_idx FROM inflation.rpi_fct WHERE yr=i) INTO adjusted;
        RETURN ROUND(adjusted::numeric,2);
END; 
$$ LANGUAGE plpgsql;
ALTER FUNCTION RPIConversion(float8, integer, integer) OWNER TO postgres;
GRANT EXECUTE ON FUNCTION RPIConversion(float8,integer,integer) TO remote;
GRANT SELECT ON ALL TABLES IN SCHEMA inflation TO remote;
GRANT USAGE ON SCHEMA inflation TO remote;
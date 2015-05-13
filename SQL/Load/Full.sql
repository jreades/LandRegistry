SET search_path TO landreg, public;

-- 
-- Check for duplicates in the loader_fct
-- 
DELETE FROM loader_fct t 
WHERE EXISTS (SELECT 1 FROM loader_fct t1 WHERE t1.transaction_id = t.transaction_id AND t1.ctid > t.ctid)

--
-- Remove constraints and indexes prior to bulk load
--
ALTER TABLE ONLY price_paid_fct 
	DROP CONSTRAINT IF EXISTS ppf_pidx; 
DROP INDEX IF EXISTS ppf_authority_idx;
DROP INDEX IF EXISTS ppf_county_idx;
DROP INDEX IF EXISTS ppf_date_idx;
DROP INDEX IF EXISTS ppf_pc_idx;
DROP INDEX IF EXISTS ppf_type_idx;

--
-- Move the data into position
-- 
TRUNCATE TABLE price_paid_fct;

COPY loader_fct TO '/tmp/table.csv' DELIMITER ',';
COPY price_paid_fct FROM '/tmp/table.csv' DELIMITER ',';

ANALYZE price_paid_fct;
TRUNCATE TABLE loader_fct; 

--
-- Rebuild the constraints and indexes 
--

ALTER TABLE ONLY price_paid_fct
    ADD CONSTRAINT ppf_pidx PRIMARY KEY (transaction_id);

CREATE INDEX ppf_authority_idx ON price_paid_fct USING btree (authority_nm);

CREATE INDEX ppf_county_idx ON price_paid_fct USING btree (county_nm);

CREATE INDEX ppf_date_idx ON price_paid_fct USING btree (completion_dt);

CREATE INDEX ppf_pc_idx ON price_paid_fct USING btree (pc);

CREATE INDEX ppf_type_idx ON price_paid_fct USING btree (property_type_cd, new_build_cd, tenure_cd);

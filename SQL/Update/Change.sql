UPDATE landreg.price_paid_fct 
SET completion_dt=%s, pc=%s, property_type_cd=%s, new_build_cd=%s, tenure_cd=%s, paon=%s, saon=%s, street_nm=%s, locality_nm=%s, town_nm=%s, authority_nm=%s, county_nm=%s, status_cd=%s, price_int=%s  
WHERE transaction_id='{tid}';

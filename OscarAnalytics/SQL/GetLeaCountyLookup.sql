-- FUNCTION: crdc_import.GetLeaCountyLookup()
-- DROP FUNCTION crdc_import."GetLeaCountyLookup"();

CREATE OR REPLACE FUNCTION crdc_import."GetLeaCountyLookup"()
    RETURNS TABLE(
        state_code       TEXT,
        school_district  TEXT,
        lea_id           TEXT,
        city             TEXT,
        zip_code         TEXT,
        state_fip        TEXT,
        county           TEXT,
        county_fip       TEXT
    )
    LANGUAGE plpgsql
    COST 100
    VOLATILE
    ROWS 20000

AS $BODY$
/*
    Function    : GetLeaCountyLookup
    Description : Returns one row per Local Education Agency (LEA), including:
                  – state_code       : two-letter state postal code
                  – school_district  : LEA name
                  – lea_id           : LEA identifier
                  – city             : LEA’s city
                  – zip_code         : LEA’s ZIP code
                  – state_fip        : two-digit state FIPS
                  – county           : county name
                  – county_fip       : three-digit county FIPS
    Author      : Oscar A Mallen
    Output      : A lookup table of LEAs joined to their county geocode.
    Example     :
        SELECT * FROM crdc_import."GetLeaCountyLookup"() LIMIT 10;
*/
BEGIN
    RETURN QUERY
    SELECT
        lc.lea_state::TEXT     AS state_code,
        lc.lea_name::TEXT      AS school_district,
        lc.leaid::TEXT         AS lea_id,
        lc.lea_city::TEXT      AS city,
        lc.lea_zip::TEXT       AS zip_code,
        geo.stfip::TEXT        AS state_fip,
        geo.nmcnty::TEXT       AS county,
        geo.cnty::TEXT         AS county_fip
    FROM
        crdc_import.lea_characteristics lc
    LEFT JOIN
        crdc_import.edge_geocode_publiclea_1718 geo
      ON ltrim(lc.leaid,'0') = geo.leaid::TEXT
    ORDER BY
        lc.lea_state,
        lc.lea_name;
END;
$BODY$;

-- ALTER FUNCTION crdc_import."GetLeaCountyLookup"() OWNER TO postgres;

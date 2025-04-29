-- FUNCTION: crdc_import.GetCountyPovertyStats()
-- DROP FUNCTION IF EXISTS crdc_import."GetCountyPovertyStats"();

CREATE OR REPLACE FUNCTION crdc_import."GetCountyPovertyStats"()
  RETURNS TABLE(
    county               TEXT,
    county_fip           TEXT,
    total_population     BIGINT,
    total_pop_5_17       BIGINT,
    total_pov_5_17       BIGINT,
    pct_poverty_5_17     NUMERIC
  )
  LANGUAGE sql
  COST 100
  VOLATILE
  ROWS 3129
AS $BODY$
  /*
    Function    : GetCountyPovertyStats
    Description : Aggregates USSD17 poverty estimates up to the county level.
                  – county             : county name
                  – county_fip         : three-digit FIPS code
                  – total_population   : sum of estimated total population
                  – total_pop_5_17     : sum of population aged 5–17
                  – total_pov_5_17     : sum of 5–17 year-olds in poverty
                  – pct_poverty_5_17   : percent of 5–17 in poverty
    Author      : Oscar A Mallen
    Change log  :
    Example     :
      SELECT * FROM crdc_import."GetCountyPovertyStats"();
  */
  WITH ussd_clean AS (
    SELECT
      "unnamed_2"           AS district_id,
      "unnamed_4"::BIGINT   AS est_population,
      "unnamed_5"::BIGINT   AS pop_5_17,
      "unnamed_6"::BIGINT   AS pop_5_17_in_poverty
    FROM crdc_import.ussd17
    WHERE "unnamed_2" ~ '^\d{5}$'
  ),
  district_to_county AS (
    SELECT
      substring(lea_id FROM 3 FOR 5) AS district_id,
      county,
      county_fip
    FROM crdc_import."GetLeaCountyLookup"()
    WHERE county IS NOT NULL
  ),
  ussd_by_county AS (
    SELECT
      dtc.county,
      dtc.county_fip,
      uc.est_population,
      uc.pop_5_17,
      uc.pop_5_17_in_poverty
    FROM district_to_county dtc
    LEFT JOIN ussd_clean uc
      ON uc.district_id = dtc.district_id
  )
  SELECT
    ubc.county,
    ubc.county_fip,
    COALESCE(SUM(ubc.est_population),0)           AS total_population,
    COALESCE(SUM(ubc.pop_5_17),0)                 AS total_pop_5_17,
    COALESCE(SUM(ubc.pop_5_17_in_poverty),0)      AS total_pov_5_17,
    ROUND(
      COALESCE(SUM(ubc.pop_5_17_in_poverty),0)::NUMERIC
      / NULLIF(COALESCE(SUM(ubc.pop_5_17),0),0)
      * 100
    ,2)                                            AS pct_poverty_5_17
  FROM ussd_by_county ubc
  GROUP BY
    ubc.county,
    ubc.county_fip
  ORDER BY
    pct_poverty_5_17 DESC
$BODY$;

-- ALTER FUNCTION crdc_import."GetCountyPovertyStats"() OWNER TO postgres;

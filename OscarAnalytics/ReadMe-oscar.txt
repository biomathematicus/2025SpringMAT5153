Interesting Queries and Notes   by Oscar Mallen and OscarGPT

1.) Run this Query (), and you will see many null values on county_fip because of 
missing IDs in the geocode table

All certified LEA matched with geocode data:
---------------------------------------
SELECT
  lc.LEA_STATE   AS state_code,      -- e.g. “AL”
  lc.LEA_NAME    AS school_district, -- district name
  lc.LEAID       AS lea_id,          -- unique LEA identifier
  lc.LEA_CITY    AS city,            -- city from LEA characteristics
  lc.LEA_ZIP     AS zip_code,        -- ZIP code from LEA characteristics
  geo.nmcnty     AS county,          -- county name from geo table
  geo.cnty       AS county_fip       -- county FIPS from geo table
FROM
  crdc_import.lea_characteristics lc
LEFT JOIN
  crdc_import.edge_geocode_publiclea_1718 geo
    ON lc.LEAID = geo.LEAID
;
--------------------------------------

run this query next and you will see a huge discrepancy
--------------------------------------------------
SELECT 
  COUNT(*) AS total_leas,
  COUNT(geo.LEAID) AS matched_leas
FROM crdc_import.lea_characteristics lc
LEFT JOIN crdc_import.edge_geocode_publiclea_1718 geo
  ON lc.LEAID = geo.LEAID;
--------------------------------------------------

Out of the 17604 LEA_IDs only 14214 matched...
This is extremely odd because the CRDC Technical Doc states there is ->
17637 LEA, 17604 of which are certified. So where are the leaIDs of ->
the remaining 3390 ?

districts that did not match their geocode data
----------------------------------------------------------
SELECT
  lc.LEA_STATE   AS state_code,
  lc.LEA_NAME    AS school_district,
  lc.LEAID       AS lea_id,
  lc.LEA_CITY    AS city,
  lc.LEA_ZIP     AS zip_code
FROM
  crdc_import.lea_characteristics lc
LEFT JOIN
  crdc_import.edge_geocode_publiclea_1718 geo
  ON lc.LEAID = geo.LEAID
WHERE
  geo.LEAID IS NULL;
----------------------------------------------------------
The query shows exactly 3390 queries many of which have leading 0s 

exactly 17604 :
---------------------------------------------------------------
SELECT
  lc.LEA_STATE   AS state_code,
  lc.LEA_NAME    AS school_district,
  lc.LEAID       AS lea_id,
  lc.LEA_CITY    AS city,
  lc.LEA_ZIP     AS zip_code,
  geo.nmcnty     AS county,
  geo.cnty       AS county_fip
FROM
  crdc_import.lea_characteristics lc
LEFT JOIN
  crdc_import.edge_geocode_publiclea_1718 geo
    ON ltrim(lc.LEAID, '0') = geo.leaid::TEXT;

---------------------------------------------------------------
This lets us accurately refernce the total number of local learning 
authorities easily by matching to the LeaID with relevant info
////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////

2.) Match each county to its poverty data using usdd17 and calcualte a ratio for poverty in the group of (5-17) yrs
-------------------------------------------------------------------
WITH ussd_agg AS (
  SELECT
    "unnamed_2"                               AS district_id,
    SUM("unnamed_4"::BIGINT)                  AS est_population,
    SUM("unnamed_5"::BIGINT)                  AS pop_5_17,
    SUM("unnamed_6"::BIGINT)                  AS pop_5_17_in_poverty
  FROM crdc_import.ussd17
  WHERE "unnamed_2" ~ '^\d{5}$'               -- only real 5-digit codes
  GROUP BY "unnamed_2"
)
SELECT
  lcl.state_code,
  lcl.county,
  lcl.county_fip,
  SUM(ua.est_population)               AS total_population,
  SUM(ua.pop_5_17)                     AS total_pop_5_17,
  SUM(ua.pop_5_17_in_poverty)          AS total_pov_5_17,
  ROUND(
    SUM(ua.pop_5_17_in_poverty)::NUMERIC
    / NULLIF(SUM(ua.pop_5_17), 0)
    * 100
  ,2)                                   AS pct_poverty_5_17
FROM
  crdc_import."GetLeaCountyLookup"() AS lcl
  JOIN ussd_agg AS ua
    ON substring(lcl.lea_id FROM 3 FOR 5) = ua.district_id
GROUP BY
  lcl.state_code,
  lcl.county,
  lcl.county_fip
ORDER BY
  pct_poverty_5_17 DESC
;
-------------------------------------------------------------------
This gets us 3128 Rows which is one number short of the distinct counties
in get county by GetLeaCountyLookup(). If we switch to a left join we will include
counties that dont have poverty data 
--------------------------------------------------------------------
WITH ussd_clean AS (
  -- 1) strip out header rows and cast your four key columns
  SELECT
    "unnamed_2"       AS district_id,
    "unnamed_4"::BIGINT AS est_population,
    "unnamed_5"::BIGINT AS pop_5_17,
    "unnamed_6"::BIGINT AS pop_5_17_in_poverty
  FROM crdc_import.ussd17
  WHERE "unnamed_2" ~ '^\d{5}$'
),
district_to_county AS (
  -- 2) build a small lookup of district → county
  SELECT
    substring(lea_id FROM 3 FOR 5) AS district_id,
    county,
    county_fip
  FROM crdc_import."GetLeaCountyLookup"()
  WHERE county IS NOT NULL
),
ussd_by_county AS (
  -- 3) left-join so even counties with no poverty rows still appear
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
-- 4) aggregate up to one row per county, filling missing sums with 0
SELECT
  county,
  county_fip,
  COALESCE(SUM(est_population),    0) AS total_population,
  COALESCE(SUM(pop_5_17),          0) AS total_pop_5_17,
  COALESCE(SUM(pop_5_17_in_poverty),0) AS total_pov_5_17,
  ROUND(
    COALESCE(SUM(pop_5_17_in_poverty),0)::NUMERIC
    / NULLIF(COALESCE(SUM(pop_5_17),0),0)
    * 100
  ,2)                                 AS pct_poverty_5_17
FROM ussd_by_county
GROUP BY
  county,
  county_fip
ORDER BY
  pct_poverty_5_17 DESC
;
--------------------------------------------------------------------
returns 3129 row, with richmond county missing poverty data 










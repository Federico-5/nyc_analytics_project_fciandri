WITH source AS (
   SELECT * FROM {{ source('raw', 'source_nyc_dob_job_apps') }}
),

cleaned AS (
   SELECT
       -- Keep all other columns as they are
       * EXCEPT (
           job_filing_number,
           filing_status,
           job_type,
           job_description,
           building_type,
           borough,
           house_no,
           street_name,
           apt_condo_no_s,
           zip,
           postcode,
           bin,
           bbl,
           block,
           lot,
           commmunity_board,
           council_district,
           census_tract,
           nta,
           latitude,
           longitude,
           filing_date,
           current_status_date,
           approved_date,
           first_permit_date,
           signoff_date,
           initial_cost,
           existing_dwelling_units,
           proposed_dwelling_units,
           existing_stories,
           proposed_no_of_stories,
           existing_height,
           proposed_height,
           total_construction_floor_area,
           owner_s_business_name
       ),

       -- Identifiers
       UPPER(TRIM(job_filing_number)) AS job_filing_number,

       -- Filing details
       UPPER(TRIM(filing_status)) AS filing_status,
       UPPER(TRIM(job_type)) AS job_type,
       TRIM(job_description) AS job_description,
       TRIM(building_type) AS building_type,

       -- Dates: raw data is all STRING, so handle both ISO and MM/DD/YYYY formats
       COALESCE(
           SAFE_CAST(filing_date AS TIMESTAMP),
           SAFE.PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S %p', filing_date),
           SAFE.PARSE_TIMESTAMP('%m/%d/%Y', filing_date)
       ) AS filing_date,
       COALESCE(
           SAFE_CAST(current_status_date AS TIMESTAMP),
           SAFE.PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S %p', current_status_date),
           SAFE.PARSE_TIMESTAMP('%m/%d/%Y', current_status_date)
       ) AS current_status_date,
       COALESCE(
           SAFE_CAST(approved_date AS TIMESTAMP),
           SAFE.PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S %p', approved_date),
           SAFE.PARSE_TIMESTAMP('%m/%d/%Y', approved_date)
       ) AS approved_date,
       COALESCE(
           SAFE_CAST(first_permit_date AS TIMESTAMP),
           SAFE.PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S %p', first_permit_date),
           SAFE.PARSE_TIMESTAMP('%m/%d/%Y', first_permit_date)
       ) AS first_permit_date,
       COALESCE(
           SAFE_CAST(signoff_date AS TIMESTAMP),
           SAFE.PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S %p', signoff_date),
           SAFE.PARSE_TIMESTAMP('%m/%d/%Y', signoff_date)
       ) AS signoff_date,

       -- Location - standardized borough (same logic as 311 staging = conformed)
       CASE
           WHEN UPPER(TRIM(borough)) IN ('MANHATTAN', 'NEW YORK COUNTY') THEN 'Manhattan'
           WHEN UPPER(TRIM(borough)) IN ('BRONX', 'THE BRONX') THEN 'Bronx'
           WHEN UPPER(TRIM(borough)) IN ('BROOKLYN', 'KINGS COUNTY') THEN 'Brooklyn'
           WHEN UPPER(TRIM(borough)) IN ('QUEENS', 'QUEEN', 'QUEENS COUNTY') THEN 'Queens'
           WHEN UPPER(TRIM(borough)) IN ('STATEN ISLAND', 'RICHMOND COUNTY') THEN 'Staten Island'
           ELSE 'UNKNOWN or CITYWIDE'
       END AS borough,

       -- Location - zip: use zip, fall back to postcode, keep first 5 digits only
       CASE
           WHEN REGEXP_CONTAINS(TRIM(COALESCE(zip, postcode)), r'^\d{5}')
           THEN SUBSTR(TRIM(COALESCE(zip, postcode)), 1, 5)
           ELSE NULL
       END AS zip,

       -- Location - address and property identifiers (IDs stay STRING)
       TRIM(house_no) AS house_number,
       UPPER(TRIM(street_name)) AS street_name,
       TRIM(apt_condo_no_s) AS apt_condo_number,
       TRIM(bin) AS bin,
       TRIM(bbl) AS bbl,
       TRIM(block) AS block,
       TRIM(lot) AS lot,
       TRIM(commmunity_board) AS community_board,   -- fix triple-m typo
       SAFE_CAST(council_district AS INT64) AS council_district,
       TRIM(census_tract) AS census_tract,
       TRIM(nta) AS nta,
       SAFE_CAST(latitude AS NUMERIC) AS latitude,
       SAFE_CAST(longitude AS NUMERIC) AS longitude,

       -- Measures: remove $ and commas, then cast to numbers
       SAFE_CAST(REGEXP_REPLACE(initial_cost, r'[$,]', '') AS NUMERIC) AS initial_cost,
       SAFE_CAST(existing_dwelling_units AS INT64) AS existing_dwelling_units,
       SAFE_CAST(proposed_dwelling_units AS INT64) AS proposed_dwelling_units,
       SAFE_CAST(existing_stories AS INT64) AS existing_stories,
       SAFE_CAST(proposed_no_of_stories AS INT64) AS proposed_stories,
       SAFE_CAST(existing_height AS NUMERIC) AS existing_height,
       SAFE_CAST(proposed_height AS NUMERIC) AS proposed_height,
       SAFE_CAST(REGEXP_REPLACE(total_construction_floor_area, r'[,]', '') AS NUMERIC) AS total_construction_floor_area,

       -- Clearer name
       TRIM(owner_s_business_name) AS owner_business_name,

       -- Metadata
       CURRENT_TIMESTAMP() AS _stg_loaded_at

   FROM source
),

filtered AS (
   SELECT *
   FROM cleaned
   WHERE job_filing_number IS NOT NULL
     AND filing_date IS NOT NULL
     AND CAST(filing_date AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 YEAR)

   -- Deduplicate: keep the most recent status per filing
   QUALIFY ROW_NUMBER() OVER (
       PARTITION BY job_filing_number
       ORDER BY current_status_date DESC
   ) = 1
)

SELECT * FROM filtered
-- All should be part of this table after running: stg_nyc_dob_job_filings
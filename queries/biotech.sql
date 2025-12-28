SELECT
  publication_number,
  application_number,
  pct_number,
  filing_date,
  -- This flattens the array into a single string for the CSV row
  inv_country AS inventor_country,
  
  -- Count of DIFFERENT IPCs
  (SELECT COUNT(DISTINCT code) FROM UNNEST(ipc)) AS total_ipc_count,
  
  -- Count of MATCHED IPCs
  ARRAY_LENGTH(ARRAY(
    SELECT code FROM UNNEST(ipc) 
    WHERE 
      REGEXP_CONTAINS(code, r'^A01H\s*0*(1|4)/')
      OR REGEXP_CONTAINS(code, r'^A01K\s*0*67/')
      OR REGEXP_CONTAINS(code, r'^A61K\s*35/(1[2-9]|[2-6]\d|7\d)')
      OR REGEXP_CONTAINS(code, r'^A61K\s*0*(38|39|48)/')
      OR REGEXP_CONTAINS(code, r'^C02F\s*0*3/34')
      OR REGEXP_CONTAINS(code, r'^C07G\s*0*(11|13|15)/')
      OR REGEXP_CONTAINS(code, r'^C07K\s*0*(4|14|16|17|19)/')
      OR REGEXP_CONTAINS(code, r'^C12[MNPQ]')
      OR REGEXP_CONTAINS(code, r'^C40B\s*10/')
      OR REGEXP_CONTAINS(code, r'^C40B\s*40/0[2-8]')
      OR REGEXP_CONTAINS(code, r'^C40B\s*50/06')
      OR REGEXP_CONTAINS(code, r'^G01N\s*27/327')
      OR REGEXP_CONTAINS(code, r'^G01N\s*33/(5[3457]|68|7[468]|88|92)')
      OR REGEXP_CONTAINS(code, r'^G06F\s*19/(1[0-8]|2[0-4])')
  )) AS matched_ipc_count

FROM
  `patents-public-data.patents.publications` p,
  -- UNNEST here creates one row per distinct country code
  UNNEST(ARRAY(
    SELECT DISTINCT country_code 
    FROM UNNEST(inventor_harmonized) 
    WHERE country_code IS NOT NULL
  )) AS inv_country

WHERE
  country_code = 'WO'
  AND EXISTS (
    SELECT 1
    FROM UNNEST(ipc) AS i
    WHERE
      REGEXP_CONTAINS(i.code, r'^A01H\s*0*(1|4)/')
      OR REGEXP_CONTAINS(i.code, r'^A01K\s*0*67/')
      OR REGEXP_CONTAINS(i.code, r'^A61K\s*35/(1[2-9]|[2-6]\d|7\d)')
      OR REGEXP_CONTAINS(i.code, r'^A61K\s*0*(38|39|48)/')
      OR REGEXP_CONTAINS(i.code, r'^C02F\s*0*3/34')
      OR REGEXP_CONTAINS(i.code, r'^C07G\s*0*(11|13|15)/')
      OR REGEXP_CONTAINS(i.code, r'^C07K\s*0*(4|14|16|17|19)/')
      OR REGEXP_CONTAINS(i.code, r'^C12[MNPQ]')
      OR REGEXP_CONTAINS(i.code, r'^C40B\s*10/')
      OR REGEXP_CONTAINS(i.code, r'^C40B\s*40/0[2-8]')
      OR REGEXP_CONTAINS(i.code, r'^C40B\s*50/06')
      OR REGEXP_CONTAINS(i.code, r'^G01N\s*27/327')
      OR REGEXP_CONTAINS(i.code, r'^G01N\s*33/(5[3457]|68|7[468]|88|92)')
      OR REGEXP_CONTAINS(i.code, r'^G06F\s*19/(1[0-8]|2[0-4])')
  )
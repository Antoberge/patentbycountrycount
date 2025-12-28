WITH categorized_patents AS (
  SELECT
    publication_number,
    filing_date,
    -- Scalar subquery to find the primary group for each patent
    (
      SELECT MIN(tech_group)
      FROM (
        SELECT 
          CASE 
            -- 2. Mobile communication
            WHEN REGEXP_CONTAINS(i.code, r'^H04B\s*7/|^H04W') 
                 AND NOT REGEXP_CONTAINS(i.code, r'^H04W\s*(4/24|12)') THEN 2
            
            -- 3. Security
            WHEN REGEXP_CONTAINS(i.code, r'^G06F\s*12/14|^G06F\s*21|^G06K\s*19|^G09C|^G11C\s*8/20|^H04K|^H04L\s*9|^H04M\s*1/(66|67|68|69|70|727)|^H04N\s*7/(167|171)|^H04W\s*12|^G06Q\s*20|^G07F\s*7/(08|12)|^G07G\s*1/(12|14)|^H04L\s*12/14|^H04W\s*4/24') THEN 3
            
            -- 1. High speed network
            WHEN REGEXP_CONTAINS(i.code, r'^H03K|^H03L|^H03M|^H04B\s*1/(69|71)|^H04J|^H04L|^H04M\s*(3|13|19|99)|^H04Q|^H04B\s*(1/00|1/68|1/72|3|17)|^H04H') 
                 AND NOT REGEXP_CONTAINS(i.code, r'^H04L\s*(9|12/14)') THEN 1
            
            -- 4. Sensor and device network
            WHEN REGEXP_CONTAINS(i.code, r'^G08B\s*(1/08|3/10|5/|7/06|13/18|13/19|13/22|25|26|27)|^G08C|^G08G\s*1/(01|06)|^H04B\s*(1/59|5)') THEN 4
            
            -- 5. High speed computing
            WHEN REGEXP_CONTAINS(i.code, r'^G06F\s*(5|7|9|11|13|15/00|15/16|15/17|15/18|15/76|15/82)') THEN 5
            
            -- 6. Large-capacity storage
            WHEN REGEXP_CONTAINS(i.code, r'^G06F\s*3/(06|08)|^G06F\s*12|^G06K\s*(1|7|13)|^G11B|^G11C|^H04N\s*5/(78|90)') 
                 AND NOT REGEXP_CONTAINS(i.code, r'^G06F\s*12/14|^G11C\s*8/20') THEN 6
            
            -- 7. Large-capacity information analysis
            WHEN REGEXP_CONTAINS(i.code, r'^G06F\s*17/(30|40)|^G06F\s*17/(00|10|50)|^G06F\s*19|^G06Q\s*(10|30|40|50|90|99)|^G08G') 
                 AND NOT REGEXP_CONTAINS(i.code, r'^G08G\s*1/(01|06|0962|0969)') THEN 7
            
            -- 8. Cognition and meaning understanding
            WHEN REGEXP_CONTAINS(i.code, r'^G06F\s*17/(20|28)|^G06K\s*9|^G06T\s*7|^G10L\s*(13/027|15|17|25/63|25/66)') THEN 8
            
            -- 9. Human interface
            WHEN REGEXP_CONTAINS(i.code, r'^H04M\s*1|^G06F\s*3/(01|0489|14|153|16)|^G06K\s*11|^G06T\s*11/80|^G08G\s*1/0962|^G09B\s*(5|7|9)') THEN 9
            
            -- 10. Imaging and sound
            WHEN REGEXP_CONTAINS(i.code, r'^H04N|^G06T|^G09G|^H04R|^H04S|^G10L') THEN 10
            
            -- 11. Information communication device
            WHEN REGEXP_CONTAINS(i.code, r'^H03[BCDFGHJ]|^H01B\s*11|^H01L\s*(29|33|21|25|27|43)|^G02B\s*6|^G02F|^H01S\s*5|^B81B\s*7/02|^B82Y\s*10|^H01P|^H01Q') THEN 11
            
            -- 12. Electronic measurement
            WHEN REGEXP_CONTAINS(i.code, r'^G01S|^G01V\s*(3|8|15)') THEN 12
            
            -- 13. Others
            WHEN REGEXP_CONTAINS(i.code, r'^G06F\s*3/(00|05|09|12|13|18)|^G06E|^G06F\s*1|^G06G\s*7|^G06J|^G06K\s*(15|17)|^G06N|^H04M\s*(15|17)') THEN 13
            
            ELSE NULL
          END AS tech_group
        FROM UNNEST(ipc) AS i
      )
      WHERE tech_group IS NOT NULL
    ) AS tech_group_id
  FROM
    `patents-public-data.patents.publications`
  WHERE
    country_code = 'WO'
)
SELECT
  publication_number,
  filing_date,
  tech_group_id
FROM
  categorized_patents
WHERE
  tech_group_id IS NOT NULL
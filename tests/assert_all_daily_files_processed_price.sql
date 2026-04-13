-- Test: All downloaded daily files should be processed in fct_price
-- Returns rows where a downloaded file is missing from fct_price

SELECT
  log.csv_filename
FROM {{ ref('stg_csv_archive_log') }} log
WHERE log.source_type = 'daily'
  AND NOT EXISTS (
    SELECT 1
    FROM {{ ref('fct_price') }} p
    WHERE p.file = log.csv_filename
  )

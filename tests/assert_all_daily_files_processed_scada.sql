-- Test: All downloaded daily files should be processed in fct_scada
-- Returns rows where a downloaded file is missing from fct_scada

SELECT
  log.csv_filename
FROM {{ ref('stg_csv_archive_log') }} log
WHERE log.source_type = 'daily'
  AND NOT EXISTS (
    SELECT 1
    FROM {{ ref('fct_scada') }} s
    WHERE s.file = log.csv_filename
  )

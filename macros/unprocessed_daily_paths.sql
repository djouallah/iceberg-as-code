{% macro unprocessed_daily_paths(var_name, source_type, fact_table) %}
  SET VARIABLE {{ var_name }} = (
    {% if is_incremental() %}
      SELECT COALESCE(NULLIF(list('{{ get_csv_archive_path() }}' || archive_path), []), [''])
      FROM {{ ref('stg_csv_archive_log') }}
      WHERE source_type = '{{ source_type }}'
        AND csv_filename NOT IN (SELECT DISTINCT file FROM {{ fact_table }})
    {% else %}
      SELECT COALESCE(NULLIF(list(file), []), [''])
      FROM glob('{{ get_csv_archive_path() }}/daily/*.gz')
    {% endif %}
  )
{% endmacro %}

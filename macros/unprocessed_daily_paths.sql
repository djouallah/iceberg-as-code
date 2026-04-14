{% macro unprocessed_daily_paths(var_name, source_type, fact_table) %}
  SET VARIABLE {{ var_name }} = (
    {% if is_incremental() %}
      SELECT COALESCE(NULLIF(list(file), []), [''])
      FROM glob('{{ get_csv_archive_path() }}/daily/*.gz') g(file)
      WHERE split_part(split_part(file, '/', -1), '.', 1)
            NOT IN (SELECT DISTINCT file FROM {{ fact_table }})
    {% else %}
      SELECT COALESCE(NULLIF(list(file), []), [''])
      FROM glob('{{ get_csv_archive_path() }}/daily/*.gz')
    {% endif %}
  )
{% endmacro %}

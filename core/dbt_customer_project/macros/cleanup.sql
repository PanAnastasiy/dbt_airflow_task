{% macro drop_my_schema() %}
    {% do adapter.drop_schema(api.Relation.create(database=target.database, schema=target.schema)) %}
    {{ log("Schema dropped!", info=True) }}
{% endmacro %}

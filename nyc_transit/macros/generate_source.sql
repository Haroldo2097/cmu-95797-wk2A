
{% macro generate_source(schema_name, generate_columns=false) %}
    {% set tables = dbt_utils.get_relations_by_prefix(schema_name, '') %}
    
    {% for table in tables %}
        {% set columns = dbt_utils.get_filtered_columns_in_relation(table) %}
        
        {% if generate_columns %}
            {{ log("Generating source for table " ~ table, info=True) }}
            source {{ schema_name }} {{ table.name }} {
                {% for column in columns %}
                    {{ column.name }} {{ column.data_type }}
                {% endfor %}
            }
        {% else %}
            {{ log("Generating source for table " ~ table, info=True) }}
            source {{ schema_name }} {{ table.name }} {}
        {% endif %}
    {% endfor %}
{% endmacro %}

{% macro limit_data_in_dev(colname,numdays=3) %}

{% if target.name == 'dev' %}
where {{ colname }} >= dateadd('day',-{{ numdays }}, current_timestamp)
{% endif %}
{% endmacro %}
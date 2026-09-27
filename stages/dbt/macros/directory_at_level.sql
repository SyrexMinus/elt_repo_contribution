{% macro directory_at_level(filename, nest_level) -%}
case
    when array_size(split({{ filename }}, '/')) <= 1 then '.'
    else array_to_string(
        array_slice(
            split({{ filename }}, '/'),
            0,
            least(
                {{ nest_level }},
                array_size(split({{ filename }}, '/')) - 1
            )
        ),
        '/'
    )
end
{%- endmacro %}

{{ config(
    materialized = 'incremental',
    unique_key = 'page_view_id'
) }}

WITH events AS (

    SELECT
        *
    FROM
        {{ source(
            'snowplow',
            'events'
        ) }}

{% if is_incremental() %}
WHERE
    collector_tstamp >= (
        SELECT
            MAX(max_collector_tstamp)
        FROM
            {{ this }}
    )
{% endif %}
),
page_views AS (
    SELECT
        *
    FROM
        events
    WHERE
        event = 'page_view'
),
aggregated_page_events AS (
    SELECT
        page_view_id,
        COUNT(*) * 10 AS approx_time_on_page,
        MIN(derived_tstamp) AS page_view_start,
        MAX(collector_tstamp) AS max_collector_tstamp
    FROM
        events
    GROUP BY
        1
),
joined AS (
    SELECT
        *
    FROM
        page_views
        LEFT JOIN aggregated_page_events USING (page_view_id)
)
SELECT
    *
FROM
    joined

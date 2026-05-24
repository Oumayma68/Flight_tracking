-- Dimension des vols / appareils
-- Chaque ligne représente un appareil unique identifié par son icao24
--
-- Note : On utilise un modèle incrémental pour capturer les nouveaux appareils
-- sans reconstruire toute la table à chaque run.

{{
    config(
        materialized='incremental',
        unique_key='vol_sk',
        on_schema_change='sync_all_columns'
    )
}}

WITH source AS (

    SELECT DISTINCT
        icao24,
        callsign,
        origine_pays,
        MIN(timestamp_position_utc)     AS premiere_vue_utc,
        MAX(timestamp_position_utc)     AS derniere_vue_utc,
        COUNT(*)                        AS nb_positions_total

    FROM {{ ref('int_positions_deduped') }}

    {% if is_incremental() %}
    WHERE ingested_at >= DATEADD(
        hour,
        -{{ var('incremental_lookback_hours', 2) }},
        CURRENT_TIMESTAMP()
    )
    {% endif %}

    GROUP BY icao24, callsign, origine_pays

),

-- Classement des callsigns par fréquence d'apparition
-- (un avion peut voler sous plusieurs callsigns dans la journée)
ranked AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY icao24
            ORDER BY nb_positions_total DESC
        ) AS rn_callsign

    FROM source

),

final AS (

    SELECT
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['icao24']) }}  AS vol_sk,

        -- Clé naturelle
        icao24,

        -- Attributs
        callsign                        AS callsign_principal,
        origine_pays,

        -- Dériver la compagnie aérienne depuis le callsign (3 premières lettres IATA)
        CASE
            WHEN LENGTH(callsign) >= 3
            THEN LEFT(callsign, 3)
            ELSE 'XXX'
        END                             AS code_compagnie,

        -- Catégorie de vol (estimation basée sur le callsign)
        CASE
            WHEN callsign LIKE 'AFR%'   THEN 'Air France'
            WHEN callsign LIKE 'EZY%'   THEN 'EasyJet'
            WHEN callsign LIKE 'RYR%'   THEN 'Ryanair'
            WHEN callsign LIKE 'BAW%'   THEN 'British Airways'
            WHEN callsign LIKE 'DLH%'   THEN 'Lufthansa'
            WHEN callsign LIKE 'IBE%'   THEN 'Iberia'
            WHEN callsign LIKE 'HOP%'   THEN 'HOP! Air France'
            WHEN callsign LIKE 'TVF%'   THEN 'Transavia France'
            WHEN callsign LIKE 'VLG%'   THEN 'Vueling'
            WHEN callsign LIKE 'AEE%'   THEN 'Aegean Airlines'
            ELSE 'Autre'
        END                             AS compagnie_nom,

        -- Timestamps
        premiere_vue_utc,
        derniere_vue_utc,
        nb_positions_total,

        -- Métadonnées dbt
        CURRENT_TIMESTAMP()             AS dbt_updated_at

    FROM ranked
    WHERE rn_callsign = 1   -- Garder le callsign le plus fréquent

)

SELECT * FROM final

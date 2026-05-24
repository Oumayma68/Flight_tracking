-- Couche 2 : Déduplication avancée des signaux ADS-B
-- Problème résolu :
--   Le même signal ADS-B est capté par plusieurs antennes au sol.
--   Un avion peut donc apparaître 2 à 4 fois dans la même fenêtre de 10s
--   avec des coordonnées légèrement différentes selon l'antenne.
--
-- Stratégie de déduplication :
--   1. Grouper par (icao24 + fenêtre de 30s)
--   2. Garder la position avec le meilleur "score de qualité"
--   3. Générer un surrogate key stable pour les joins aval

{{
    config(
        materialized='incremental',
        unique_key='position_id',
        on_schema_change='sync_all_columns'
    )
}}

WITH staged AS (

    SELECT * FROM {{ ref('stg_positions') }}

    -- Ne traiter que les données non suspectes
    WHERE flag_altitude_suspecte = FALSE
      AND flag_vitesse_suspecte = FALSE
      AND flag_coords_invalides = FALSE

    {% if is_incremental() %}
    AND ingested_at >= DATEADD(
        hour,
        -{{ var('incremental_lookback_hours', 2) }},
        CURRENT_TIMESTAMP()
    )
    {% endif %}

),

-- Créer des "fenêtres de 30 secondes" pour regrouper les signaux quasi-simultanés
windowed AS (

    SELECT
        *,
        -- Arrondir le timestamp à la fenêtre de 30s
        DATEADD(
                 'second',
                  FLOOR(EXTRACT(SECOND FROM timestamp_position_utc) / 30) * 30,
                  DATE_TRUNC('minute', timestamp_position_utc)
                )                      AS window_30s,

        -- Score de qualité : plus c'est bas, meilleure est la source
        -- Priorité : ADS-B direct (0) > ASTERIX (1) > MLAT (2) > FLARM (3)
        COALESCE(source_capteur, 99)                AS quality_score

    FROM staged

),

-- Déduplication : garder la meilleure position par (icao24 + fenêtre 30s)
deduplicated AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY icao24, window_30s
            ORDER BY
                quality_score ASC,           -- Meilleur capteur en premier
                altitude_baro_m DESC NULLS LAST,  -- Altitude la plus haute (moins de bruit)
                ingested_at ASC              -- Plus ancien en cas d'égalité
        ) AS rn

    FROM windowed

),

-- Calculer le taux de montée réel (différence entre 2 positions consécutives)
with_vertical_rate AS (

    SELECT
        d.*,
        -- Taux de montée calculé vs déclaré par le transpondeur
        LAG(altitude_baro_m) OVER (
            PARTITION BY icao24
            ORDER BY timestamp_position_utc
        )                                           AS prev_altitude_m,

        LAG(timestamp_position_utc) OVER (
            PARTITION BY icao24
            ORDER BY timestamp_position_utc
        )                                           AS prev_timestamp,

        -- Delta temps en secondes
        DATEDIFF(
            'second',
            LAG(timestamp_position_utc) OVER (
                PARTITION BY icao24
                ORDER BY timestamp_position_utc
            ),
            timestamp_position_utc
        )                                           AS delta_sec

    FROM deduplicated d
    WHERE rn = 1  -- Une seule ligne par avion par fenêtre de 30s

),

final AS (

    SELECT
        -- Surrogate Key stable 
        {{ dbt_utils.generate_surrogate_key(['icao24', 'window_30s']) }}
                                                    AS position_id,

        --  Identifiants
        icao24,
        callsign,
        origine_pays,

        --  Temps 
        timestamp_position_utc,
        window_30s,
        ingested_at,
        batch_id,

        --  Position 
        latitude,
        longitude,
        altitude_baro_m,
        altitude_geo_m,
        altitude_baro_ft,

        --  Cinématique 
        vitesse_ms,
        vitesse_kmh,
        cap_degres,
        taux_montee_ms                              AS taux_montee_declare_ms,

        -- Taux de montée calculé (plus fiable que le déclaré)
        CASE
            WHEN delta_sec > 0 AND delta_sec <= 120
            THEN ROUND((altitude_baro_m - prev_altitude_m) / delta_sec, 2)
            ELSE NULL
        END                                         AS taux_montee_calcule_ms,

        -- Statut & Qualité 
        au_sol,
        squawk,
        source_capteur,
        quality_score,

        -- Flag : le taux de montée calculé est-il cohérent ?
        -- Un avion ne peut pas monter/descendre de plus de 100 m/s (~20 000 ft/min)
        CASE
            WHEN delta_sec > 0 AND delta_sec <= 120
                AND ABS((altitude_baro_m - prev_altitude_m) / delta_sec) > 100
            THEN TRUE
            ELSE FALSE
        END                                         AS flag_changement_altitude_suspect

    FROM with_vertical_rate

)

SELECT * FROM final

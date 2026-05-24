-- Couche 1 : Nettoyage et typage des données brutes OpenSky
-- Transformations :
--   - Conversion des timestamps UNIX → TIMESTAMP_NTZ
--   - Conversion altitude mètres → pieds (standard aviation)
--   - Conversion vitesse m/s → km/h
--   - Nettoyage des callsigns (trim)
--   - Filtre sur la dernière heure pour les modèles incrémentaux en aval

WITH source AS (

    SELECT * FROM {{ source('raw', 'FLIGHT_POSITIONS') }}

    -- Filtre incremental : ne traite que les nouvelles données
    {% if is_incremental() %}
    WHERE ingested_at >= DATEADD(
        hour,
        -{{ var('incremental_lookback_hours', 2) }},
        CURRENT_TIMESTAMP()
    )
    {% endif %}

),

cleaned AS (

    SELECT
        --  Identifiants
        id                                              AS position_id_raw,
        LOWER(TRIM(icao24))                             AS icao24,
        UPPER(TRIM(callsign))                           AS callsign,
        TRIM(origine_pays)                              AS origine_pays,

        --  Temps 
        -- Conversion timestamp UNIX → TIMESTAMP lisible
        TO_TIMESTAMP_NTZ(time_position)                 AS timestamp_position_utc,
        TO_TIMESTAMP_NTZ(last_contact)                  AS dernier_contact_utc,
        ingested_at,
        batch_id,

        --  Position GPS 
        ROUND(latitude::FLOAT, 6)                       AS latitude,
        ROUND(longitude::FLOAT, 6)                      AS longitude,

        --  Altitude 
        -- On garde les deux (baro = standard ATC, geo = GPS)
        ROUND(baro_altitude::FLOAT, 1)                  AS altitude_baro_m,
        ROUND(geo_altitude::FLOAT, 1)                   AS altitude_geo_m,
        -- Conversion en pieds (standard aviation mondiale)
        ROUND(baro_altitude::FLOAT * 3.28084, 0)        AS altitude_baro_ft,
        ROUND(geo_altitude::FLOAT * 3.28084, 0)         AS altitude_geo_ft,

        --  Cinématique 
        ROUND(vitesse::FLOAT, 1)                        AS vitesse_ms,
        -- Conversion m/s → km/h
        ROUND(vitesse::FLOAT * 3.6, 1)                  AS vitesse_kmh,
        ROUND(cap::FLOAT, 1)                            AS cap_degres,
        ROUND(taux_montee::FLOAT, 1)                    AS taux_montee_ms,

        --  Statut 
        au_sol::BOOLEAN                                 AS au_sol,
        squawk,
        source_position::INTEGER                        AS source_capteur,

        --  Flags qualité 
        -- Ces flags permettent de filtrer les données suspectes dans les marts
        CASE
            WHEN baro_altitude < 0                      THEN TRUE
            WHEN baro_altitude > 45000                  THEN TRUE  -- ~13700m = plafond max
            ELSE FALSE
        END                                             AS flag_altitude_suspecte,

        CASE
            WHEN vitesse < 0                            THEN TRUE
            WHEN vitesse > 340                          THEN TRUE  -- ~Mach 1 en m/s
            ELSE FALSE
        END                                             AS flag_vitesse_suspecte,

        CASE
            WHEN latitude IS NULL OR longitude IS NULL  THEN TRUE
            WHEN latitude NOT BETWEEN -90 AND 90        THEN TRUE
            WHEN longitude NOT BETWEEN -180 AND 180     THEN TRUE
            ELSE FALSE
        END                                             AS flag_coords_invalides

    FROM source

    -- Filtre basique : on veut des données avec au minimum une position GPS
    WHERE latitude IS NOT NULL
      AND longitude IS NOT NULL
      AND icao24 IS NOT NULL

)

SELECT * FROM cleaned

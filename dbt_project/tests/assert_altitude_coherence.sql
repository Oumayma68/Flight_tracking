--  détecte les changements d'altitude physiquement impossibles
-- Un avion commercial ne peut pas changer d'altitude de plus de 100 m/s
-- (soit 20 000 ft/min, ce qui est déjà une urgence absolue).
-- Si ce test retourne des lignes, c'est une anomalie de données.
-- Ce test retourne 0 ligne si tout est OK (convention dbt).
{{ config(severity='warn') }}

WITH consecutive_positions AS (

    SELECT
        fp.position_sk,
        fp.vol_sk,
        fp.timestamp_utc,
        fp.altitude_baro_m,
        fp.latitude,
        fp.longitude,

        -- Position précédente du même avion
        LAG(fp.altitude_baro_m) OVER (
            PARTITION BY fp.vol_sk
            ORDER BY fp.timestamp_utc
        )                                           AS altitude_precedente_m,

        LAG(fp.timestamp_utc) OVER (
            PARTITION BY fp.vol_sk
            ORDER BY fp.timestamp_utc
        )                                           AS timestamp_precedent

    FROM {{ ref('fact_positions') }} fp
    WHERE fp.au_sol = FALSE  -- On ne teste que les avions en vol

),

anomalies AS (

    SELECT
        position_sk,
        vol_sk,
        timestamp_utc,
        altitude_baro_m,
        altitude_precedente_m,
        timestamp_precedent,

        -- Delta altitude en mètres
        ABS(altitude_baro_m - altitude_precedente_m)    AS delta_altitude_m,

        -- Delta temps en secondes
        DATEDIFF('second', timestamp_precedent, timestamp_utc)
                                                        AS delta_secondes,

        -- Taux de changement réel (m/s)
        CASE
            WHEN DATEDIFF('second', timestamp_precedent, timestamp_utc) > 0
            THEN ABS(altitude_baro_m - altitude_precedente_m)
                 / DATEDIFF('second', timestamp_precedent, timestamp_utc)
            ELSE NULL
        END                                             AS taux_changement_ms

    FROM consecutive_positions

    -- On ne teste que quand on a deux positions proches (< 2 minutes)
    WHERE altitude_precedente_m IS NOT NULL
      AND timestamp_precedent IS NOT NULL
      AND DATEDIFF('second', timestamp_precedent, timestamp_utc) BETWEEN 1 AND 120

)

-- Retourner UNIQUEMENT les anomalies (test échoue si > 0 lignes)
SELECT *
FROM anomalies
WHERE taux_changement_ms > 100  -- Plus de 100 m/s de variation = impossible

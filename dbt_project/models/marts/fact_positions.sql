-- Table de faits : chaque ligne = une position d'avion dédupliquée
-- C'est la table centrale du Star Schema.
--
-- Granularité : 1 ligne par avion par fenêtre de 30 secondes

{{
    config(
        materialized='incremental',
        unique_key='position_sk',
        on_schema_change='sync_all_columns',
        cluster_by=['vol_sk', "timestamp_utc::DATE"]
    )
}}

WITH positions AS (

    SELECT * FROM {{ ref('int_positions_deduped') }}

    {% if is_incremental() %}
    WHERE ingested_at >= DATEADD(
        hour,
        -{{ var('incremental_lookback_hours', 2) }},
        CURRENT_TIMESTAMP()
    )
    {% endif %}

),

-- Join avec dim_vols pour récupérer la surrogate key
dim_vols AS (

    SELECT vol_sk, icao24
    FROM {{ ref('dim_vols') }}

),

final AS (

    SELECT
        --  Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['p.position_id']) }}
                                                    AS position_sk,

        --  Clés étrangères (Star Schema) 
        dv.vol_sk,

        --  Dimension Temps (dégradée dans la fact) 
        p.timestamp_position_utc                    AS timestamp_utc,
        DATE_TRUNC('hour', p.timestamp_position_utc)
                                                    AS heure_utc,
        DATE(p.timestamp_position_utc)              AS date_utc,
        EXTRACT(HOUR FROM p.timestamp_position_utc) AS heure_journee,
        DAYOFWEEK(p.timestamp_position_utc)         AS jour_semaine,

        --  Position GPS 
        p.latitude,
        p.longitude,

        --  Altitude 
        p.altitude_baro_m,
        p.altitude_baro_ft,

        -- Tranche de vol (FL = Flight Level)
        CASE
            WHEN p.au_sol = TRUE                    THEN 'SOL'
            WHEN p.altitude_baro_ft < 10000         THEN 'BASSE_ALTITUDE'   -- < FL100
            WHEN p.altitude_baro_ft < 18000         THEN 'TRANSITION'       -- FL100-FL180
            WHEN p.altitude_baro_ft < 29000         THEN 'CROISIERE_BASSE'  -- FL180-FL290
            WHEN p.altitude_baro_ft < 41000         THEN 'CROISIERE_HAUTE'  -- FL290-FL410
            ELSE 'TRES_HAUTE_ALTITUDE'              -- > FL410
        END                                         AS tranche_vol,

        --  Cinématique 
        p.vitesse_ms,
        p.vitesse_kmh,
        p.cap_degres,
        p.taux_montee_declare_ms,
        p.taux_montee_calcule_ms,

        -- Phase de vol estimée
        CASE
            WHEN p.au_sol = TRUE                        THEN 'SOL'
            WHEN p.altitude_baro_ft < 1500
                AND p.taux_montee_calcule_ms > 2        THEN 'DECOLLAGE'
            WHEN p.altitude_baro_ft < 1500
                AND p.taux_montee_calcule_ms < -2       THEN 'ATTERRISSAGE'
            WHEN p.altitude_baro_ft < 10000
                AND p.taux_montee_calcule_ms > 2        THEN 'MONTEE'
            WHEN p.altitude_baro_ft < 10000
                AND p.taux_montee_calcule_ms < -2       THEN 'DESCENTE'
            WHEN p.altitude_baro_ft >= 25000
                AND ABS(COALESCE(p.taux_montee_calcule_ms, 0)) <= 2
                                                        THEN 'CROISIERE'
            ELSE 'TRANSITION'
        END                                         AS phase_vol,

        -- Statut & Qualité
        p.au_sol,
        p.squawk,
        p.source_capteur,
        p.flag_changement_altitude_suspect,

        --  Métadonnées 
        p.ingested_at,
        p.batch_id,
        CURRENT_TIMESTAMP()                         AS dbt_updated_at

    FROM positions p
    LEFT JOIN dim_vols dv ON p.icao24 = dv.icao24

    -- Exclure les positions sans correspondance dans dim_vols (ne devrait pas arriver)
    WHERE dv.vol_sk IS NOT NULL

)

SELECT * FROM final

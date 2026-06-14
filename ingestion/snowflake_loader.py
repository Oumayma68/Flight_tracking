"""
Charge les positions de vol dans Snowflake via MERGE INTO (déduplication).

"""
import os
from typing import Optional
import snowflake.connector
from snowflake.connector import DictCursor

# Lecture Airflow Variables 
try:
    from airflow.models import Variable
    def get_var(key: str, default: str = "") -> str:
        return Variable.get(key, default_var=default)
except ImportError:
    import os
    def get_var(key: str, default: str = "") -> str:
        return os.getenv(key, default)


class SnowflakeLoader:
    """
    Gère les insertions Snowflake avec déduplication via MERGE INTO.
    
    """

    def __init__(self):
        #  Tous les credentials depuis Airflow Variables
         self.config = {
             "account":   os.environ.get("SNOWFLAKE_ACCOUNT"),
    	     "user":      os.environ.get("SNOWFLAKE_USER"),
    	     "password":  os.environ.get("SNOWFLAKE_PASSWORD"),
    	     "database":  os.environ.get("SNOWFLAKE_DATABASE",  "FLIGHT_TRACKING"),
    	     "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    	     "schema":    os.environ.get("SNOWFLAKE_SCHEMA",    "RAW"),
    	     "role":      os.environ.get("SNOWFLAKE_ROLE",      "ACCOUNTADMIN"),
         }
         self._conn = None

    # Gestion connexion 

    def connect(self) -> None:
        try:
            self._conn = snowflake.connector.connect(**self.config)
            print(
                f"✅ Connecté à Snowflake : "
                f"{self.config['database']}.{self.config['schema']}"
            )
        except Exception as e:
            print(f"Connexion Snowflake échouée : {e}")
            raise

    def disconnect(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    #  Insertion principale 

    def upsert_positions(self, positions: list[dict]) -> dict:
        """
        Insère les positions via MERGE INTO pour garantir zéro doublon.

        Stratégie :
            1. INSERT dans une table TEMP (session Snowflake)
            2. MERGE INTO la table finale depuis le TEMP
            → Seules les nouvelles positions (id inconnu) sont insérées

        Returns:
            dict {"inserted": int, "skipped": int, "total": int}
        """
        if not positions:
            print("Aucune position à insérer")
            return {"inserted": 0, "skipped": 0, "total": 0}

        if not self._conn:
            raise RuntimeError("Pas de connexion. Utilise le context manager `with SnowflakeLoader()`.")

        cursor = self._conn.cursor(DictCursor)

        try:
            #  Étape 1 : Table temporaire (durée de session) 
            cursor.execute("""
                CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_FLIGHT_POSITIONS (
                    id              VARCHAR(64),
                    icao24          VARCHAR(10),
                    callsign        VARCHAR(20),
                    origine_pays    VARCHAR(100),
                    time_position   BIGINT,
                    last_contact    BIGINT,
                    longitude       FLOAT,
                    latitude        FLOAT,
                    geo_altitude    FLOAT,
                    baro_altitude   FLOAT,
                    au_sol          BOOLEAN,
                    vitesse         FLOAT,
                    cap             FLOAT,
                    taux_montee     FLOAT,
                    squawk          VARCHAR(10),
                    source_position INTEGER,
                    ingested_at     TIMESTAMP_NTZ,
                    batch_id        VARCHAR(64)
                )
            """)
            cursor.execute("TRUNCATE TABLE TEMP_FLIGHT_POSITIONS")

            #  Étape 2 : Remplir la table temporaire 
            cursor.executemany("""
                INSERT INTO TEMP_FLIGHT_POSITIONS VALUES (
                    %(id)s, %(icao24)s, %(callsign)s, %(origine_pays)s,
                    %(time_position)s, %(last_contact)s, %(longitude)s,
                    %(latitude)s, %(geo_altitude)s, %(baro_altitude)s,
                    %(au_sol)s, %(vitesse)s, %(cap)s, %(taux_montee)s,
                    %(squawk)s, %(source_position)s, %(ingested_at)s,
                    %(batch_id)s
                )
            """, [self._normalize(p) for p in positions])

            #  Étape 3 : MERGE INTO — cœur de la déduplication 
            cursor.execute("""
                MERGE INTO RAW.FLIGHT_POSITIONS AS target
                USING TEMP_FLIGHT_POSITIONS AS source
                ON target.id = source.id
                WHEN NOT MATCHED THEN INSERT (
                    id, icao24, callsign, origine_pays,
                    time_position, last_contact, longitude, latitude,
                    geo_altitude, baro_altitude, au_sol, vitesse, cap,
                    taux_montee, squawk, source_position, ingested_at, batch_id
                ) VALUES (
                    source.id, source.icao24, source.callsign, source.origine_pays,
                    source.time_position, source.last_contact, source.longitude,
                    source.latitude, source.geo_altitude, source.baro_altitude,
                    source.au_sol, source.vitesse, source.cap, source.taux_montee,
                    source.squawk, source.source_position, source.ingested_at,
                    source.batch_id
                )
            """)

            row = cursor.fetchone()
            inserted = row.get("number of rows inserted", 0) if row else 0
            skipped  = len(positions) - inserted

            print(
                f" MERGE terminé | "
                f"Insérés : {inserted} | "
                f"Doublons ignorés : {skipped} | "
                f"Total batch : {len(positions)}"
            )
            return {"inserted": inserted, "skipped": skipped, "total": len(positions)}

        except Exception as e:
            print(f"Erreur MERGE Snowflake : {e}")
            raise
        finally:
            cursor.close()

    #  Utilitaires 

    def get_last_ingestion_stats(self) -> dict:
        """Stats de la dernière heure d'ingestion."""
        cursor = self._conn.cursor(DictCursor)
        try:
            cursor.execute("""
                SELECT
                    COUNT(*)                AS total_positions,
                    COUNT(DISTINCT icao24)  AS avions_uniques,
                    COUNT(DISTINCT batch_id) AS nombre_batches,
                    MAX(ingested_at)        AS dernier_batch
                FROM RAW.FLIGHT_POSITIONS
                WHERE ingested_at >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
            """)
            return cursor.fetchone() or {}
        finally:
            cursor.close()

    @staticmethod
    def _normalize(position: dict) -> dict:
        """Normalise un dict de position pour le binding Snowflake."""
        return {
            "id":               position.get("id"),
            "icao24":           position.get("icao24"),
            "callsign":         position.get("callsign"),
            "origine_pays":     position.get("origine_pays"),
            "time_position":    position.get("time_position"),
            "last_contact":     position.get("last_contact"),
            "longitude":        position.get("longitude"),
            "latitude":         position.get("latitude"),
            "geo_altitude":     position.get("geo_altitude"),
            "baro_altitude":    position.get("baro_altitude"),
            "au_sol":           position.get("au_sol", False),
            "vitesse":          position.get("vitesse"),
            "cap":              position.get("cap"),
            "taux_montee":      position.get("taux_montee"),
            "squawk":           position.get("squawk"),
            "source_position":  position.get("source_position"),
            "ingested_at":      position.get("ingested_at"),
            "batch_id":         position.get("batch_id"),
        }

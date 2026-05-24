"""
Client pour l'API OpenSky Network — authentification OAuth2.
Utilise client_id + client_secret pour obtenir un access_token,
puis appelle l'API avec ce token.

"""

import os
import hashlib
import requests
from datetime import datetime, timedelta, timezone
from typing import Optional
import time
# Bounding Box par défaut : France métropolitaine 
DEFAULT_BBOX = {
    "lat_min": float(os.getenv("BBOX_LAT_MIN", 41.0)),
    "lat_max": float(os.getenv("BBOX_LAT_MAX", 51.5)),
    "lon_min": float(os.getenv("BBOX_LON_MIN", -5.5)),
    "lon_max": float(os.getenv("BBOX_LON_MAX", 10.0)),
}

# Mapping des colonnes retournées par l'API
OPENSKY_COLUMNS = [
    "icao24",           # 0
    "callsign",         # 1
    "origine_pays",     # 2
    "time_position",    # 3
    "last_contact",     # 4
    "longitude",        # 5
    "latitude",         # 6
    "baro_altitude",    # 7
    "au_sol",           # 8
    "vitesse",          # 9
    "cap",              # 10
    "taux_montee",      # 11
    "sensors",          # 12  
    "geo_altitude",     # 13  
    "squawk",           # 14  
    "spi",              # 15  
    "source_position",  # 16
]

# URL du token OAuth2 OpenSky
OPENSKY_TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
OPENSKY_API_URL   = "https://opensky-network.org/api/states/all"


class OpenSkyClient:
    """
    Client OpenSky Network avec authentification OAuth2.

    Flux OAuth2 :
        1. POST /auth/token avec client_id + client_secret
        2. Récupère access_token (valide ~1h)
        3. Appelle /states/all avec Authorization: Bearer <token>
        4. Renouvelle le token automatiquement si expiré

    Usage :
        client = OpenSkyClient()
        positions = client.get_states_france()
    """

    def __init__(self,client_id: Optional[str] = None, client_secret: Optional[str] = None,):
        self.client_id     = client_id     or os.getenv("OPENSKY_CLIENT_ID",     "")
        self.client_secret = client_secret or os.getenv("OPENSKY_CLIENT_SECRET", "")

        if not self.client_id or not self.client_secret:
            raise ValueError(
                "OPENSKY_CLIENT_ID et OPENSKY_CLIENT_SECRET sont requis. "
                "Définis-les dans les Airflow Variables (FT_OPENSKY_CLIENT_ID / FT_OPENSKY_CLIENT_SECRET)."
            )

        self.session      = requests.Session()
        self._token       = None
        self._token_expiry = 0  # timestamp UNIX d'expiration

    # Authentification OAuth2 

    def _get_token(self) -> str:
        """
        Récupère un access_token OAuth2.
        Utilise le token en cache s'il est encore valide (marge de 60s).
        """
        now = time.time()

        # Token encore valide → réutiliser
        if self._token and now < self._token_expiry - 60:
            return self._token

        print("🔑 Renouvellement du token OAuth2 OpenSky...")

        try:
            response = self.session.post(
                OPENSKY_TOKEN_URL,
                data={
                    "grant_type":    "client_credentials",
                    "client_id":     self.client_id,
                    "client_secret": self.client_secret,
                },
                timeout=10,
            )
            response.raise_for_status()
            token_data = response.json()

            self._token        = token_data["access_token"]
            expires_in         = token_data.get("expires_in", 3600)
            self._token_expiry = now + expires_in

            print(f"✅ Token OAuth2 obtenu — expire dans {expires_in}s")
            return self._token

        except requests.exceptions.HTTPError as e:
            raise RuntimeError(f"Échec authentification OpenSky OAuth2 : {e}") from e

    #  Récupération des états 

    def get_states(self, bbox: Optional[dict] = None, max_retries: int = 3, retry_delay: float = 5.0) -> list[dict]:
        """
        Récupère les positions ADS-B dans une bounding box.

        Args:
            bbox        : dict lat_min/lat_max/lon_min/lon_max (défaut = France)
            max_retries : tentatives en cas d'erreur réseau
            retry_delay : secondes entre les tentatives

        Returns:
            Liste de dicts — une entrée par avion détecté
        """
        bbox       = bbox or DEFAULT_BBOX
        batch_id   = self._generate_batch_id()
        ingested_at = datetime.now(timezone.utc).isoformat()

        params = {
            "lamin": bbox["lat_min"],
            "lamax": bbox["lat_max"],
            "lomin": bbox["lon_min"],
            "lomax": bbox["lon_max"],
        }

        data = None
        for attempt in range(1, max_retries + 1):
            try:
                token = self._get_token()
                response = self.session.get(
                    OPENSKY_API_URL,
                    params=params,
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=15,
                )

                # Token expiré → forcer le renouvellement et réessayer
                if response.status_code == 401:
                    print("Token expiré — renouvellement forcé")
                    self._token = None
                    continue

                # Rate limit
                if response.status_code == 429:
                    print(f"Rate limit OpenSky — attente 60s")
                    time.sleep(60)
                    continue

                response.raise_for_status()
                data = response.json()
                break

            except requests.exceptions.Timeout:
                print(f"Timeout OpenSky — tentative {attempt}/{max_retries}")
                time.sleep(retry_delay * attempt)

            except requests.exceptions.ConnectionError:
                print(f"Connexion impossible — tentative {attempt}/{max_retries}")
                time.sleep(retry_delay * attempt)

            except RuntimeError as e:
                # Erreur OAuth2 → inutile de réessayer
                print(f"Erreur auth : {e}")
                return []

        if not data or not data.get("states"):
            print("Aucun état de vol reçu (zone vide ou API indisponible)")
            return []

        positions = [
            p for s in data["states"]
            if (p := self._parse_state(s, batch_id, ingested_at))
        ]

        print(f" {len(positions)} avions récupérés | batch={batch_id[:8]}")
        return positions

    def get_states_france(self) -> list[dict]:
        """Raccourci : vols au-dessus de la France métropolitaine."""
        return self.get_states(DEFAULT_BBOX)

    #  Parsing 

    def _parse_state(self, state: list, batch_id: str, ingested_at: str) -> Optional[dict]:
        """Parse un état brut en dict — filtre les positions sans coords GPS."""
        try:
            parsed = {
                col: state[i] if i < len(state) else None
                for i, col in enumerate(OPENSKY_COLUMNS)
            }

            # Sans position GPS → inutilisable
            if parsed["latitude"] is None or parsed["longitude"] is None:
                return None

            if parsed["callsign"]:
                parsed["callsign"] = parsed["callsign"].strip()

            parsed["id"] = self._generate_position_id(
                parsed["icao24"],
                parsed["time_position"],
                parsed["latitude"],
                parsed["longitude"],
            )
            parsed["ingested_at"] = ingested_at
            parsed["batch_id"]    = batch_id

            # Colonnes inutiles
            parsed.pop("spi", None)
            parsed.pop("last_contact2", None)

            return parsed

        except (IndexError, TypeError):
            return None

    #  Utilitaires 

    @staticmethod
    def _generate_position_id(icao24, time_position, latitude, longitude) -> str:
        """Hash SHA256 unique par position — clé de déduplication."""
        key = f"{icao24}_{time_position}_{round(latitude, 4)}_{round(longitude, 4)}"
        return hashlib.sha256(key.encode()).hexdigest()

    @staticmethod
    def _generate_batch_id() -> str:
        """Identifiant unique pour le batch d'ingestion."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        return hashlib.md5(ts.encode()).hexdigest()

    def close(self):
        """Ferme la session HTTP."""
        self.session.close()

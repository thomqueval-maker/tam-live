"""Carte locale des bus et trams de Montpellier à partir des flux open data."""

from __future__ import annotations

import csv
import io
import json
import os
import threading
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import requests
from flask import Flask, jsonify, render_template
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2


VEHICLE_POSITIONS_URL = os.getenv(
    "TAM_VEHICLE_POSITIONS_URL",
    "https://gtfsproxy.e-tam.fr/COMMON/VehiclePosition.pb",
)
TRAM_LINES_FILE = Path(os.getenv(
    "TAM_TRAM_LINES_FILE",
    str(Path(__file__).resolve().parent / "data" / "MMM_MMM_LigneTram.json"),
)).resolve()
BUS_LINES_FILE = Path(os.getenv(
    "TAM_BUS_LINES_FILE",
    str(Path(__file__).resolve().parent / "data" / "MMM_MMM_BusLigne.json"),
)).resolve()
BUSTRAM_LINES_FILE = Path(os.getenv(
    "TAM_BUSTRAM_LINES_FILE",
    str(Path(__file__).resolve().parent / "data" / "MMM_MMM_Bustram.json"),
)).resolve()
GTFS_URL = os.getenv(
    "TAM_GTFS_URL",
    "https://gtfsproxy.e-tam.fr/COMMON/GTFS.zip",
)
VEHICLE_CACHE_SECONDS = 5
LINES_CACHE_SECONDS = 60 * 60
GTFS_CACHE_SECONDS = 6 * 60 * 60

app = Flask(__name__)
http = requests.Session()
http.headers.update({"User-Agent": "TAM-Tram-Live/1.0 (local open-data map)"})

_lines_lock = threading.Lock()
_bus_lines_lock = threading.Lock()
_bustram_lines_lock = threading.Lock()
_vehicle_lock = threading.Lock()
_gtfs_lock = threading.Lock()
_vehicle_cache: tuple[float, dict] | None = None
_lines_cache: tuple[float, dict] | None = None
_bus_lines_cache: tuple[float, dict] | None = None
_bustram_lines_cache: tuple[float, dict] | None = None
_gtfs_cache: tuple[float, dict] | None = None


def _fetch_lines() -> dict:
    """Load and cache the tram line GeoJSON shipped with the app."""
    global _lines_cache
    now = time.monotonic()
    with _lines_lock:
        if _lines_cache and now - _lines_cache[0] < LINES_CACHE_SECONDS:
            return _lines_cache[1]

        with TRAM_LINES_FILE.open("r", encoding="utf-8-sig") as source:
            geojson = json.load(source)
        if not isinstance(geojson, dict) or geojson.get("type") != "FeatureCollection" or not isinstance(
            geojson.get("features"), list
        ):
            raise ValueError("Le fichier des lignes n'est pas un GeoJSON FeatureCollection valide.")
        _lines_cache = (time.monotonic(), geojson)
        return geojson


def _fetch_bus_lines() -> dict:
    """Load and cache the bus line GeoJSON shipped with the app."""
    global _bus_lines_cache
    now = time.monotonic()
    with _bus_lines_lock:
        if _bus_lines_cache and now - _bus_lines_cache[0] < LINES_CACHE_SECONDS:
            return _bus_lines_cache[1]

        with BUS_LINES_FILE.open("r", encoding="utf-8-sig") as source:
            geojson = json.load(source)
        if not isinstance(geojson, dict) or geojson.get("type") != "FeatureCollection" or not isinstance(
            geojson.get("features"), list
        ):
            raise ValueError("Le fichier des lignes de bus n'est pas un GeoJSON FeatureCollection valide.")
        _bus_lines_cache = (time.monotonic(), geojson)
        return geojson


def _fetch_bustram_lines() -> dict:
    """Load and cache the Bustram A GeoJSON shipped with the app."""
    global _bustram_lines_cache
    now = time.monotonic()
    with _bustram_lines_lock:
        if _bustram_lines_cache and now - _bustram_lines_cache[0] < LINES_CACHE_SECONDS:
            return _bustram_lines_cache[1]

        with BUSTRAM_LINES_FILE.open("r", encoding="utf-8-sig") as source:
            geojson = json.load(source)
        if not isinstance(geojson, dict) or geojson.get("type") != "FeatureCollection" or not isinstance(
            geojson.get("features"), list
        ):
            raise ValueError("Le fichier du Bustram A n'est pas un GeoJSON FeatureCollection valide.")
        _bustram_lines_cache = (time.monotonic(), geojson)
        return geojson


def _gtfs_mode(route_type: int) -> str:
    if route_type == 0 or 900 <= route_type < 1000:
        return "tramway"
    if route_type == 3 or 700 <= route_type < 800:
        return "bus"
    return "autre"


def _fetch_gtfs_data() -> dict:
    """Download and cache line colors plus trip destinations from the static GTFS."""
    global _gtfs_cache
    now = time.monotonic()
    with _gtfs_lock:
        if _gtfs_cache and now - _gtfs_cache[0] < GTFS_CACHE_SECONDS:
            return _gtfs_cache[1]

        response = http.get(GTFS_URL, timeout=(5, 30))
        response.raise_for_status()
        try:
            archive = zipfile.ZipFile(io.BytesIO(response.content))
        except zipfile.BadZipFile as exc:
            raise ValueError("Le fichier GTFS reçu n'est pas une archive ZIP valide.") from exc

        members = {name.rsplit("/", 1)[-1]: name for name in archive.namelist()}
        if "routes.txt" not in members or "trips.txt" not in members:
            raise ValueError("Le GTFS ne contient pas routes.txt et trips.txt.")

        routes_by_id: dict[str, dict] = {}
        with archive.open(members["routes.txt"]) as binary, io.TextIOWrapper(
            binary, encoding="utf-8-sig", newline=""
        ) as text:
            for row in csv.DictReader(text):
                route_id = row.get("route_id", "")
                if not route_id:
                    continue
                try:
                    route_type = int(row.get("route_type", "-1"))
                except (TypeError, ValueError):
                    route_type = -1
                color = row.get("route_color", "").strip().lstrip("#")
                text_color = row.get("route_text_color", "").strip().lstrip("#")
                routes_by_id[route_id] = {
                    "route_id": route_id,
                    "short_name": row.get("route_short_name", "").strip(),
                    "long_name": row.get("route_long_name", "").strip(),
                    "route_type": route_type,
                    "mode": _gtfs_mode(route_type),
                    "color": f"#{color}" if len(color) == 6 and all(c in "0123456789abcdefABCDEF" for c in color) else "",
                    "text_color": f"#{text_color}" if len(text_color) == 6 and all(c in "0123456789abcdefABCDEF" for c in text_color) else "",
                }

        trips_by_id: dict[str, dict] = {}
        with archive.open(members["trips.txt"]) as binary, io.TextIOWrapper(
            binary, encoding="utf-8-sig", newline=""
        ) as text:
            for row in csv.DictReader(text):
                trip_id = row.get("trip_id", "")
                if trip_id:
                    trips_by_id[trip_id] = {
                        "route_id": row.get("route_id", ""),
                        "headsign": row.get("trip_headsign", "").strip(),
                    }

        data = {"routes_by_id": routes_by_id, "trips_by_id": trips_by_id}
        _gtfs_cache = (time.monotonic(), data)
        return data


def _cached_gtfs_data() -> dict | None:
    with _gtfs_lock:
        if _gtfs_cache and time.monotonic() - _gtfs_cache[0] < GTFS_CACHE_SECONDS:
            return _gtfs_cache[1]
    return None


def _vehicle_to_dict(entity, gtfs_data: dict | None = None) -> dict | None:
    if not entity.HasField("vehicle"):
        return None

    vehicle = entity.vehicle
    if not vehicle.HasField("position"):
        return None

    position = vehicle.position
    latitude = position.latitude
    longitude = position.longitude
    if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
        return None

    trip = vehicle.trip if vehicle.HasField("trip") else None
    vehicle_details = vehicle.vehicle if vehicle.HasField("vehicle") else None
    timestamp = int(vehicle.timestamp) if vehicle.HasField("timestamp") else None
    current_status = (
        int(vehicle.current_status) if vehicle.HasField("current_status") else None
    )

    trip_id = trip.trip_id if trip else ""
    trip_info = (gtfs_data or {}).get("trips_by_id", {}).get(trip_id, {})
    route_id = (trip.route_id if trip and trip.route_id else "") or trip_info.get("route_id", "")
    route_info = (gtfs_data or {}).get("routes_by_id", {}).get(route_id, {})

    return {
        "id": (vehicle_details.id if vehicle_details and vehicle_details.id else entity.id),
        "label": vehicle_details.label if vehicle_details else "",
        "route_id": route_id,
        "trip_id": trip_id,
        "route_short_name": route_info.get("short_name", ""),
        "route_long_name": route_info.get("long_name", ""),
        "route_type": route_info.get("route_type"),
        "route_mode": route_info.get("mode", ""),
        "route_color": route_info.get("color", ""),
        "route_text_color": route_info.get("text_color", ""),
        "direction": trip_info.get("headsign", ""),
        "latitude": latitude,
        "longitude": longitude,
        "bearing": int(position.bearing) if position.HasField("bearing") else None,
        "timestamp": timestamp,
        "current_status": current_status,
        "stop_id": vehicle.stop_id if vehicle.HasField("stop_id") else "",
    }


def _fetch_vehicles() -> dict:
    """Download and decode one GTFS-Realtime VehiclePositions feed."""
    global _vehicle_cache
    now = time.monotonic()
    with _vehicle_lock:
        if _vehicle_cache and now - _vehicle_cache[0] < VEHICLE_CACHE_SECONDS:
            return _vehicle_cache[1]

        response = http.get(VEHICLE_POSITIONS_URL, timeout=12)
        response.raise_for_status()
        feed = gtfs_realtime_pb2.FeedMessage()
        try:
            feed.ParseFromString(response.content)
        except DecodeError as exc:
            raise ValueError("Le flux reçu n'est pas un message GTFS-Realtime valide.") from exc

        gtfs_data = _cached_gtfs_data()
        vehicles = [
            item
            for entity in feed.entity
            if (item := _vehicle_to_dict(entity, gtfs_data)) is not None
        ]
        fetched_at = datetime.now(timezone.utc).isoformat()
        feed_timestamp = (
            int(feed.header.timestamp) if feed.header.HasField("timestamp") else None
        )
        payload = {
            "vehicles": vehicles,
            "count": len(vehicles),
            "fetched_at": fetched_at,
            "feed_timestamp": feed_timestamp,
        }
        _vehicle_cache = (time.monotonic(), payload)
        return payload


@app.get("/")
def index():
    return render_template("index.html")


@app.get("/api/lines")
def lines():
    try:
        return jsonify(_fetch_lines())
    except (OSError, ValueError) as exc:
        app.logger.warning("Impossible de charger les lignes du tram : %s", exc)
        return jsonify(error="Impossible de charger le tracé des lignes. Vérifiez votre connexion."), 502


@app.get("/api/bus-lines")
def bus_lines():
    try:
        return jsonify(_fetch_bus_lines())
    except (OSError, ValueError) as exc:
        app.logger.warning("Impossible de charger les lignes de bus : %s", exc)
        return jsonify(error="Impossible de charger le tracé des lignes de bus."), 502


@app.get("/api/bustram-lines")
def bustram_lines():
    try:
        return jsonify(_fetch_bustram_lines())
    except (OSError, ValueError) as exc:
        app.logger.warning("Impossible de charger le Bustram A : %s", exc)
        return jsonify(error="Impossible de charger le tracé du Bustram A."), 502


@app.get("/api/routes")
def routes():
    try:
        data = _fetch_gtfs_data()
        route_list = [
            route for route in data["routes_by_id"].values()
            if route["mode"] in {"tramway", "bus"}
        ]
        route_list.sort(key=lambda route: (route["mode"] != "tramway", route["short_name"], route["long_name"]))
        return jsonify(routes=route_list)
    except (requests.RequestException, ValueError, zipfile.BadZipFile) as exc:
        app.logger.warning("Impossible de charger les lignes GTFS : %s", exc)
        return jsonify(error="Impossible de charger les lignes et directions du réseau."), 502


@app.get("/api/vehicles")
def vehicles():
    try:
        return jsonify(_fetch_vehicles())
    except requests.RequestException as exc:
        app.logger.warning("Impossible de lire le flux de positions TAM : %s", exc)
        return jsonify(error="Le flux de positions TAM est momentanément inaccessible."), 502
    except ValueError as exc:
        app.logger.warning("Flux de positions TAM invalide : %s", exc)
        return jsonify(error="Le flux reçu n'est pas lisible pour le moment."), 502


if __name__ == "__main__":
    # Local access only: this is a small personal dashboard, not a public server.
    app.run(host="127.0.0.1", port=int(os.getenv("PORT", "5000")), debug=False)

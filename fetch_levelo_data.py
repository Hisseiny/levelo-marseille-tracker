#!/usr/bin/env python3
"""Collect Le Velo Marseille station data from GBFS API, save to Supabase and JSON."""

import json
import os
import sys
from datetime import datetime

import requests
from supabase import create_client, Client

BASE_URL = "https://gbfs.omega.fifteen.eu/gbfs/2.2/marseille/en"
STATION_STATUS_URL = f"{BASE_URL}/station_status.json"
STATION_INFO_URL = f"{BASE_URL}/station_information.json"

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("Error: SUPABASE_URL and SUPABASE_KEY environment variables required")
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_api_data():
    """Fetch station status and info from GBFS API."""
    try:
        status_resp = requests.get(STATION_STATUS_URL, timeout=10)
        status_resp.raise_for_status()
        status_data = status_resp.json()["data"]["stations"]

        info_resp = requests.get(STATION_INFO_URL, timeout=10)
        info_resp.raise_for_status()
        info_data = info_resp.json()["data"]["stations"]

        print(f"API: {len(status_data)} stations status, {len(info_data)} stations info")
        return status_data, info_data

    except requests.exceptions.RequestException as e:
        print(f"API error: {e}")
        return None, None


def process_data(status_data, info_data):
    """Merge status + info, compute availability metrics."""
    info_map = {s["station_id"]: s for s in info_data}
    records = []

    for status in status_data:
        sid = status["station_id"]
        info = info_map.get(sid, {})

        bikes = status.get("num_bikes_available", 0)
        stands = status.get("num_docks_available", 0)
        capacity = info.get("capacity", 1)
        rate = round(bikes / capacity * 100, 1) if capacity > 0 else 0

        if bikes == 0 or stands == 0 or rate < 15:
            display_status = "critical"
        elif rate < 40:
            display_status = "warning"
        elif rate > 70:
            display_status = "excellent"
        else:
            display_status = "good"

        records.append({
            "station_id": sid,
            "station_name": info.get("name", "Unknown"),
            "address": info.get("address", ""),
            "latitude": info.get("lat", 0),
            "longitude": info.get("lon", 0),
            "available_bikes": bikes,
            "available_stands": stands,
            "total_capacity": capacity,
            "status": status.get("status", "unknown"),
            "display_status": display_status,
            "availability_rate": rate,
            "last_update": datetime.now().isoformat(),
        })

    print(f"Processed {len(records)} stations")
    return records


def save_to_supabase(data):
    """Upsert metadata + insert observations into Supabase."""
    saved = 0
    errors = 0

    for record in data:
        sid = record["station_id"]
        lat = record["latitude"]
        zone = "Nord Marseille" if lat >= 43.30 else "Centre Marseille" if lat >= 43.28 else "Sud Marseille"

        try:
            supabase.table("stations_metadata").upsert({
                "station_id": sid,
                "station_name": record["station_name"],
                "address": record["address"],
                "latitude": record["latitude"],
                "longitude": record["longitude"],
                "total_capacity": record["total_capacity"],
                "zone": zone,
                "updated_at": datetime.now().isoformat(),
            }, on_conflict="station_id").execute()

            supabase.table("levelo_observations").insert({
                "station_id": sid,
                "available_bikes": record["available_bikes"],
                "available_stands": record["available_stands"],
                "status": record["status"],
            }).execute()

            saved += 1
        except Exception as e:
            print(f"  Error station {sid}: {e}")
            errors += 1

    print(f"Supabase: {saved} saved, {errors} errors")
    return saved > 0


def export_json(data):
    """Export station data to JSON for the dashboard."""
    os.makedirs("data", exist_ok=True)
    path = "data/levelo_data.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"JSON exported: {path} ({len(data)} stations)")


def main():
    print(f"Le Velo data collection - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    status_data, info_data = fetch_api_data()
    if not status_data or not info_data:
        sys.exit(1)

    records = process_data(status_data, info_data)
    if not records:
        print("No data to process")
        sys.exit(1)

    save_to_supabase(records)
    export_json(records)
    print("Done")


if __name__ == "__main__":
    main()

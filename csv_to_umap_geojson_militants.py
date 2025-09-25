#!/usr/bin/env python3
"""
csv_to_umap_geojson_militants.py

Utilisation :
    python csv_to_umap_geojson_militants.py --input "listemilitants.csv"

Fonctionnalités :
 - Normalisation avancée des adresses
 - Géocodage Nominatim puis fallback BAN (api-adresse.data.gouv.fr)
 - Tentatives supplémentaires (sans numéro, simplifiée, centre-ville)
 - Caches persistants (geocache.json / geocache_new.json)
 - Dossier horodaté avec :
      * output_umap.geojson
      * problematic_rows.csv (incomplètes / non géocodées)
      * geocache_added.json
      * geocache_new_added.json
"""

import csv
import json
import re
import time
import argparse
import requests
from pathlib import Path
from datetime import datetime
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# --------------------------------------------------------------------------- #
# ---------------------- Paramètres & arguments ----------------------------- #
# --------------------------------------------------------------------------- #

parser = argparse.ArgumentParser(description="Convertir un CSV en GeoJSON uMap-ready")
parser.add_argument("--input", required=True, help="Chemin du fichier CSV d'entrée")
args = parser.parse_args()

INPUT_CSV   = Path(args.input)
CACHE_FILE  = "geocache.json"
CACHE_NEW   = "geocache_new.json"
OUTPUT_ROOT = Path("outputs")

# --------------------------------------------------------------------------- #
# -------------------------- Fonctions utilitaires -------------------------- #
# --------------------------------------------------------------------------- #

def clean_text(txt: str) -> str:
    """Normalisation légère pour améliorer le géocodage"""
    if not isinstance(txt, str):
        return ""
    txt = txt.upper()
    txt = re.sub(r"[’'`]", " ", txt)
    txt = re.sub(r"[^A-Z0-9ÀÂÄÉÈÊËÎÏÔÖÙÛÜÇ \\-]", " ", txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt

def geocode_ban(address: str):
    """Fallback via Base Adresse Nationale"""
    url = "https://api-adresse.data.gouv.fr/search/"
    params = {"q": address, "limit": 1}
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.ok:
            data = r.json()
            if data.get("features"):
                feat = data["features"][0]
                lon, lat = feat["geometry"]["coordinates"]
                return lat, lon
    except Exception:
        pass
    return None

def make_feature(lon, lat, name, desc):
    """Création d'une Feature uMap-friendly (sans HTML)"""
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [lon, lat]},
        "properties": {
            "name": name,
            "description": desc,
            "_umap_options": {"color": "blue", "iconClass": "Drop"}
        }
    }

def load_cache(path):
    return json.load(open(path, encoding="utf-8")) if Path(path).exists() else {}

# --------------------------------------------------------------------------- #
# ---------------------------- Initialisations ------------------------------ #
# --------------------------------------------------------------------------- #

geocache     = load_cache(CACHE_FILE)
geocache_new = load_cache(CACHE_NEW)

geolocator = Nominatim(user_agent="geo_umap_script")
geocode    = RateLimiter(geolocator.geocode, min_delay_seconds=1)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
outdir    = OUTPUT_ROOT / timestamp
outdir.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------------------------------- #
# ----------------------------- Traitement CSV ------------------------------ #
# --------------------------------------------------------------------------- #

features          = []
problematic_rows  = []
new_cache_entries = {}
new_cache_new     = {}

# 👉 Essai automatique du bon séparateur (',' ou ';')
# On lit la première ligne pour décider
with open(INPUT_CSV, "r", encoding="utf-8") as f_test:
    first_line = f_test.readline()
delimiter = ";" if first_line.count(";") > first_line.count(",") else ","

with open(INPUT_CSV, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f, delimiter=delimiter)
    headers = reader.fieldnames if reader.fieldnames else []
    for row in reader:
        nom   = clean_text(row.get("Nom", ""))
        addr  = clean_text(row.get("Adresse", ""))
        cp    = clean_text(row.get("Code Postal", ""))
        ville = clean_text(row.get("Ville", ""))
        full  = f"{addr}, {cp} {ville}, France".strip(", ")

        if not addr or not cp or not ville:
            row["reason"] = "incomplete"
            problematic_rows.append(row)
            continue

        if full in geocache:
            lat, lon = geocache[full]
        else:
            lat = lon = None
            # 1️⃣ Nominatim direct
            try:
                res = geocode(full)
                if res: lat, lon = res.latitude, res.longitude
            except Exception:
                pass
            # 2️⃣ Nominatim sans numéro
            if not lat:
                addr_wo_num = re.sub(r"^\d+\s+", "", addr)
                if addr_wo_num != addr:
                    try:
                        res = geocode(f"{addr_wo_num}, {cp} {ville}, France")
                        if res: lat, lon = res.latitude, res.longitude
                    except Exception:
                        pass
            # 3️⃣ BAN
            if not lat:
                ban_res = geocode_ban(full)
                if ban_res: lat, lon = ban_res
            # 4️⃣ BAN simplifiée
            if not lat:
                addr_simple = re.sub(r"(RESIDENCE|CITE|LOTISSEMENT).*", "", addr)
                if addr_simple.strip():
                    ban_res = geocode_ban(f"{addr_simple}, {cp} {ville}, France")
                    if ban_res: lat, lon = ban_res
            # 5️⃣ Centre ville
            if not lat:
                ban_res = geocode_ban(f"{cp} {ville} France")
                if ban_res: lat, lon = ban_res

            if lat:
                geocache[full] = (lat, lon)
                new_cache_entries[full] = (lat, lon)
                if full not in geocache_new:
                    geocache_new[full] = (lat, lon)
                    new_cache_new[full] = (lat, lon)

        if lat:
            features.append(make_feature(lon, lat, nom, f"{addr}, {cp} {ville}"))
        else:
            row["reason"] = "not_geocoded"
            problematic_rows.append(row)

# --------------------------------------------------------------------------- #
# ------------------------------ Sauvegardes -------------------------------- #
# --------------------------------------------------------------------------- #

# GeoJSON final
geojson = {"type": "FeatureCollection", "features": features}
with open(outdir / "output_umap.geojson", "w", encoding="utf-8") as f:
    json.dump(geojson, f, ensure_ascii=False)

# Caches globaux
with open(CACHE_FILE, "w", encoding="utf-8") as f:
    json.dump(geocache, f, ensure_ascii=False, indent=2)
with open(CACHE_NEW, "w", encoding="utf-8") as f:
    json.dump(geocache_new, f, ensure_ascii=False, indent=2)

# Nouvelles entrées pour ce run
with open(outdir / "geocache_added.json", "w", encoding="utf-8") as f:
    json.dump(new_cache_entries, f, ensure_ascii=False, indent=2)
with open(outdir / "geocache_new_added.json", "w", encoding="utf-8") as f:
    json.dump(new_cache_new, f, ensure_ascii=False, indent=2)

# Lignes problématiques
if problematic_rows:
    with open(outdir / "problematic_rows.csv", "w", newline="", encoding="utf-8") as f:
        fieldnames = (headers if headers else ["Nom","Adresse","Code Postal","Ville"]) + ["reason"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(problematic_rows)

print(f"✅ GeoJSON prêt pour uMap : {outdir/'output_umap.geojson'}")
print(f"⚠️ Lignes problématiques : {len(problematic_rows)} (voir {outdir/'problematic_rows.csv'})")
#!/usr/bin/env python3
"""
Script de collecte des données Le Vélo Marseille - VERSION OPTIMISÉE
- Récupère les données depuis l'API GBFS Omega
- Sauvegarde dans Supabase (PostgreSQL) avec batch inserts
- Exporte en JSON pour le dashboard

Optimisations :
- Batch inserts (2 requêtes au lieu de 200+)
- Constantes pour les zones géographiques
- Gestion améliorée de la capacité nulle
"""

import os
import json
import requests
from datetime import datetime
from supabase import create_client, Client

# ═══════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════

# URLs de l'API Le Vélo (GBFS Omega Fifteen)
BASE_URL = "https://gbfs.omega.fifteen.eu/gbfs/2.2/marseille/en"
STATION_STATUS_URL = f"{BASE_URL}/station_status.json"
STATION_INFO_URL = f"{BASE_URL}/station_information.json"

# Zones géographiques de Marseille (latitude)
ZONE_NORD_LIMIT = 43.30
ZONE_CENTRE_LIMIT = 43.28

# Seuils de disponibilité pour les statuts
THRESHOLD_CRITICAL = 15
THRESHOLD_WARNING = 40
THRESHOLD_EXCELLENT = 70

# Configuration Supabase
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# Vérification des variables d'environnement
if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ Erreur : Variables SUPABASE_URL et SUPABASE_KEY requises")
    exit(1)

# Connexion à Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ═══════════════════════════════════════════════════════════
# FONCTIONS UTILITAIRES
# ═══════════════════════════════════════════════════════════

def determine_zone(latitude: float) -> str:
    """
    Détermine la zone géographique en fonction de la latitude
    
    Args:
        latitude: Latitude de la station
        
    Returns:
        Nom de la zone (Nord/Centre/Sud Marseille)
    """
    if latitude >= ZONE_NORD_LIMIT:
        return 'Nord Marseille'
    elif latitude >= ZONE_CENTRE_LIMIT:
        return 'Centre Marseille'
    else:
        return 'Sud Marseille'

def calculate_status(bikes: int, capacity: int) -> str:
    """
    Calcule le statut d'affichage d'une station
    
    Args:
        bikes: Nombre de vélos disponibles
        capacity: Capacité totale de la station
        
    Returns:
        Statut : critical, warning, good, excellent
    """
    if bikes == 0 or capacity == 0:
        return "critical"
    
    availability_rate = (bikes / capacity * 100)
    
    if availability_rate < THRESHOLD_CRITICAL:
        return "critical"
    elif availability_rate < THRESHOLD_WARNING:
        return "warning"
    elif availability_rate > THRESHOLD_EXCELLENT:
        return "excellent"
    else:
        return "good"

# ═══════════════════════════════════════════════════════════
# FONCTIONS PRINCIPALES
# ═══════════════════════════════════════════════════════════

def fetch_api_data():
    """
    Récupère les données depuis l'API GBFS
    
    Returns:
        Tuple (status_data, info_data) ou (None, None) en cas d'erreur
    """
    print("📡 Récupération des données API...")
    
    try:
        # Récupérer le statut des stations
        print(f"   → {STATION_STATUS_URL}")
        status_response = requests.get(STATION_STATUS_URL, timeout=10)
        status_response.raise_for_status()
        status_data = status_response.json()['data']['stations']
        print(f"   ✅ Stations status: {len(status_data)}")
        
        # Récupérer les infos des stations
        print(f"   → {STATION_INFO_URL}")
        info_response = requests.get(STATION_INFO_URL, timeout=10)
        info_response.raise_for_status()
        info_data = info_response.json()['data']['stations']
        print(f"   ✅ Stations info: {len(info_data)}")
        
        print("✅ API accessible")
        return status_data, info_data
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Erreur API : {e}")
        return None, None

def process_data(status_data, info_data):
    """
    Fusionne et nettoie les données
    Calcule les métriques (taux de disponibilité, statut)
    
    Args:
        status_data: Données de statut des stations
        info_data: Informations des stations
        
    Returns:
        Liste des enregistrements traités
    """
    print("🔄 Traitement des données...")
    
    # Créer un dictionnaire des infos par station_id (O(N) au lieu de O(N²))
    info_dict = {station['station_id']: station for station in info_data}
    
    processed = []
    
    for status in status_data:
        station_id = status['station_id']
        info = info_dict.get(station_id, {})
        
        # Récupérer les valeurs (avec gestion améliorée de la capacité)
        bikes = status.get('num_bikes_available', 0)
        stands = status.get('num_docks_available', 0)
        capacity = info.get('capacity', 0) or 0  # Gère None et 0
        
        # Calculer le taux de disponibilité
        availability_rate = round((bikes / capacity * 100), 1) if capacity > 0 else 0.0
        
        # Déterminer le statut d'affichage
        display_status = calculate_status(bikes, capacity)
        
        # Créer l'enregistrement
        record = {
            'station_id': station_id,
            'station_name': info.get('name', 'Station inconnue'),
            'address': info.get('address', 'Adresse non disponible'),
            'latitude': info.get('lat', 0.0),
            'longitude': info.get('lon', 0.0),
            'available_bikes': bikes,
            'available_stands': stands,
            'total_capacity': capacity,
            'status': status.get('status', 'unknown'),
            'display_status': display_status,
            'availability_rate': availability_rate,
            'last_update': datetime.now().isoformat()
        }
        
        processed.append(record)
    
    print(f"✅ {len(processed)} stations traitées")
    
    # Afficher un exemple pour debug
    if processed:
        example = processed[0]
        print(f"   📍 Exemple: {example['station_name']} - {example['available_bikes']}/{example['total_capacity']} vélos")
    
    return processed

def save_to_supabase(data):
    """
    Sauvegarde dans Supabase avec batch inserts (OPTIMISÉ)
    
    Architecture :
    - stations_metadata : informations statiques (UPSERT batch)
    - levelo_observations : données dynamiques (INSERT batch)
    
    Args:
        data: Liste des enregistrements à sauvegarder
        
    Returns:
        True si succès, False sinon
    """
    print("💾 Sauvegarde dans Supabase (batch inserts)...")
    
    # Préparer les batches
    metadata_batch = []
    observations_batch = []
    
    for record in data:
        station_id = record['station_id']
        
        # Déterminer la zone géographique
        zone = determine_zone(record['latitude'])
        
        # Batch 1 : Métadonnées des stations
        metadata_batch.append({
            'station_id': station_id,
            'station_name': record['station_name'],
            'address': record['address'],
            'latitude': record['latitude'],
            'longitude': record['longitude'],
            'total_capacity': record['total_capacity'],
            'zone': zone,
            'updated_at': datetime.now().isoformat()
        })
        
        # Batch 2 : Observations
        observations_batch.append({
            'station_id': station_id,
            'available_bikes': record['available_bikes'],
            'available_stands': record['available_stands'],
            'status': record['status']
        })
    
    # Exécuter les batch inserts
    saved_metadata = 0
    saved_observations = 0
    
    # 1. UPSERT batch metadata (1 seule requête)
    try:
        print(f"   → Upsert {len(metadata_batch)} métadonnées...")
        supabase.table('stations_metadata').upsert(
            metadata_batch, 
            on_conflict='station_id'
        ).execute()
        saved_metadata = len(metadata_batch)
        print(f"   ✅ {saved_metadata} métadonnées mises à jour")
    except Exception as e:
        print(f"   ❌ Erreur UPSERT batch metadata: {e}")
        return False
    
    # 2. INSERT batch observations (1 seule requête)
    try:
        print(f"   → Insert {len(observations_batch)} observations...")
        supabase.table('levelo_observations').insert(observations_batch).execute()
        saved_observations = len(observations_batch)
        print(f"   ✅ {saved_observations} observations insérées")
    except Exception as e:
        print(f"   ❌ Erreur INSERT batch observations: {e}")
        return False
    
    print(f"✅ Sauvegarde terminée : {saved_metadata} stations, {saved_observations} observations")
    return True

def export_json(data):
    """
    Exporte les données en JSON pour le dashboard Dust
    
    Args:
        data: Liste des enregistrements à exporter
        
    Returns:
        True si succès, False sinon
    """
    print("💾 Export JSON...")
    
    try:
        # Créer le dossier data s'il n'existe pas
        os.makedirs('data', exist_ok=True)
        
        # Sauvegarder en JSON
        output_file = 'data/levelo_data.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        # Calculer la taille du fichier
        file_size = os.path.getsize(output_file)
        file_size_kb = file_size / 1024
        
        print(f"✅ JSON exporté : {len(data)} stations ({file_size_kb:.1f} KB)")
        print(f"   📁 Fichier : {output_file}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur export JSON : {e}")
        return False

def main():
    """
    Fonction principale - Orchestration du workflow
    """
    print("=" * 70)
    print("🚴 COLLECTE DONNÉES LE VÉLO MARSEILLE (VERSION OPTIMISÉE)")
    print("=" * 70)
    print(f"⏰ Début : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # 1. Récupérer les données de l'API
    status_data, info_data = fetch_api_data()
    
    if not status_data or not info_data:
        print("❌ Impossible de récupérer les données API")
        exit(1)
    
    # 2. Traiter les données
    processed_data = process_data(status_data, info_data)
    
    if not processed_data:
        print("❌ Aucune donnée à traiter")
        exit(1)
    
    # 3. Sauvegarder dans Supabase (batch inserts)
    supabase_success = save_to_supabase(processed_data)
    
    if not supabase_success:
        print("❌ Erreur lors de la sauvegarde Supabase")
        exit(1)
    
    # 4. Exporter en JSON (pour le dashboard)
    json_success = export_json(processed_data)
    
    if not json_success:
        print("⚠️  Erreur lors de l'export JSON (non bloquant)")
    
    # 5. Résumé
    print()
    print("=" * 70)
    print("✅ Collecte terminée avec succès !")
    print(f"   📊 {len(processed_data)} stations traitées")
    print(f"   💾 2 requêtes SQL (batch inserts)")
    print(f"   📁 JSON exporté")
    print(f"⏰ Fin : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

# ═══════════════════════════════════════════════════════════
# POINT D'ENTRÉE
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    main()

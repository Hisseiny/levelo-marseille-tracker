#!/usr/bin/env python3
"""
Script de collecte des données Le Vélo Marseille
- Récupère les données depuis l'API GBFS
- Sauvegarde dans Supabase (PostgreSQL)
- Exporte en JSON pour le dashboard
"""

import os
import json
import requests
from datetime import datetime
from supabase import create_client, Client

# ═══════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════

# URLs de l'API Le Vélo (GBFS - General Bikeshare Feed Specification)
BASE_URL = "https://gbfs.fifteen.eu/marseille"
STATION_STATUS_URL = f"{BASE_URL}/gbfs/2/fr/station_status"
STATION_INFO_URL = f"{BASE_URL}/gbfs/2/fr/station_information"

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
# FONCTIONS
# ═══════════════════════════════════════════════════════════

def fetch_api_data():
    """
    Récupère les données depuis l'API GBFS
    Retourne : (status_data, info_data) ou (None, None) en cas d'erreur
    """
    print("📡 Récupération des données API...")
    
    try:
        # Récupérer le statut des stations (vélos disponibles en temps réel)
        print(f"   → {STATION_STATUS_URL}")
        status_response = requests.get(STATION_STATUS_URL, timeout=10)
        status_response.raise_for_status()
        status_data = status_response.json()['data']['stations']
        print(f"   ✅ Stations status: {len(status_data)}")
        
        # Récupérer les infos des stations (nom, adresse, capacité)
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
    """
    print("🔄 Traitement des données...")
    
    # Créer un dictionnaire des infos par station_id
    info_dict = {station['station_id']: station for station in info_data}
    
    processed = []
    
    for status in status_data:
        station_id = status['station_id']
        info = info_dict.get(station_id, {})
        
        # Récupérer les valeurs
        bikes = status.get('num_bikes_available', 0)
        stands = status.get('num_docks_available', 0)
        capacity = info.get('capacity', 1)
        
        # Calculer le taux de disponibilité
        availability_rate = round((bikes / capacity * 100), 1) if capacity > 0 else 0
        
        # Déterminer le statut d'affichage
        if bikes == 0:
            display_status = "critical"
        elif stands == 0:
            display_status = "critical"
        elif availability_rate < 15:
            display_status = "critical"
        elif availability_rate < 40:
            display_status = "warning"
        elif availability_rate > 70:
            display_status = "excellent"
        else:
            display_status = "good"
        
        # Créer l'enregistrement
        record = {
            'station_id': station_id,
            'station_name': info.get('name', 'Station inconnue'),
            'address': info.get('address', 'Adresse non disponible'),
            'latitude': info.get('lat', 0),
            'longitude': info.get('lon', 0),
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
        print(f"   📍 Exemple: {example['station_name']} - {example['available_bikes']} vélos")
    
    return processed

def save_to_supabase(data):
    """
    Sauvegarde dans la nouvelle structure à 2 tables
    - stations_metadata : informations statiques (nom, adresse, capacité)
    - levelo_observations : données dynamiques (vélos disponibles)
    """
    print("💾 Sauvegarde dans Supabase (nouvelle structure)...")
    
    saved_metadata = 0
    saved_observations = 0
    errors = []
    
    for record in data:
        try:
            station_id = record['station_id']
            
            # Déterminer la zone géographique
            lat = record['latitude']
            if lat >= 43.30:
                zone = 'Nord Marseille'
            elif lat >= 43.28:
                zone = 'Centre Marseille'
            else:
                zone = 'Sud Marseille'
            
            # 1. UPSERT dans stations_metadata
            # (mise à jour si existe, insertion sinon)
            supabase.table('stations_metadata').upsert({
                'station_id': station_id,
                'station_name': record['station_name'],
                'address': record['address'],
                'latitude': record['latitude'],
                'longitude': record['longitude'],
                'total_capacity': record['total_capacity'],
                'zone': zone,
                'updated_at': datetime.now().isoformat()
            }, on_conflict='station_id').execute()
            
            saved_metadata += 1
            
            # 2. INSERT dans levelo_observations
            # (toujours une nouvelle ligne pour l'historique)
            supabase.table('levelo_observations').insert({
                'station_id': station_id,
                'available_bikes': record['available_bikes'],
                'available_stands': record['available_stands'],
                'status': record['status']
            }).execute()
            
            saved_observations += 1
            
        except Exception as e:
            error_msg = f"Station {station_id}: {str(e)}"
            errors.append(error_msg)
            print(f"⚠️  {error_msg}")
            continue
    
    print(f"✅ {saved_metadata} métadonnées mises à jour")
    print(f"✅ {saved_observations} observations insérées")
    
    if errors:
        print(f"⚠️  {len(errors)} erreurs rencontrées")
    
    return saved_observations > 0

def export_json(data):
    """
    Exporte les données en JSON pour le dashboard Dust
    """
    print("💾 Export JSON...")
    
    try:
        # Créer le dossier data s'il n'existe pas
        os.makedirs('data', exist_ok=True)
        
        # Sauvegarder en JSON
        output_file = 'data/levelo_data.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ JSON exporté : {len(data)} stations")
        print(f"   📁 Fichier : {output_file}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur export JSON : {e}")
        return False

def main():
    """
    Fonction principale
    """
    print("=" * 70)
    print("🚴 COLLECTE DONNÉES LE VÉLO MARSEILLE")
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
    
    # 3. Sauvegarder dans Supabase
    supabase_success = save_to_supabase(processed_data)
    
    if not supabase_success:
        print("⚠️  Erreur lors de la sauvegarde Supabase")
    
    # 4. Exporter en JSON (pour le dashboard)
    json_success = export_json(processed_data)
    
    if not json_success:
        print("⚠️  Erreur lors de l'export JSON")
    
    # 5. Résumé
    print()
    print("=" * 70)
    if supabase_success and json_success:
        print("✅ Collecte terminée avec succès !")
    else:
        print("⚠️  Collecte terminée avec des avertissements")
    print(f"⏰ Fin : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

# ═══════════════════════════════════════════════════════════
# POINT D'ENTRÉE
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    main()

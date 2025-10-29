#!/usr/bin/env python3
"""
🚴 LE VÉLO MARSEILLE - Collecteur de Données
Récupère les données de l'API et les stocke dans Supabase
"""

import os
import sys
import requests
import json
from datetime import datetime
from supabase import create_client, Client

# Configuration des URLs (sans clé API)
URL_STATUS = "https://gbfs.omega.fifteen.eu/gbfs/2.2/marseille/en/station_status.json"
URL_INFO = "https://gbfs.omega.fifteen.eu/gbfs/2.2/marseille/en/station_information.json"

# Connexion Supabase
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ Erreur : Variables d'environnement SUPABASE_URL et SUPABASE_KEY requises")
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_api_data():
    """Récupère les données de l'API Le Vélo"""
    print("📡 Récupération des données API...")
    
    try:
        response_status = requests.get(URL_STATUS, timeout=10)
        response_info = requests.get(URL_INFO, timeout=10)
        
        if response_status.status_code != 200 or response_info.status_code != 200:
            print(f"❌ Erreur HTTP : {response_status.status_code}, {response_info.status_code}")
            return None
        
        data_status = response_status.json()
        data_info = response_info.json()
        
        print(f"✅ API accessible")
        
        return {
            'status': data_status['data']['stations'],
            'info': data_info['data']['stations']
        }
    except Exception as e:
        print(f"❌ Erreur lors de la récupération : {e}")
        return None

def process_data(api_data):
    """Traite et fusionne les données"""
    print("🔄 Traitement des données...")
    
    stations_status = api_data['status']
    stations_info = api_data['info']
    
    info_dict = {s['station_id']: s for s in stations_info}
    
    processed = []
    for status in stations_status:
        station_id = status['station_id']
        if station_id in info_dict:
            info = info_dict[station_id]
            
            bikes = status.get('num_bikes_available', 0)
            capacity = info.get('capacity', 0)
            availability_rate = (bikes / capacity * 100) if capacity > 0 else 0
            
            # Déterminer le statut
            if availability_rate < 15:
                status_label = "critical"
            elif availability_rate < 40:
                status_label = "warning"
            elif availability_rate < 70:
                status_label = "good"
            else:
                status_label = "excellent"
            
            processed.append({
                'station_id': station_id,
                'name': info.get('name', 'Station inconnue'),
                'address': info.get('address', 'Adresse non disponible'),
                'lat': info.get('lat', 0.0),
                'lon': info.get('lon', 0.0),
                'capacity': capacity,
                'bikes_available': bikes,
                'docks_available': status.get('num_docks_available', 0),
                'availability_rate': round(availability_rate, 1),
                'status': status_label,
                'timestamp': datetime.now().isoformat()
            })
    
    print(f"✅ {len(processed)} stations traitées")
    return processed

def save_to_supabase(stations_data):
    """Sauvegarde dans Supabase"""
    print("💾 Sauvegarde dans Supabase...")
    
    try:
        # Insérer les données
        response = supabase.table('stations').insert(stations_data).execute()
        
        print(f"✅ {len(stations_data)} stations sauvegardées")
        return True
    except Exception as e:
        print(f"❌ Erreur Supabase : {e}")
        return False

def export_json(stations_data):
    """Exporte en JSON pour le dashboard Frame"""
    print("📤 Export JSON...")
    
    try:
        os.makedirs('data', exist_ok=True)
        
        with open('data/levelo_data.json', 'w', encoding='utf-8') as f:
            json.dump(stations_data, f, ensure_ascii=False, indent=2)
        
        print("✅ JSON exporté : data/levelo_data.json")
        return True
    except Exception as e:
        print(f"❌ Erreur export JSON : {e}")
        return False

def get_latest_stats():
    """Récupère les stats depuis Supabase"""
    try:
        # Obtenir les dernières données
        response = supabase.table('stations').select('*').order('timestamp', desc=True).limit(200).execute()
        return response.data
    except:
        return []

def main():
    """Fonction principale"""
    print("=" * 70)
    print("🚴 LE VÉLO MARSEILLE - Collecte de Données")
    print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    # 1. Récupérer les données
    api_data = fetch_api_data()
    if not api_data:
        print("❌ Échec de la récupération")
        sys.exit(1)
    
    # 2. Traiter les données
    stations_data = process_data(api_data)
    if not stations_data:
        print("❌ Échec du traitement")
        sys.exit(1)
    
    # 3. Sauvegarder dans Supabase
    if not save_to_supabase(stations_data):
        print("❌ Échec de la sauvegarde")
        sys.exit(1)
    
    # 4. Exporter en JSON
    if not export_json(stations_data):
        print("⚠️  Échec de l'export JSON (non critique)")
    
    # 5. Statistiques
    print("\n📊 STATISTIQUES")
    print("=" * 70)
    total_bikes = sum(s['bikes_available'] for s in stations_data)
    total_capacity = sum(s['capacity'] for s in stations_data)
    critical = len([s for s in stations_data if s['status'] == 'critical'])
    warning = len([s for s in stations_data if s['status'] == 'warning'])
    good = len([s for s in stations_data if s['status'] == 'good'])
    excellent = len([s for s in stations_data if s['status'] == 'excellent'])
    
    print(f"🚴 Vélos disponibles : {total_bikes}/{total_capacity}")
    print(f"📍 Taux moyen : {(total_bikes/total_capacity*100):.1f}%")
    print(f"\n🔴 Critiques   : {critical}")
    print(f"🟡 Attention   : {warning}")
    print(f"🟢 Bonnes      : {good}")
    print(f"🔵 Excellentes : {excellent}")
    print("=" * 70)
    print("✅ Collecte terminée avec succès !")

if __name__ == "__main__":
    main()

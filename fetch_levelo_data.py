#!/usr/bin/env python3
"""
🚴 LE VÉLO MARSEILLE - Collecteur de Données
"""

import os
import sys
import requests
import json
from datetime import datetime
from supabase import create_client, Client

# Configuration
URL_STATUS = "https://gbfs.omega.fifteen.eu/gbfs/2.2/marseille/en/station_status.json"
URL_INFO = "https://gbfs.omega.fifteen.eu/gbfs/2.2/marseille/en/station_information.json"

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ Variables d'environnement manquantes")
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_api_data():
    """Récupère les données de l'API"""
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
        print(f"❌ Erreur : {e}")
        return None

def process_data(api_data):
    """Traite et fusionne les données"""
    print("🔄 Traitement des données...")
    
    stations_status = api_data['status']
    stations_info = api_data['info']
    
    print(f"   Stations status: {len(stations_status)}")
    print(f"   Stations info: {len(stations_info)}")
    
    # Afficher un exemple de station_id pour debug
    if stations_status:
        sample_id = stations_status[0]['station_id']
        print(f"   Exemple station_id: '{sample_id}' (type: {type(sample_id)})")
    
    info_dict = {s['station_id']: s for s in stations_info}
    
    processed = []
    skipped = 0
    
    for status in stations_status:
        station_id = status['station_id']
        
        if station_id not in info_dict:
            skipped += 1
            continue
        
        info = info_dict[station_id]
        
        bikes = status.get('num_bikes_available', 0)
        capacity = info.get('capacity', 0)
        
        # Essayer de convertir en int, sinon utiliser un hash
        try:
            if isinstance(station_id, str):
                # Si c'est une string numérique
                if station_id.isdigit():
                    station_id_int = int(station_id)
                else:
                    # Sinon, utiliser un hash
                    station_id_int = hash(station_id) % 1000000
            else:
                station_id_int = int(station_id)
        except Exception as e:
            print(f"   ⚠️  Erreur conversion station_id '{station_id}': {e}")
            skipped += 1
            continue
        
        try:
            processed.append({
                'station_id': station_id_int,
                'station_name': str(info.get('name', 'Station inconnue'))[:255],
                'address': str(info.get('address', 'Adresse non disponible'))[:255],
                'latitude': float(info.get('lat', 0.0)),
                'longitude': float(info.get('lon', 0.0)),
                'total_capacity': int(capacity),
                'available_bikes': int(bikes),
                'available_stands': int(status.get('num_docks_available', 0)),
                'status': 'active' if status.get('is_renting', 0) == 1 else 'inactive',
                'last_update': datetime.now().isoformat()
            })
        except Exception as e:
            print(f"   ⚠️  Erreur traitement station {station_id}: {e}")
            skipped += 1
            continue
    
    print(f"✅ {len(processed)} stations traitées")
    if skipped > 0:
        print(f"⚠️  {skipped} stations ignorées")
    
    return processed

def save_to_supabase(stations_data):
    """Sauvegarde dans Supabase"""
    print("💾 Sauvegarde dans Supabase...")
    
    if not stations_data:
        print("❌ Aucune donnée à sauvegarder")
        return False
    
    try:
        # Sauvegarder par batch de 50 pour éviter les timeouts
        batch_size = 50
        total_saved = 0
        
        for i in range(0, len(stations_data), batch_size):
            batch = stations_data[i:i+batch_size]
            response = supabase.table('levelo_data').insert(batch).execute()
            total_saved += len(batch)
            print(f"   Batch {i//batch_size + 1}: {len(batch)} stations")
        
        print(f"✅ {total_saved} stations sauvegardées")
        return True
    except Exception as e:
        print(f"❌ Erreur Supabase : {e}")
        import traceback
        traceback.print_exc()
        return False

def export_json(stations_data):
    """Exporte en JSON"""
    print("📤 Export JSON...")
    
    try:
        os.makedirs('data', exist_ok=True)
        
        # Ajouter les stats pour le dashboard
        for station in stations_data:
            bikes = station['available_bikes']
            capacity = station['total_capacity']
            availability_rate = (bikes / capacity * 100) if capacity > 0 else 0
            
            if availability_rate < 15:
                station['display_status'] = "critical"
            elif availability_rate < 40:
                station['display_status'] = "warning"
            elif availability_rate < 70:
                station['display_status'] = "good"
            else:
                station['display_status'] = "excellent"
            
            station['availability_rate'] = round(availability_rate, 1)
        
        with open('data/levelo_data.json', 'w', encoding='utf-8') as f:
            json.dump(stations_data, f, ensure_ascii=False, indent=2)
        
        print("✅ JSON exporté")
        return True
    except Exception as e:
        print(f"❌ Erreur export : {e}")
        return False

def main():
    """Fonction principale"""
    print("=" * 70)
    print("🚴 LE VÉLO MARSEILLE - Collecte de Données")
    print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    # 1. Récupérer
    api_data = fetch_api_data()
    if not api_data:
        sys.exit(1)
    
    # 2. Traiter
    stations_data = process_data(api_data)
    if not stations_data or len(stations_data) == 0:
        print("❌ Aucune station traitée")
        sys.exit(1)
    
    # 3. Sauvegarder
    if not save_to_supabase(stations_data):
        sys.exit(1)
    
    # 4. Exporter
    export_json(stations_data)
    
    # 5. Stats
    print("\n📊 STATISTIQUES")
    print("=" * 70)
    total_bikes = sum(s['available_bikes'] for s in stations_data)
    total_capacity = sum(s['total_capacity'] for s in stations_data)
    print(f"🚴 Vélos : {total_bikes}/{total_capacity}")
    print(f"📍 Taux : {(total_bikes/total_capacity*100):.1f}%")
    print("=" * 70)
    print("✅ Collecte terminée !")

if __name__ == "__main__":
    main()

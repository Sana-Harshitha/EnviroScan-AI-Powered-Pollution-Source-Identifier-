import os
import requests
import pandas as pd
import numpy as np
import osmnx as ox
from dotenv import load_dotenv
from openaq_utils.openaq_sensors import get_sensors
from weather_utils.current_weather import get_current_weather

load_dotenv()
API_KEY = os.getenv("OPENAQ_API_KEY")
WEATHER_KEY = os.getenv("OPENWEATHER_API_KEY")
HEADERS = {"X-API-Key": API_KEY} if API_KEY else {}

# --- Cities with bounding boxes (more cities for more data) ---
cities = {
    "Delhi": {"min_lat": 28.4, "max_lat": 28.9, "min_lon": 76.8, "max_lon": 77.4},
    "Mumbai": {"min_lat": 18.9, "max_lat": 19.3, "min_lon": 72.7, "max_lon": 72.95},
    "Bangalore": {"min_lat": 12.85, "max_lat": 13.1, "min_lon": 77.5, "max_lon": 77.7},
    "Chennai": {"min_lat": 13.0, "max_lat": 13.2, "min_lon": 80.2, "max_lon": 80.35},
    "Kolkata": {"min_lat": 22.5, "max_lat": 22.7, "min_lon": 88.3, "max_lon": 88.45},
}

# --- Function to fetch OpenAQ + Weather + OSM features ---
def get_values(lat, lon, required_params=None, radius=5000, osm_dist=2000):
    if required_params is None:
        required_params = ['pm25', 'pm10', 'no2', 'co', 'so2', 'o3']

    # OpenAQ sensors
    stations = get_sensors(lat, lon, radius=radius, limit=10)
    if not stations:
        row = {p: None for p in required_params}
        row.update({
            'latitude': lat,
            'longitude': lon,
            'station_id': None,
            'station_name': None,
            'weather': get_current_weather(lat, lon, WEATHER_KEY)
        })
    else:
        nearest_id, nearest = min(
            stations.items(),
            key=lambda kv: kv[1].get('distance_m') or float('inf')
        )
        row = {p: None for p in required_params}
        row.update({
            'latitude': lat,
            'longitude': lon,
            'station_id': nearest_id,
            'station_name': nearest.get('station_name'),
            'weather': get_current_weather(lat, lon, WEATHER_KEY)
        })
        for s in nearest.get('sensors', []):
            param = s.get('parameter')
            if param in required_params:
                sid = s.get('sensor_id')
                meas_url = f"https://api.openaq.org/v3/sensors/{sid}/measurements"
                r = requests.get(meas_url, headers=HEADERS, params={"limit":1, "sort":"desc"})
                if r.status_code == 200:
                    mvals = r.json().get('results', [])
                    if mvals:
                        row[param] = mvals[0].get('value')

    # OSM features
    tags = {"landuse": ["industrial", "farmland", "farmyard"], "amenity": ["waste_disposal", "recycling"]}
    try:
        landuse_gdf = ox.features_from_point((lat, lon), dist=osm_dist, tags=tags)
        row['num_industrial'] = len(landuse_gdf[landuse_gdf.get('landuse') == 'industrial']) if 'landuse' in landuse_gdf.columns else 0
        row['num_farmland'] = len(landuse_gdf[landuse_gdf.get('landuse') == 'farmland']) if 'landuse' in landuse_gdf.columns else 0
        row['num_dumpsites'] = len(landuse_gdf[landuse_gdf.get('amenity') == 'waste_disposal']) if 'amenity' in landuse_gdf.columns else 0
        row['num_recycling'] = len(landuse_gdf[landuse_gdf.get('amenity') == 'recycling']) if 'amenity' in landuse_gdf.columns else 0
    except:
        row.update({'num_industrial': 0, 'num_farmland':0, 'num_dumpsites':0, 'num_recycling':0})

    return row


locations = []
step = 0.10  # ~2 km spacing for more points
for city, bbox in cities.items():
    lats = np.arange(bbox["min_lat"], bbox["max_lat"], step)
    lons = np.arange(bbox["min_lon"], bbox["max_lon"], step)
    locations += [(lat, lon) for lat in lats for lon in lons]

print(f"Total sampling points: {len(locations)}")

# --- Fetch data ---
rows = []
for i, (lat, lon) in enumerate(locations):
    print(f"Fetching point {i+1}/{len(locations)}: ({lat},{lon})")
    try:
        data = get_values(lat, lon)
        rows.append(data)
    except Exception as e:
        print(f"Error at ({lat},{lon}): {e}")

# --- Convert to DataFrame ---
df = pd.DataFrame(rows)
df['source_air_quality'] = 'OpenAQ'
df['source_weather'] = 'OpenWeatherMap'
df['timestamp'] = pd.Timestamp.now()

# --- Save ---
os.makedirs("./data", exist_ok=True)
df.to_csv("./data/module1_data_training.csv", index=False)
df.to_json("./data/module1_data_training.json", orient='records', lines=True)

print(f"Total records collected: {len(df)}")
print(df.head())

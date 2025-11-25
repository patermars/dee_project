import os
from flask import Flask, jsonify
import pymysql
import requests
from datetime import datetime
import random

app = Flask(__name__)

DB_CONFIG = {
    'host': "tramway.proxy.rlwy.net",
    'user': "root",
    'password': "cPirYIbjHosWGnPqxATBhipfheDtnBLa",
    'database': "railway",
    'port': 49438,
    'connect_timeout': 10,
    'charset': 'utf8mb4'
}

TOMTOM_API_KEY = "UGueLwdYpWdFsN9O8q1PW3Mu3bSIAuTD"

TRAFFIC_FLOW_POINTS = [
    {"name": "Hinjewadi Phase 1 Junction", "lat": 18.5950, "lon": 73.7183, "zoom": 15},
    {"name": "Baner Road Junction", "lat": 18.5596, "lon": 73.7802, "zoom": 15},
    {"name": "Shivajinagar Circle", "lat": 18.5308, "lon": 73.8478, "zoom": 15},
    {"name": "Viman Nagar Square", "lat": 18.5679, "lon": 73.9143, "zoom": 15},
    {"name": "Katraj Bypass", "lat": 18.4489, "lon": 73.8671, "zoom": 15},
    {"name": "Wakad Bridge", "lat": 18.5993, "lon": 73.7580, "zoom": 15},
    {"name": "Pune University Circle", "lat": 18.5492, "lon": 73.8093, "zoom": 15},
    {"name": "Koregaon Park Junction", "lat": 18.5333, "lon": 73.8949, "zoom": 15},
    {"name": "Camp Area", "lat": 18.5165, "lon": 73.8562, "zoom": 15},
    {"name": "Swargate Bus Stand", "lat": 18.5049, "lon": 73.8562, "zoom": 15},
    {"name": "FC Road", "lat": 18.5314, "lon": 73.8478, "zoom": 15},
    {"name": "JM Road", "lat": 18.5165, "lon": 73.8562, "zoom": 15},
    {"name": "Aundh Chowk", "lat": 18.5621, "lon": 73.8097, "zoom": 15},
    {"name": "Pimpri Chowk", "lat": 18.6298, "lon": 73.8026, "zoom": 15},
    {"name": "Chinchwad Station", "lat": 18.6415, "lon": 73.7956, "zoom": 15},
    {"name": "Nigdi", "lat": 18.6580, "lon": 73.7693, "zoom": 15},
    {"name": "Balewadi", "lat": 18.5836, "lon": 73.7744, "zoom": 15},
    {"name": "Mahalunge", "lat": 18.6130, "lon": 73.7758, "zoom": 15},
    {"name": "Mundhwa", "lat": 18.5517, "lon": 73.9314, "zoom": 15},
    {"name": "Kharadi", "lat": 18.5536, "lon": 73.9447, "zoom": 15},
    {"name": "Magarpatta", "lat": 18.5143, "lon": 73.9256, "zoom": 15},
    {"name": "Hadapsar", "lat": 18.5089, "lon": 73.9260, "zoom": 15},
    {"name": "Saswad Road", "lat": 18.4935, "lon": 73.9441, "zoom": 15},
    {"name": "Fatima Nagar", "lat": 18.5115, "lon": 73.9062, "zoom": 15},
    {"name": "Wadgaon Sheri", "lat": 18.5583, "lon": 73.9163, "zoom": 15}
]

INCIDENT_BBOX = {
    "min_lat": 18.4000,
    "min_lon": 73.7000,
    "max_lat": 18.6500,
    "max_lon": 73.9500
}

def get_db_connection():
    """Create database connection"""
    try:
        connection = pymysql.connect(**DB_CONFIG)
        print("✅ Database connected")
        return connection
    except Exception as e:
        print(f"❌ Database error: {e}")
        raise

def fetch_traffic_flow(lat, lon, zoom=15):
    """Fetch traffic flow data from TomTom API"""
    url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/{zoom}/json"
    params = {"key": TOMTOM_API_KEY, "point": f"{lat},{lon}"}
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"⚠️ API error: {e}")
        return None

def parse_and_save_flow(data, location_name, connection):
    """Parse and save traffic flow data"""
    try:
        flow_data = data["flowSegmentData"]
        current_speed = flow_data.get("currentSpeed", 0)
        
        if current_speed > 0:
            current_speed = max(5, current_speed * random.uniform(0.9, 1.1))
        
        with connection.cursor() as cursor:
            sql = """
                INSERT INTO traffic_flow
                (location_name, timestamp, latitude, longitude, current_speed,
                 free_flow_speed, current_travel_time, free_flow_travel_time,
                 confidence, road_closure)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                timestamp = VALUES(timestamp),
                current_speed = VALUES(current_speed),
                current_travel_time = VALUES(current_travel_time)
            """
            cursor.execute(sql, (
                location_name,
                datetime.utcnow(),
                flow_data["coordinates"]["coordinate"][0]["latitude"],
                flow_data["coordinates"]["coordinate"][0]["longitude"],
                current_speed,
                flow_data.get("freeFlowSpeed", 0),
                flow_data.get("currentTravelTime", 0),
                flow_data.get("freeFlowTravelTime", 0),
                flow_data.get("confidence", 0),
                flow_data.get("roadClosure", False)
            ))
            connection.commit()
            return True
    except Exception as e:
        print(f"❌ Save error: {e}")
        return False

def fetch_incidents(bbox):
    """Fetch traffic incidents"""
    url = "https://api.tomtom.com/traffic/services/5/incidentDetails"
    params = {
        "key": TOMTOM_API_KEY,
        "bbox": f"{bbox['min_lon']},{bbox['min_lat']},{bbox['max_lon']},{bbox['max_lat']}",
        "fields": "{incidents{type,geometry{type,coordinates},properties{id,iconCategory,magnitudeOfDelay,events{description},startTime,delay,from}}}",
        "language": "en-GB",
        "timeValidityFilter": "present"
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"⚠️ Incident API error: {e}")
        return None

def parse_and_save_incidents(data, connection):
    """Parse and save incidents"""
    incident_count = 0
    try:
        if "incidents" not in data:
            return incident_count
        
        with connection.cursor() as cursor:
            for incident in data["incidents"]:
                try:
                    props = incident.get("properties", {})
                    coords = incident.get("geometry", {}).get("coordinates", [[0, 0]])
                    
                    if coords and len(coords) > 0:
                        if isinstance(coords[0], list):
                            lon, lat = coords[0][0], coords[0][1]
                        else:
                            lon, lat = coords[0], coords[1]
                    else:
                        continue
                    
                    events = props.get("events", [{}])
                    event_desc = events[0].get("description", "Unknown") if events else "Unknown"
                    
                    sql = """
                        INSERT INTO traffic_incidents
                        (incident_id, incident_type, severity, description, latitude, longitude,
                         start_time, last_updated, delay_seconds, affected_road)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE last_updated = VALUES(last_updated)
                    """
                    
                    cursor.execute(sql, (
                        props.get("id", f"inc_{incident_count}"),
                        str(props.get("iconCategory", "unknown")),
                        str(props.get("magnitudeOfDelay", 0)),
                        event_desc,
                        lat, lon,
                        props.get("startTime"),
                        datetime.utcnow(),
                        props.get("delay", 0),
                        props.get("from", "Unknown")
                    ))
                    incident_count += 1
                except:
                    continue
            
            connection.commit()
    except Exception as e:
        print(f"❌ Incident error: {e}")
    
    return incident_count

@app.route('/')
def home():
    """Health check"""
    return jsonify({
        "status": "🚦 Traffic Ingestion Service",
        "version": "1.0",
        "timestamp": datetime.utcnow().isoformat()
    })

@app.route('/health')
def health():
    """Health check for Render"""
    return jsonify({"status": "healthy"}), 200

@app.route('/status')
def status():
    """Database status"""
    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) as count FROM traffic_flow")
            result = cursor.fetchone()
            count = result[0] if result else 0
        connection.close()
        
        return jsonify({
            "status": "✅ Connected",
            "records": count,
            "timestamp": datetime.utcnow().isoformat()
        }), 200
    except Exception as e:
        return jsonify({
            "status": "❌ Error",
            "error": str(e)
        }), 500

@app.route('/init-db', methods=['GET', 'POST'])
def init_database():
    """Initialize database tables"""
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS traffic_flow (
                id INT AUTO_INCREMENT PRIMARY KEY,
                location_name VARCHAR(200) UNIQUE,
                timestamp DATETIME,
                latitude DOUBLE,
                longitude DOUBLE,
                current_speed DOUBLE,
                free_flow_speed DOUBLE,
                current_travel_time INT,
                free_flow_travel_time INT,
                confidence DOUBLE,
                road_closure BOOLEAN,
                INDEX idx_timestamp (timestamp)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS traffic_incidents (
                id INT AUTO_INCREMENT PRIMARY KEY,
                incident_id VARCHAR(100) UNIQUE,
                incident_type VARCHAR(100),
                severity VARCHAR(50),
                description TEXT,
                latitude DOUBLE,
                longitude DOUBLE,
                start_time VARCHAR(50),
                last_updated DATETIME,
                delay_seconds INT,
                affected_road VARCHAR(200)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        
        connection.commit()
        cursor.close()
        
        return jsonify({
            "status": "✅ Database initialized",
            "tables": ["traffic_flow", "traffic_incidents"]
        }), 200
        
    except Exception as e:
        return jsonify({"status": "❌ Error", "error": str(e)}), 500
    finally:
        if connection:
            connection.close()

@app.route('/ingest', methods=['GET', 'POST'])
def ingest_data():
    """Main ingestion endpoint"""
    print(f"\n{'='*60}")
    print(f"🚀 Ingestion started at {datetime.utcnow()}")
    print(f"{'='*60}\n")
    
    connection = None
    success_count = 0
    incident_count = 0
    
    try:
        connection = get_db_connection()
        
        # Collect traffic flow
        print("📊 Collecting traffic flow...")
        for idx, point in enumerate(TRAFFIC_FLOW_POINTS, 1):
            print(f"  [{idx}/{len(TRAFFIC_FLOW_POINTS)}] {point['name']}")
            
            flow_data = fetch_traffic_flow(point["lat"], point["lon"], point["zoom"])
            if flow_data:
                if parse_and_save_flow(flow_data, point["name"], connection):
                    success_count += 1
                    print(f"    ✅ Saved")
                else:
                    print(f"    ❌ Failed")
            else:
                print(f"    ⚠️ No data")
        
        # Collect incidents
        print("\n🚨 Collecting incidents...")
        incident_data = fetch_incidents(INCIDENT_BBOX)
        if incident_data:
            incident_count = parse_and_save_incidents(incident_data, connection)
            print(f"  ✅ Saved {incident_count} incidents")
        
        print(f"\n{'='*60}")
        print(f"✅ Completed: {success_count} points, {incident_count} incidents")
        print(f"{'='*60}\n")
        
        return jsonify({
            "status": "✅ success",
            "flow_points": success_count,
            "incidents": incident_count,
            "timestamp": datetime.utcnow().isoformat()
        }), 200
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        return jsonify({"status": "❌ error", "message": str(e)}), 500
    
    finally:
        if connection:
            connection.close()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port, debug=False)
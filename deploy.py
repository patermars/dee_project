

# deploy.py
import os
from flask import Flask, jsonify
import pymysql
import requests
from datetime import datetime, timedelta
import random
import time
from typing import Dict, List, Optional
import json
from collections import defaultdict

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

CALL_INTERVAL_SECONDS = 30
DATA_RETENTION_MINUTES = 10

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
    try:
        connection = pymysql.connect(**DB_CONFIG)
        return connection
    except Exception as e:
        print(f"Database error: {e}")
        raise

def create_tables():
    conn = get_db_connection()
    cursor = conn.cursor()
    
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
            INDEX idx_timestamp (timestamp),
            INDEX idx_location (location_name)
        )
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
            start_time DATETIME,
            end_time DATETIME,
            last_updated DATETIME,
            delay_seconds INT,
            affected_road VARCHAR(200),
            INDEX idx_start_time (start_time),
            INDEX idx_severity (severity),
            INDEX idx_type (incident_type)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS route_health (
            id INT AUTO_INCREMENT PRIMARY KEY,
            route_name VARCHAR(200) UNIQUE,
            timestamp DATETIME,
            health_score INT,
            congestion_level VARCHAR(50),
            delay_percentage DOUBLE,
            incident_count INT,
            recommendation TEXT,
            INDEX idx_timestamp (timestamp),
            INDEX idx_route_name (route_name),
            INDEX idx_health_score (health_score)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS hourly_aggregates (
            id INT AUTO_INCREMENT PRIMARY KEY,
            route_name VARCHAR(200),
            hour_of_day INT,
            day_of_week INT,
            date DATE,
            timestamp DATETIME,
            avg_speed DOUBLE,
            avg_travel_time DOUBLE,
            avg_delay DOUBLE,
            vehicle_count INT,
            congestion_level VARCHAR(50),
            UNIQUE KEY unique_route_hour (route_name, hour_of_day),
            INDEX idx_timestamp (timestamp),
            INDEX idx_route_hour (route_name, hour_of_day),
            INDEX idx_date (date)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS vehicle_classification (
            id INT AUTO_INCREMENT PRIMARY KEY,
            location_name VARCHAR(200),
            vehicle_type VARCHAR(50),
            timestamp DATETIME,
            count INT,
            percentage DOUBLE,
            UNIQUE KEY unique_location_vehicle (location_name, vehicle_type),
            INDEX idx_timestamp (timestamp),
            INDEX idx_location (location_name)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS speed_distribution (
            id INT AUTO_INCREMENT PRIMARY KEY,
            location_name VARCHAR(200),
            speed_range VARCHAR(50),
            timestamp DATETIME,
            vehicle_count INT,
            percentage DOUBLE,
            UNIQUE KEY unique_location_speed (location_name, speed_range),
            INDEX idx_timestamp (timestamp),
            INDEX idx_location (location_name)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS congestion_hotspots (
            id INT AUTO_INCREMENT PRIMARY KEY,
            location_name VARCHAR(200) UNIQUE,
            latitude DOUBLE,
            longitude DOUBLE,
            timestamp DATETIME,
            congestion_score DOUBLE,
            avg_speed DOUBLE,
            delay_minutes DOUBLE,
            severity VARCHAR(50),
            INDEX idx_timestamp (timestamp),
            INDEX idx_severity (severity)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_kpi (
            id INT AUTO_INCREMENT PRIMARY KEY,
            date DATE UNIQUE,
            timestamp DATETIME,
            total_vehicles INT,
            avg_speed DOUBLE,
            congestion_level VARCHAR(50),
            avg_travel_time_minutes DOUBLE,
            total_incidents INT,
            change_percent DOUBLE,
            INDEX idx_date (date)
        )
    """)
    
    conn.commit()
    cursor.close()
    conn.close()
    print("All tables created successfully")

def fetch_traffic_flow(lat, lon, zoom=15):
    url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/{zoom}/json"
    params = {"key": TOMTOM_API_KEY, "point": f"{lat},{lon}"}
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Traffic flow API call failed: {e}")
        return None

def parse_traffic_flow(data, location_name):
    try:
        flow_data = data["flowSegmentData"]
        
        current_speed = flow_data.get("currentSpeed", 0)
        if current_speed > 0:
            variation = random.uniform(0.9, 1.1)
            current_speed = max(5, current_speed * variation)
        
        parsed = {
            "location_name": location_name,
            "timestamp": datetime.utcnow(),
            "latitude": flow_data["coordinates"]["coordinate"][0]["latitude"],
            "longitude": flow_data["coordinates"]["coordinate"][0]["longitude"],
            "current_speed": current_speed,
            "free_flow_speed": flow_data.get("freeFlowSpeed", 0),
            "current_travel_time": flow_data.get("currentTravelTime", 0),
            "free_flow_travel_time": flow_data.get("freeFlowTravelTime", 0),
            "confidence": flow_data.get("confidence", 0),
            "road_closure": flow_data.get("roadClosure", False)
        }
        return parsed
    except Exception as e:
        print(f"Parsing traffic flow failed: {e}")
        return None

def fetch_incidents(bbox):
    url = "https://api.tomtom.com/traffic/services/5/incidentDetails"
    params = {
        "key": TOMTOM_API_KEY,
        "bbox": f"{bbox['min_lon']},{bbox['min_lat']},{bbox['max_lon']},{bbox['max_lat']}",
        "fields": "{incidents{type,geometry{type,coordinates},properties{id,iconCategory,magnitudeOfDelay,events{description,code,iconCategory},startTime,endTime,from,to,length,delay,roadNumbers,timeValidity}}}",
        "language": "en-GB",
        "categoryFilter": "0,1,2,3,4,5,6,7,8,9,10,11,14",
        "timeValidityFilter": "present"
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Incident API call failed: {e}")
        return None

def parse_incidents(data):
    incidents = []
    try:
        if "incidents" not in data:
            return incidents
            
        for incident in data["incidents"]:
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
            event_desc = events[0].get("description", "Unknown incident") if events else "Unknown incident"
            
            parsed = {
                "incident_id": props.get("id", ""),
                "incident_type": str(props.get("iconCategory", "unknown")),
                "severity": str(props.get("magnitudeOfDelay", 0)),
                "description": event_desc,
                "latitude": lat,
                "longitude": lon,
                "start_time": convert_iso_to_mysql_datetime(props.get("startTime")),
                "end_time": convert_iso_to_mysql_datetime(props.get("endTime")), 
                "last_updated": datetime.utcnow(),
                "delay_seconds": props.get("delay", 0),
                "affected_road": props.get("from", "Unknown road")
            }
            incidents.append(parsed)
    except Exception as e:
        print(f"Parsing incidents failed: {e}")
    
    return incidents

def convert_iso_to_mysql_datetime(iso_string):
    if not iso_string:
        return None
    try:
        dt = datetime.strptime(iso_string.replace('Z', '+00:00').split('+')[0], '%Y-%m-%dT%H:%M:%S')
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        print(f"DateTime conversion failed: {e}")
        return None

def calculate_vehicle_distribution(flow_points_data):
    base_distribution = {
        "Cars": 0.65,
        "Trucks": 0.15,
        "Buses": 0.10,
        "Motorcycles": 0.10
    }
    
    for vehicle_type in base_distribution:
        variation = random.uniform(0.9, 1.1)
        base_distribution[vehicle_type] = max(0.05, min(0.8, base_distribution[vehicle_type] * variation))
    
    total = sum(base_distribution.values())
    for vehicle_type in base_distribution:
        base_distribution[vehicle_type] /= total
    
    total_vehicles = sum([point.get("vehicle_count", 100) for point in flow_points_data])
    
    distribution = []
    for v_type, percentage in base_distribution.items():
        count = int(total_vehicles * percentage)
        distribution.append({
            "vehicle_type": v_type,
            "count": count,
            "percentage": percentage * 100
        })
    
    return distribution

def calculate_speed_distribution(flow_points_data):
    speed_ranges = [
        {"range": "0-20 km/h", "min": 0, "max": 20},
        {"range": "20-40 km/h", "min": 20, "max": 40}
    ]
    
    distribution = []
    for speed_range in speed_ranges:
        count = 0
        for point in flow_points_data:
            speed = point.get("current_speed", 0)
            if speed_range["min"] <= speed < speed_range["max"]:
                count += 1
        
        if count > 0:
            count = max(1, int(count * random.uniform(0.8, 1.2)))
        
        percentage = (count / len(flow_points_data) * 100) if flow_points_data else 0
        distribution.append({
            "speed_range": speed_range["range"],
            "vehicle_count": count * 100,
            "percentage": percentage
        })
    
    return distribution

def identify_congestion_hotspots(flow_points_data):
    hotspots = []
    
    for point in flow_points_data:
        if point.get("current_speed", 0) > 0 and point.get("free_flow_speed", 0) > 0:
            speed_ratio = point["current_speed"] / point["free_flow_speed"]
            congestion_score = (1 - speed_ratio) * 100
            
            congestion_score *= random.uniform(0.9, 1.1)
            congestion_score = min(100, congestion_score)
            
            delay_minutes = (point.get("current_travel_time", 0) - point.get("free_flow_travel_time", 0)) / 60
            
            if congestion_score > 30:
                severity = "Severe" if congestion_score > 60 else "High" if congestion_score > 40 else "Moderate"
                
                hotspots.append({
                    "location_name": point["location_name"],
                    "latitude": point["latitude"],
                    "longitude": point["longitude"],
                    "congestion_score": congestion_score,
                    "avg_speed": point["current_speed"],
                    "delay_minutes": delay_minutes,
                    "severity": severity
                })
    
    return sorted(hotspots, key=lambda x: x["congestion_score"], reverse=True)

def calculate_daily_kpi(flow_points_data, incidents_count):
    if not flow_points_data:
        return None
    
    base_vehicles = len(flow_points_data) * 150
    total_vehicles = int(base_vehicles * random.uniform(0.9, 1.1))
    
    speeds = [point.get("current_speed", 45) for point in flow_points_data]
    avg_speed = sum(speeds) / len(speeds) if speeds else 45
    avg_speed *= random.uniform(0.95, 1.05)
    
    travel_times = [point.get("current_travel_time", 0) / 60 for point in flow_points_data]
    avg_travel_time = sum(travel_times) / len(travel_times) if travel_times else 0
    avg_travel_time *= random.uniform(0.95, 1.05)
    
    delays = [point.get("current_travel_time", 0) - point.get("free_flow_travel_time", 0) for point in flow_points_data]
    avg_delay = sum(delays) / len(delays) if delays else 0
    avg_delay *= random.uniform(0.9, 1.1)
    
    if avg_delay == 0:
        congestion_level = "Low"
    elif avg_delay < 300:
        congestion_level = "Moderate"
    elif avg_delay < 600:
        congestion_level = "High"
    else:
        congestion_level = "Severe"
    
    change_percent = random.uniform(5.0, 20.0)
    
    return {
        "total_vehicles": total_vehicles,
        "avg_speed": avg_speed,
        "congestion_level": congestion_level,
        "avg_travel_time_minutes": avg_travel_time,
        "total_incidents": incidents_count,
        "change_percent": change_percent
    }

def calculate_hourly_aggregates(route_name, flow_point_data):
    now = datetime.now()
    
    current_speed = flow_point_data.get("current_speed", 45)
    travel_time = flow_point_data.get("current_travel_time", 0) / 60
    delay = flow_point_data.get("current_travel_time", 0) - flow_point_data.get("free_flow_travel_time", 0)
    
    current_speed *= random.uniform(0.95, 1.05)
    travel_time *= random.uniform(0.95, 1.05)
    delay *= random.uniform(0.9, 1.1)
    
    if delay == 0:
        congestion_level = "Low"
    elif delay < 300:
        congestion_level = "Moderate"
    elif delay < 600:
        congestion_level = "High"
    else:
        congestion_level = "Severe"
    
    vehicle_count = int(150 * random.uniform(0.8, 1.2))
    
    return {
        "route_name": route_name,
        "hour_of_day": now.hour,
        "day_of_week": now.weekday(),
        "date": now.date(),
        "timestamp": now,
        "avg_speed": current_speed,
        "avg_travel_time": travel_time,
        "avg_delay": delay / 60,
        "vehicle_count": vehicle_count,
        "congestion_level": congestion_level
    }

def calculate_route_health_score(flow_point_data, incident_count):
    current_speed = flow_point_data.get("current_speed", 45)
    free_flow_speed = flow_point_data.get("free_flow_speed", 60)
    current_travel_time = flow_point_data.get("current_travel_time", 0)
    free_flow_travel_time = flow_point_data.get("free_flow_travel_time", 0)
    
    base_score = 100
    
    if current_travel_time > 0 and free_flow_travel_time > 0:
        delay_percent = ((current_travel_time - free_flow_travel_time) / free_flow_travel_time) * 100
        base_score -= min(delay_percent * 0.4, 40)
    
    if current_speed > 0 and free_flow_speed > 0:
        speed_ratio = current_speed / free_flow_speed
        if speed_ratio < 1:
            base_score -= (1 - speed_ratio) * 30
    
    base_score -= min(incident_count * 5, 20)
    
    base_score *= random.uniform(0.95, 1.05)
    
    return max(0, min(100, int(base_score)))

def get_congestion_level(health_score):
    if health_score >= 80:
        return "Low"
    elif health_score >= 60:
        return "Moderate"
    elif health_score >= 40:
        return "High"
    else:
        return "Severe"

def get_recommendation(health_score, delay_percent):
    if health_score >= 80:
        return "Clear route - Good time to travel"
    elif health_score >= 60:
        return "Moderate delays expected - Consider alternatives"
    elif health_score >= 40:
        return "Heavy traffic - Delay likely"
    else:
        return "Severe congestion - Avoid if possible"

def save_traffic_flow(flow_data):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        sql = """
            INSERT INTO traffic_flow
            (location_name, timestamp, latitude, longitude, current_speed,
             free_flow_speed, current_travel_time, free_flow_travel_time,
             confidence, road_closure)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            timestamp = VALUES(timestamp),
            latitude = VALUES(latitude),
            longitude = VALUES(longitude),
            current_speed = VALUES(current_speed),
            free_flow_speed = VALUES(free_flow_speed),
            current_travel_time = VALUES(current_travel_time),
            free_flow_travel_time = VALUES(free_flow_travel_time),
            confidence = VALUES(confidence),
            road_closure = VALUES(road_closure)
        """
        values = (
            flow_data["location_name"],
            flow_data["timestamp"],
            flow_data["latitude"],
            flow_data["longitude"],
            flow_data["current_speed"],
            flow_data["free_flow_speed"],
            flow_data["current_travel_time"],
            flow_data["free_flow_travel_time"],
            flow_data["confidence"],
            flow_data["road_closure"]
        )
        cursor.execute(sql, values)
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Saving traffic flow failed: {e}")

def save_incident(incident_data):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        sql = """
            INSERT INTO traffic_incidents
            (incident_id, incident_type, severity, description, latitude, longitude,
             start_time, end_time, last_updated, delay_seconds, affected_road)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            last_updated = VALUES(last_updated),
            end_time = VALUES(end_time),
            delay_seconds = VALUES(delay_seconds)
        """
        values = (
            incident_data["incident_id"],
            incident_data["incident_type"],
            incident_data["severity"],
            incident_data["description"],
            incident_data["latitude"],
            incident_data["longitude"],
            incident_data["start_time"],
            incident_data["end_time"],
            incident_data["last_updated"],
            incident_data["delay_seconds"],
            incident_data["affected_road"]
        )
        cursor.execute(sql, values)
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Saving incident failed: {e}")

def save_route_health(location_name, health_score, congestion_level, delay_percent, incident_count, recommendation):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        sql = """
            INSERT INTO route_health
            (route_name, timestamp, health_score, congestion_level, 
             delay_percentage, incident_count, recommendation)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            timestamp = VALUES(timestamp),
            health_score = VALUES(health_score),
            congestion_level = VALUES(congestion_level),
            delay_percentage = VALUES(delay_percentage),
            incident_count = VALUES(incident_count),
            recommendation = VALUES(recommendation)
        """
        values = (
            location_name,
            datetime.utcnow(),
            health_score,
            congestion_level,
            delay_percent,
            incident_count,
            recommendation
        )
        cursor.execute(sql, values)
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Saving route health failed: {e}")

def save_hourly_aggregates(hourly_data):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        sql = """
            INSERT INTO hourly_aggregates
            (route_name, hour_of_day, day_of_week, date, timestamp,
             avg_speed, avg_travel_time, avg_delay, vehicle_count, congestion_level)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            timestamp = VALUES(timestamp),
            avg_speed = VALUES(avg_speed),
            avg_travel_time = VALUES(avg_travel_time),
            avg_delay = VALUES(avg_delay),
            vehicle_count = VALUES(vehicle_count),
            congestion_level = VALUES(congestion_level)
        """
        values = (
            hourly_data["route_name"],
            hourly_data["hour_of_day"],
            hourly_data["day_of_week"],
            hourly_data["date"],
            hourly_data["timestamp"],
            hourly_data["avg_speed"],
            hourly_data["avg_travel_time"],
            hourly_data["avg_delay"],
            hourly_data["vehicle_count"],
            hourly_data["congestion_level"]
        )
        cursor.execute(sql, values)
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Saving hourly aggregates failed: {e}")

def save_vehicle_classification(location_name, distribution):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        timestamp = datetime.utcnow()
        
        for item in distribution:
            sql = """
                INSERT INTO vehicle_classification
                (location_name, vehicle_type, timestamp, count, percentage)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                timestamp = VALUES(timestamp),
                count = VALUES(count),
                percentage = VALUES(percentage)
            """
            values = (location_name, item["vehicle_type"], timestamp, item["count"], item["percentage"])
            cursor.execute(sql, values)
        
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Saving vehicle classification failed: {e}")

def save_speed_distribution(location_name, distribution):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        timestamp = datetime.utcnow()
        
        for item in distribution:
            sql = """
                INSERT INTO speed_distribution
                (location_name, speed_range, timestamp, vehicle_count, percentage)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                timestamp = VALUES(timestamp),
                vehicle_count = VALUES(vehicle_count),
                percentage = VALUES(percentage)
            """
            values = (location_name, item["speed_range"], timestamp, item["vehicle_count"], item["percentage"])
            cursor.execute(sql, values)
        
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Saving speed distribution failed: {e}")

def save_congestion_hotspots(hotspots):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        timestamp = datetime.utcnow()
        
        for hotspot in hotspots:
            sql = """
                INSERT INTO congestion_hotspots
                (location_name, latitude, longitude, timestamp, congestion_score,
                 avg_speed, delay_minutes, severity)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                latitude = VALUES(latitude),
                longitude = VALUES(longitude),
                timestamp = VALUES(timestamp),
                congestion_score = VALUES(congestion_score),
                avg_speed = VALUES(avg_speed),
                delay_minutes = VALUES(delay_minutes),
                severity = VALUES(severity)
            """
            values = (
                hotspot["location_name"],
                hotspot["latitude"],
                hotspot["longitude"],
                timestamp,
                hotspot["congestion_score"],
                hotspot["avg_speed"],
                hotspot["delay_minutes"],
                hotspot["severity"]
            )
            cursor.execute(sql, values)
        
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Saving congestion hotspots failed: {e}")

def save_daily_kpi(kpi_data):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        sql = """
            INSERT INTO daily_kpi
            (date, timestamp, total_vehicles, avg_speed, congestion_level,
             avg_travel_time_minutes, total_incidents, change_percent)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            timestamp = VALUES(timestamp),
            total_vehicles = VALUES(total_vehicles),
            avg_speed = VALUES(avg_speed),
            congestion_level = VALUES(congestion_level),
            avg_travel_time_minutes = VALUES(avg_travel_time_minutes),
            total_incidents = VALUES(total_incidents),
            change_percent = VALUES(change_percent)
        """
        values = (
            datetime.now().date(),
            datetime.utcnow(),
            kpi_data["total_vehicles"],
            kpi_data["avg_speed"],
            kpi_data["congestion_level"],
            kpi_data["avg_travel_time_minutes"],
            kpi_data["total_incidents"],
            kpi_data["change_percent"]
        )
        cursor.execute(sql, values)
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Saving daily KPI failed: {e}")

def cleanup_old_data():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cutoff_time = datetime.utcnow() - timedelta(minutes=DATA_RETENTION_MINUTES)
        
        tables = [
            "traffic_flow",
            "traffic_incidents",
            "route_health",
            "hourly_aggregates",
            "vehicle_classification",
            "speed_distribution",
            "congestion_hotspots"
        ]
        
        for table in tables:
            if table == "traffic_incidents":
                cursor.execute(f"""
                    DELETE FROM {table} 
                    WHERE end_time < %s AND end_time IS NOT NULL
                """, (cutoff_time,))
            else:
                cursor.execute(f"""
                    DELETE FROM {table} 
                    WHERE timestamp < %s
                """, (cutoff_time,))
            
            deleted_rows = cursor.rowcount
            if deleted_rows > 0:
                print(f"Cleaned up {deleted_rows} old records from {table}")
        
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Data cleanup failed: {e}")

def run_comprehensive_pipeline():
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Timestamp: {timestamp}")
    
    flow_points_collected = []
    
    print("PHASE 1: Traffic Flow Data Collection")
    
    for idx, point in enumerate(TRAFFIC_FLOW_POINTS, 1):
        print(f"[{idx}/{len(TRAFFIC_FLOW_POINTS)}] {point['name']}")
        
        flow_data = fetch_traffic_flow(point["lat"], point["lon"], point["zoom"])
        
        if flow_data:
            parsed = parse_traffic_flow(flow_data, point["name"])
            if parsed:
                save_traffic_flow(parsed)
                flow_points_collected.append(parsed)
                speed_ratio = (parsed["current_speed"] / parsed["free_flow_speed"] * 100) if parsed["free_flow_speed"] > 0 else 0
                print(f"Speed: {parsed['current_speed']:.0f} km/h ({speed_ratio:.0f}% of free flow)")
        
        time.sleep(0.3)
    
    print()
    
    print("PHASE 2: Incident Detection")
    
    incident_data = fetch_incidents(INCIDENT_BBOX)
    incident_count = 0
    
    if incident_data:
        incidents = parse_incidents(incident_data)
        incident_count = len(incidents)
        print(f"Found {incident_count} active incidents")
        
        for incident in incidents[:5]:
            save_incident(incident)
            print(f"Incident: {incident['incident_type']}: {incident['description'][:50]}...")
        
        if incident_count > 5:
            print(f"... and {incident_count - 5} more incidents")
    else:
        print("No active incidents detected")
    
    print()
    
    print("PHASE 3: Route Health Scores")
    
    for point in flow_points_collected:
        health_score = calculate_route_health_score(point, incident_count)
        congestion = get_congestion_level(health_score)
        
        delay_percent = 0
        if point.get("current_travel_time", 0) > 0 and point.get("free_flow_travel_time", 0) > 0:
            delay_percent = ((point["current_travel_time"] - point["free_flow_travel_time"]) / 
                            point["free_flow_travel_time"]) * 100
        
        recommendation = get_recommendation(health_score, delay_percent)
        
        save_route_health(point["location_name"], health_score, congestion, delay_percent, incident_count, recommendation)
        
        status = "Good" if health_score >= 80 else "Moderate" if health_score >= 60 else "Poor" if health_score >= 40 else "Severe"
        print(f"{point['location_name']}: Score {health_score}/100 - {congestion}")
    
    print()
    
    print("PHASE 4: Dashboard Metrics Calculation")
    
    if flow_points_collected:
        for point in flow_points_collected:
            vehicle_dist = calculate_vehicle_distribution([point])
            save_vehicle_classification(point["location_name"], vehicle_dist)
            
            speed_dist = calculate_speed_distribution([point])
            save_speed_distribution(point["location_name"], speed_dist)
        
        print(f"Vehicle classification saved for {len(flow_points_collected)} locations")
        print(f"Speed distribution saved for {len(flow_points_collected)} locations")
        
        hotspots = identify_congestion_hotspots(flow_points_collected)
        if hotspots:
            save_congestion_hotspots(hotspots)
            print(f"Identified {len(hotspots)} congestion hotspots")
    
    for point in flow_points_collected:
        hourly_agg = calculate_hourly_aggregates(point["location_name"], point)
        save_hourly_aggregates(hourly_agg)
    
    if flow_points_collected:
        daily_kpi = calculate_daily_kpi(flow_points_collected, incident_count)
        if daily_kpi:
            save_daily_kpi(daily_kpi)
            print(f"Daily KPIs calculated: {daily_kpi['total_vehicles']} vehicles, {daily_kpi['avg_speed']:.1f} km/h avg speed")
    
    print()
    
    print("PHASE 5: Data Cleanup")
    
    cleanup_old_data()
    
    print("Pipeline Execution Completed")

@app.route('/')
def home():
    return jsonify({
        "status": "Traffic Ingestion Service",
        "version": "1.0",
        "timestamp": datetime.utcnow().isoformat()
    })

@app.route('/health')
def health():
    return jsonify({"status": "healthy"}), 200

@app.route('/status')
def status():
    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) as count FROM traffic_flow")
            result = cursor.fetchone()
            count = result[0] if result else 0
        connection.close()
        
        return jsonify({
            "status": "Connected",
            "records": count,
            "timestamp": datetime.utcnow().isoformat()
        }), 200
    except Exception as e:
        return jsonify({
            "status": "Error",
            "error": str(e)
        }), 500

@app.route('/init-db', methods=['GET', 'POST'])
def init_database():
    connection = None
    try:
        create_tables()
        
        return jsonify({
            "status": "Database initialized",
            "tables": ["traffic_flow", "traffic_incidents", "route_health", 
                      "hourly_aggregates", "vehicle_classification", 
                      "speed_distribution", "congestion_hotspots", "daily_kpi"]
        }), 200
        
    except Exception as e:
        return jsonify({"status": "Error", "error": str(e)}), 500

@app.route('/ingest', methods=['GET', 'POST'])
def ingest_data():
    connection = None
    success_count = 0
    incident_count = 0
    
    try:
        connection = get_db_connection()
        
        for point in TRAFFIC_FLOW_POINTS:
            flow_data = fetch_traffic_flow(point["lat"], point["lon"], point["zoom"])
            if flow_data:
                parsed = parse_traffic_flow(flow_data, point["name"])
                if parsed:
                    save_traffic_flow(parsed)
                    success_count += 1
        
        incident_data = fetch_incidents(INCIDENT_BBOX)
        if incident_data:
            incidents = parse_incidents(incident_data)
            incident_count = len(incidents)
            for incident in incidents:
                save_incident(incident)
        
        return jsonify({
            "status": "success",
            "flow_points": success_count,
            "incidents": incident_count,
            "timestamp": datetime.utcnow().isoformat()
        }), 200
        
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    
    finally:
        if connection:
            connection.close()

@app.route('/run-pipeline', methods=['GET', 'POST'])
def run_pipeline():
    try:
        run_comprehensive_pipeline()
        return jsonify({
            "status": "success",
            "message": "Pipeline completed successfully",
            "timestamp": datetime.utcnow().isoformat()
        }), 200
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port, debug=False)
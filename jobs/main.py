from confluent_kafka import SerializingProducer
import os
import simplejson as json
from datetime import datetime, timedelta
import random
import uuid

LONDON_COORDINATES = { "latitude": 51.5074, "longitude": -0.1278 }
BIRMINGHAM_COORDINATES = { "latitude": 52.4862, "longitude": -1.8904 }

# Calculate the movement increment
LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES["latitude"] - LONDON_COORDINATES["latitude"]) / 100
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES["longitude"] - LONDON_COORDINATES["longitude"]) / 100

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9093')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'VEHICLE_TOPIC')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'GPS_topic')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'TRAFFIC_topic')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'WEATHER_topic')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'EMERGENCY_topic')

start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()

def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time

def simulate_vehicle_movement():
    global start_location
    # Towards Birmingham
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT
    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)
    return start_location

def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'device_id': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(10, 40),
        'direction': 'North-East',
        'make': 'BMW',
        'model': 'C500',
        'year': 2024,
        'fuelType': 'Hybrid'
    }

def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data=generate_gps_data
        print(vehicle_data)
        # break

def getProducer():
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print("Kafka error:", err)
    }
    producer = SerializingProducer(producer_config)
    try:
        simulate_journey(producer, 'abc started')
        print("Simulation complete")
    except KeyboardInterrupt:
        print("Simulation ended by the user")
    except Exception as e:
        print("Error:", e)

if __name__ == '__main__':
    getProducer()

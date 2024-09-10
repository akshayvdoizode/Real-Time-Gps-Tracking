import random
import uuid
import json
import time
from datetime import timedelta, datetime
from geopy.distance import geodesic
from confluent_kafka import SerializingProducer
import logging
import threading
from flask import Flask,Response,render_template
from flask_cors import CORS
from constants import *


app = Flask(__name__)

CORS(app)  

# Logging
logging.basicConfig(level=logging.INFO)

# Globals for time and location
start_time = datetime.now()


def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time


def simulate_vehicle_movement():
    global start_location
    start_location['latitude'] += LATITUDE_INCREMENT + random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += LONGITUDE_INCREMENT + random.uniform(-0.0005, 0.0005)
    return start_location


def has_arrived(location, destination):
    return geodesic((location['latitude'], location['longitude']),
                    (destination['latitude'], destination['longitude'])).miles < 0.1

def generate_gps_data(deviceId, timestamp, vehicle_type='private'):
    return {
        'id': uuid.uuid4(),
        'deviceId': deviceId,
        'timestamp': timestamp,
        'speed': random.uniform(0, 40),
        'direction': 'NorthEast',
        'vehicle_type': vehicle_type
    }

def generate_traffic_camera_data(deviceId, timestamp, cameraId, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': deviceId,
        'cameraId': cameraId,
        'timestamp': timestamp,
        'location': location,
        'snapshot': 'Image',
    }


def generate_weather_data(deviceId, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': deviceId,
        'timestamp': timestamp,
        'location': location,
        'temperature': random.uniform(-5, 26),
        'weatherCondition': random.choice(['Sunny', 'Cloudy', 'Rainy', 'Snowy']),
        'precipitation': random.uniform(0, 25),
        'windSpeed': random.uniform(0, 100),
        'humidity': random.randint(0, 100),
        'AQI': random.uniform(0, 500),
    }


def generate_emergency_incident_data(deviceId, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': deviceId,
        'timestamp': timestamp,
        'location': location,
        'incidentId': uuid.uuid4(),
        'type': random.choice(['Accident', 'Fire', 'Medical', 'Police', 'None']),
        'status': random.choice(['Active', 'Resolved']),
        'description': "description"
    }


def generate_vehicle_data(deviceId):
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'deviceId': deviceId,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(10, 40),
        'direction': 'North-East',
        'make': 'BMW',
        'model': 'C500',
        'year': 2024,
        'fuelType': 'Hybrid'
    }


def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError("Obj of type is not JSON serializable", obj.__class__.__name__)


def produce_to_kafka(producer, topic, data):
    try:
        producer.produce(
            topic,
            key=str(data['id']),
            value=json.dumps(data, default=json_serializer).encode('utf-8'),
            on_delivery=delivery_report
        )
    except Exception as e:
        logging.error(f"Failed to produce to Kafka: {e}")


def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Failed to deliver message: {err}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")




def simulate_journey(producer, deviceId):
    while True:
        vehicle_data = generate_vehicle_data(deviceId)
        gps_data = generate_gps_data(deviceId, vehicle_data['timestamp'])
        traffic_cam_data = generate_traffic_camera_data(deviceId, vehicle_data['timestamp'], 'Nikon-cam123', vehicle_data['location'])
        weatherdata = generate_weather_data(deviceId, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(deviceId, vehicle_data['timestamp'], vehicle_data['location'])
        if(vehicle_data['location'][0]>=BIRMINGHAM_COORDINATES['latitude'] and
           vehicle_data['location'][1]<=BIRMINGHAM_COORDINATES['longitude']):
            print("Arrived at BIRMINGHAM")
            break

        produce_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_to_kafka(producer, TRAFFIC_TOPIC, traffic_cam_data)
        produce_to_kafka(producer, WEATHER_TOPIC, weatherdata)
        produce_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)
        time.sleep(1 + random.uniform(0, 3))


def getProducer():
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: logging.error(f'Kafka error: {err}')
    }

    producer = SerializingProducer(producer_config)
    try:
        simulate_journey(producer, 'user started')
        logging.info('Simulation complete')
    except KeyboardInterrupt:
        logging.info('Simulation ended by the user')
    except Exception as e:
        logging.error(f'Error: {e}')
    finally:
        producer.flush(timeout=30)
        logging.info("Producer has been flushed and closed")


@app.route('/events')
def sse():
    def generate():
        # This is a placeholder for your Kafka data retrieval logic
        # Replace this with code to retrieve data from Kafka or another source
        while True:
            data = json.dumps({'location': simulate_vehicle_movement()})
            yield f"data: {data}\n\n"
            time.sleep(1)

    return Response(generate(), mimetype='text/event-stream')

@app.route('/')
def index():
    return render_template('main.html') 


if __name__ == '__main__':
    threading.Thread(target=lambda: app.run(host='0.0.0.0',port=5000)).start()  # Run Flask on a separate thread
    getProducer()




from confluent_kafka import SerializingProducer
import os
import simplejson as json
from datetime import datetime, timedelta
import random
import uuid

LONDON_COORDINATES = { 'latitude': 51.5074, 'longitude': -0.1278 }
BIRMINGHAM_COORDINATES = { 'latitude': 52.4862, 'longitude': -1.8904 }
random.seed(42)

# Calculate the movement increment
LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['latitude'] - LONDON_COORDINATES['latitude']) / 100
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['longitude'] - LONDON_COORDINATES['longitude']) / 100

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

def generate_gps_data(deviceId,timestamp,vehicle_type='private'):
    return {
        'id':uuid.uuid4(),
        'deviceId':deviceId,
        'timestamp':timestamp,
        'speed':random.uniform(0,40),
        'direction':'NorthEast',
        'vehicle_type':vehicle_type
    }

def generate_traffic_camera_data(deviceId,timestamp,cameraId,location):
    return {
        'id':uuid.uuid4(),
        'deviceId':deviceId,
        'cameraId':cameraId,
        'timestamp':timestamp,
        'location':location,
        'snapshot':'Image',
        
    }

def generate_weather_data(deviceId,timestamp,location):
       return {
        'id':uuid.uuid4(),
        'deviceId':deviceId,
        'timestamp':timestamp,
        'location':location,
        'temperature':random.uniform(-5,26),
        'weatherCondition':random.choice(['Sunny','Cloudy','Rainy','Sonwy']),
        'precipitation':random.uniform(0,25),
        'windSpeed':random.uniform(0,100),
        'humidity':random.randint(0,100),
        'AQI':random.uniform(0,500),
    }

def generate_emergency_incident_data(deviceId,timestamp,location):
        return {
        'id':uuid.uuid4(),
        'deviceId':deviceId,
        'timestamp':timestamp,
        'location':location,
        'incidentId':uuid.uuid4(),
        'type':random.choice(['Accident','Fire','Medical','Police','None']),
        'status':random.choice(['Active','Resolved']),
        'description':"description"
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
    if isinstance(obj,uuid.UUID):
        return str(obj)
    raise TypeError("Obj of type is not JSON serializable",obj.__class__.__name__)

def produce_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data,default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
    )



    
def delivery_report(err,msg):
    if err is not None:
        print("Failed",err)
    else:
        print("Success",msg)   




def simulate_journey(producer, deviceId):
    while True:
        vehicle_data = generate_vehicle_data(deviceId)
        gps_data=generate_gps_data(deviceId,vehicle_data['timestamp'])
        traffic_cam_data=generate_traffic_camera_data(deviceId,vehicle_data['timestamp'],'Nikon-cam123',vehicle_data['location'])
        weatherdata=generate_weather_data(deviceId,vehicle_data['timestamp'],vehicle_data['location'])
        emergency_incident_data=generate_emergency_incident_data(deviceId,vehicle_data['timestamp'],vehicle_data['location'])
        if(vehicle_data['location'][0]>=BIRMINGHAM_COORDINATES['latitude'] and
           vehicle_data['location'][0]<=BIRMINGHAM_COORDINATES['longitude']):
            print("Arrived at BIRMINGHAM")
            break
        produce_to_kafka(producer,VEHICLE_TOPIC,vehicle_data)
        produce_to_kafka(producer,GPS_TOPIC,gps_data)
        produce_to_kafka(producer,TRAFFIC_TOPIC,traffic_cam_data)
        produce_to_kafka(producer,WEATHER_TOPIC,weatherdata)
        produce_to_kafka(producer,EMERGENCY_TOPIC,emergency_incident_data)
        # time.sleep(5)
        

def getProducer():
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print('Kafka error:', err)
    }

    producer = SerializingProducer(producer_config)
    try:
        simulate_journey(producer, 'abc started')
        print('Simulation complete')
    except KeyboardInterrupt:
        print('Simulation ended by the user')
    except Exception as e:
        print(f'Error: {e}')
    finally:
        # Ensure all messages are flushed before terminating
        producer.flush(timeout=30)  # Wait up to 30 seconds for messages to be sent
        print("Producer has been flushed and closed")   

if __name__ == '__main__':
    getProducer()



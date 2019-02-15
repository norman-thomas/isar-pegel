import os

MQTT_BROKER = os.getenv('MQTT_BROKER', 'localhost')
MQTT_PORT = int(os.getenv('MQTT_PORT', '1883'))
MQTT_CLIENT_ID = os.getenv('MQTT_CLIENT_ID')
MQTT_USER = os.getenv('MQTT_USER', '')
MQTT_PASS = os.getenv('MQTT_PASS', '')

INTERVAL = int(os.getenv('INTERVAL', '5')) * 60 # minutes

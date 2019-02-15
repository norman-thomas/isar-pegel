import json
import datetime
import logging
import urllib.request
from contextlib import contextmanager

from bs4 import BeautifulSoup as Soup
import paho.mqtt.client as mqtt

from config import MQTT_BROKER, MQTT_PORT, MQTT_CLIENT_ID, MQTT_USER, MQTT_PASS

MQTT_TOPIC = 'outside/isar/water/{}'

LEVEL_URL = 'http://www.hnd.bayern.de/pegel/isar/muenchen-16005701/tabelle?setdiskr=15'
FLOW_URL = 'http://www.hnd.bayern.de/pegel/isar/muenchen-16005701/tabelle?methode=abfluss&setdiskr=15'
TEMPERATURE_URL = 'https://www.gkd.bayern.de/de/fluesse/wassertemperatur/isar/muenchen-16005701/messwerte/tabelle'
PARTICLE_URL = 'https://www.gkd.bayern.de/de/fluesse/schwebstoff/kelheim/muenchen-16005701/messwerte/tabelle?zr=woche&parameter=konzentration'

LEVEL_SELECTORS = (
    'tbody tr:nth-of-type(1) td:nth-of-type(1)',
    'tbody tr:nth-of-type(1) td:nth-of-type(2)'
)
FLOW_SELECTORS = (
    'tbody tr:nth-of-type(1) td:nth-of-type(1)',
    'tbody tr:nth-of-type(1) td:nth-of-type(2)'
)
TEMPERATURE_SELECTORS = (
    'table.tblsort tbody tr:nth-of-type(1) td:nth-of-type(1)',
    'table.tblsort tbody tr:nth-of-type(1) td:nth-of-type(2)'
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

@contextmanager
def connect():
    client = mqtt.Client(client_id=MQTT_CLIENT_ID)
    try:
        client.username_pw_set(MQTT_USER, MQTT_PASS)
        client.connect(MQTT_BROKER, MQTT_PORT)
        client.loop_write()
        yield client
    except Exception:
        logger.exception()
    finally:
        if client:
            client.disconnect()

def on_connect(client, userdata, flags, rc):
    pass

def _get_topic(k=None):
    return MQTT_TOPIC.format(k if k else 'all')

def send(client, data):
    for k, v in data.items():
        if k != "time" and v is not None:
            client.publish(_get_topic(k), payload=str(v), qos=1, retain=False)    
    client.publish(_get_topic(), payload=json.dumps(data), qos=1, retain=False)

def fetch_info():
    level = load_page(LEVEL_URL, LEVEL_SELECTORS)
    flow = load_page(FLOW_URL, FLOW_SELECTORS)
    temperature = load_page(TEMPERATURE_URL, TEMPERATURE_SELECTORS)

    return {
        'time': level['datetime'],
        'level': level['value'],
        'flow': flow['value'],
        'temperature': temperature['value']
    }

def load_page(url, selectors):
    content = urllib.request.urlopen(url).read()
    soup = Soup(content)

    datetime_str = soup.select(selectors[0])[0].text
    value = soup.select(selectors[1])[0].text

    dt = datetime.datetime.strptime(datetime_str, '%d.%m.%Y %H:%M')
    datetime_str = dt.isoformat()

    value = value.replace(',', '.').replace('\xa0', '')
    try:
        value = float(value)
    except ValueError:
        value = None

    return {
        'datetime': datetime_str,
        'value': value
    }

def lambda_handler(event, context):  # pylint: disable=unused-argument
    print(event)
    if isinstance(event, dict) and 'trigger' in event and event['trigger'] == 'cron':
        info = fetch_info()
        with connect() as client:
            send(client, info)
        return info
    return None

if __name__ == '__main__':
    data = fetch_info()
    print(data)
    with connect() as client:
        send(client, data)

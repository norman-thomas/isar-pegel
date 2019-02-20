import json
import time
import datetime
import logging
import urllib.request
from contextlib import contextmanager

from bs4 import BeautifulSoup as Soup
import paho.mqtt.client as mqtt

from config import MQTT_BROKER, MQTT_PORT, MQTT_CLIENT_ID, MQTT_USER, MQTT_PASS, INTERVAL

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

LOGGER = logging.getLogger('isar_pegel')
LOGGER.setLevel(logging.INFO)

@contextmanager
def connect():
    client = mqtt.Client(client_id=MQTT_CLIENT_ID)
    try:
        if MQTT_USER and MQTT_PASS:
            client.username_pw_set(MQTT_USER, MQTT_PASS)
        LOGGER.info('connecting to MQTT broker...')
        client.connect(MQTT_BROKER, MQTT_PORT)
        LOGGER.info('connected.')
        yield client
    except Exception:
        LOGGER.exception()
    finally:
        if client:
            client.disconnect()

def _get_topic(k=None):
    return MQTT_TOPIC.format(k if k else 'all')

def send(client, data):
    LOGGER.info('Reporting data via MQTT...')
    for k, v in data.items():
        if k != "time" and v is not None:
            client.publish(_get_topic(k), payload=str(v), qos=1, retain=False)    
    client.publish(_get_topic(), payload=json.dumps(data), qos=1, retain=False)
    client.loop_write()

def fetch_info():
    LOGGER.info('opening level page...')
    level = load_page(LEVEL_URL, LEVEL_SELECTORS)
    LOGGER.info('opening flow page...')
    flow = load_page(FLOW_URL, FLOW_SELECTORS)
    LOGGER.info('opening temperature page...')
    temperature = load_page(TEMPERATURE_URL, TEMPERATURE_SELECTORS)

    return {
        'time': level['datetime'],
        'level': level['value'],
        'flow': flow['value'],
        'temperature': temperature['value']
    }

def load_page(url, selectors):
    content = urllib.request.urlopen(url).read()
    soup = Soup(content, 'html.parser')

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
    with connect() as client:
        while True:
            data = fetch_info()
            LOGGER.info('data: %s', str(data))
            send(client, data)
            LOGGER.info('waiting %d mins...', INTERVAL // 60)
            time.sleep(INTERVAL)

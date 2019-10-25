#!/usr/bin/env python

from __future__ import print_function

import argparse
import json
import os
import pprint
import sys

# no estoy seguro si la raspi tiene python 2 o 3
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

import paho.mqtt.client as mqtt


# callbacks
def on_connect(client, userdata, flags, rc):
    print('conectado a MQTT: %s' (str(rc),))

def on_publish(client, obj, mid):
    print('publicado mensaje: %s' % (str(mid,)))

def on_log(client, obj, level, msg):
    print(msg)

def connect(hostname, username, token, port=1883):
    mqttc = mqtt.Client()
    # asigna callbacks
    mqttc.on_connect = on_connect
    mqttc.on_publish = on_publish
    mqttc.on_log = on_log
    mqttc.username_pw_set(username, token)
    mqttc.connect(hostname, port)

    return mqttc

def main(hostname, username, password, port, topic, finput):
    mqttc = connect(hostname, username, password, port)

    # parseando el archivo.json antes de enviarlo por MQTT puede ser lento
    # pero nos aseguramos que el formato sea JSON valido
    json_data = json.load(finput)

    try:
        mqttc.publish(topic, json.dumps(json_data))
    except Exception as e:
        print('error encontrado: %s' % (e,))
        return 2

    return

if __name__ == '__main__':
    default_mqtt_url = os.environ.get('MQTT_URL', 'mqtt://localhost:1883/test/topic')
    url = urlparse(default_mqtt_url)
    topic = url.path[1:] or 'test'
    port = url.port or 1883
    parser = argparse.ArgumentParser(description='Enviar datos a vikua.com via MQTT')
    parser.add_argument(
        '--host',
        help='host para conectarse a MQTT (default: %s)' % (url.hostname,),
        default=url.hostname,
    )
    parser.add_argument(
        '--port',
        type=int,
        help='puerto para conectarse a MQTT (default: %s)' % (port,),
        default=port,
    )
    parser.add_argument(
        '--username',
        help='usuario para conectarse a MQTT (default: %s)' % (url.username,),
        default=url.username,
    )
    parser.add_argument(
        '--password',
        help='password (token) para conectarse a MQTT (default: %s)' % (url.password,),
        default=url.password,
    )
    parser.add_argument(
        '--topic',
        help='topic para publicar mensajes en MQTT (default: %s)' % (topic,),
        default=topic,
    )
    parser.add_argument(
        'input',
        type=argparse.FileType('r'),
        help='archivo.json con estadisticas para enviar a MQTT (default: STDIN)',
        default=sys.stdin,
    )
    args = parser.parse_args()
    sys.exit(main(args.host, args.username, args.password, args.port, args.topic, args.input))

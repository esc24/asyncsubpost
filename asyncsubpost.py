#!/usr/bin/env python3

import asyncio
import json
import logging

import aiohttp
from hbmqtt.client import MQTTClient, ClientException
from hbmqtt.mqtt.constants import QOS_0

# MQTT
MQTT_URL = 'mqtt://127.0.0.1'
MQTT_TOPIC = 'restfulobs/weight'

# HTTP
#HTTP_URL = 'http://httpbin.org/post'
HTTP_URL = 'http://localhost:5000/v1/obs/'

async def listen():
    client = MQTTClient()
    await client.connect(MQTT_URL)
    await client.subscribe([(MQTT_TOPIC, QOS_0)])
    async with aiohttp.ClientSession() as http_session:
        try:
            for i in range(1, 100):
                message = await client.deliver_message()
                packet = message.publish_packet
                print("{}:  {} => {}".format(i,
                                             packet.variable_header.topic_name,
                                             packet.payload.data))
                print(type(packet.payload))
                print(type(packet.payload.data))
                data = json.dumps({'weight': float(packet.payload.data)})
                headers = {'content-type': 'application/json'}
                async with http_session.post(HTTP_URL,
                                             data=data,
                                             headers=headers) as resp:
                    print('HTTP: {}'.format(resp.status))
                    print(await resp.text())
            await client.unsubscribe([MQTT_TOPIC])
            await client.disconnect()
        except ClientException as e:
            logging.error(f"Client exception: {e}")

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(listen())

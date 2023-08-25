import random
import os
from paho.mqtt import client as mqtt_client
from paho.mqtt.client import Client
from rocketry import Rocketry
from dotenv import dotenv_values

from etl.energy import Energy_ETL
from utils.linkage import get_json_data, get_linkage_data

config = dotenv_values(".env")

# get linkage data
linkage_data = get_linkage_data(
    os.path.join(os.path.dirname(__file__), "json", "node_linkage.json")
)
subscribe_topics = get_json_data(
    os.path.join(os.path.dirname(__file__), "json", "sub_topics.json")
)
publish_topics = get_json_data(
    os.path.join(os.path.dirname(__file__), "json", "pub_topics.json")
)

sub_broker = config["SUB_BROKER"]
sub_port = int(config["SUB_PORT"])
sub_topics = subscribe_topics
sub_client_id = f"subscribe-{random.randint(0, 100)}"

pub_broker = config["PUB_BROKER"]
pub_port = int(config["PUB_PORT"])
pub_client_id = f"publish-{random.randint(0, 100)}"

energy_etl = Energy_ETL(linkage_data["energy_power_list"])

app = Rocketry()


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!", client)
    else:
        print("Failed to connect, return code %d\n", rc)


def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode()
    print(f"Received `{payload}` from `{topic}` topic")
    if "energy" in topic:
        energy_etl.power_receive(topic, payload)


def on_disconnect(userdata, rc, props):
    print(f"MQTT client was disconnected!")


# def connect_mqtt(broker, port) -> Client:
#     client = mqtt_client.Client(sub_client_id)
#     # client.username_pw_set(username, password)
#     client.connect(broker, port)
#     return client


def subscribe(client: mqtt_client, topic: str):
    client.subscribe(topic)


def run():
    # subscribe client
    global sub_client
    sub_client = mqtt_client.Client(sub_client_id)
    sub_client.on_connect = on_connect
    sub_client.on_disconnect = on_disconnect
    sub_client.on_message = on_message

    # publish client
    global pub_client
    pub_client = mqtt_client.Client(pub_client_id)
    pub_client.on_connect = on_connect
    pub_client.on_disconnect = on_disconnect

    energy_etl.set_sub_client(sub_client)
    energy_etl.set_pub_client(pub_client)

    sub_client.connect(sub_broker, sub_port)
    pub_client.connect(pub_broker, pub_port)

    for topic in sub_topics:
        subscribe(sub_client, topic)

    # sub_client.loop_forever()
    sub_client.loop_start()
    pub_client.loop_start()


@app.task("every 10 seconds")
def publish_mqtt():
    print("publish_mqtt")
    energy_etl.publish_sum_power(pub_client)


if __name__ == "__main__":
    run()
    app.run()
    # try:
    #     while True:
    #         pass
    # except KeyboardInterrupt:
    #     pass
    # if _client is not None:
    #     _client.loop_stop()
    #     _client.disconnect()

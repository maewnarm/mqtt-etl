import json
from typing import Any, Dict

from paho.mqtt.client import Client


class Energy_ETL:
    def __init__(self, linkage):
        self.linkage = linkage
        self.sub_client = None
        self.pub_client = None
        self.manu = {}
        self.line = {}
        self.sum = {"manufacturing": {}, "line": {}}

    def set_sub_client(self, client):
        self.sub_client = client

    def set_pub_client(self, client):
        self.pub_client = client

    # receive and store data
    def power_receive(self, topic: str, payload: str):
        print(self.linkage)
        keys = topic.split("/")
        data = json.loads(payload)
        self._filter_manu(data)
        self._filter_line(data)
        print(self.manu)
        print(self.line)

    def _filter_manu(self, data: Any):
        manu_node = self.linkage["manufacturing"]
        node = int(data["node_id"])
        for key, nodes in manu_node.items():
            if node in nodes:
                idx = nodes.index(node)
                if key in self.manu:
                    self.manu[key][idx] = float(data["value"])
                else:
                    self.manu[key] = [0] * len(nodes)
                    self.manu[key][idx] = float(data["value"])

    def _filter_line(self, data: Any):
        line_node = self.linkage["line"]
        node = int(data["node_id"])
        for key, nodes in line_node.items():
            if node in nodes:
                idx = nodes.index(node)
                if key in self.line:
                    self.line[key][idx] = float(data["value"])
                else:
                    self.line[key] = [0] * len(nodes)
                    self.line[key][idx] = float(data["value"])

    # summary and publish data
    def power_sum(self) -> Dict[str, Any]:
        for key, values in self.manu.items():
            self.sum["manufacturing"][key] = sum(values)
        for key, values in self.line.items():
            self.sum["line"][key] = sum(values)
        print(self.sum)

    def publish_sum_power(self, client: Client):
        self.power_sum()
        manu = self.sum["manufacturing"]
        line = self.sum["line"]
        # TODO mapping manu/line topic json
        # TODO publish by client

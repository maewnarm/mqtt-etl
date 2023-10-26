import json
from typing import Any, Dict, List
from pydantic import BaseModel
from datetime import datetime as dt
from paho.mqtt.client import Client


class PublishEnergy(BaseModel):
    LineCode: str
    LineName: str
    ProductionDate: str
    value: float
    timestamp: str


class Energy_ETL:
    def __init__(self, linkage):
        self.linkage = linkage
        self.sub_client: Client = None
        self.pub_client: Client = None
        self.division = "EPD"
        self.type = "E"
        self.manu = {}
        self.line = {}
        self.sum = {"manufacturing": {}, "line": {}, "factory": 0}

    def set_sub_client(self, client):
        self.sub_client = client
        # print("set_sub_client: ", client)

    def set_pub_client(self, client):
        self.pub_client = client

    # receive and store data
    def power_receive(self, topic: str, payload: str):
        # print("print power_receive linkage", self.linkage)
        keys = topic.split("/")
        data = json.loads(payload)
        # print("data: ", data)
        self._filter_manu(data)
        self._filter_line(data)

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
        # line_node = self.linkage["line"]
        line_code = self.linkage["lineCode"]
        node = int(data["node_id"])
        for key, line_data in line_code.items():
            if node in line_data["node_id"]:
                idx = line_data["node_id"].index(node)
                if key in self.line:
                    self.line[key][idx] = float(data["value"])
                else:
                    self.line[key] = [0] * len(line_data["node_id"])
                    self.line[key][idx] = float(data["value"])

    # summary and publish data
    def power_sum_m(self) -> Dict[str, Any]:
        for key, values in self.manu.items():
            self.sum["manufacturing"][key] = sum(values)
        list = []
        for m in self.sum["manufacturing"]:
            list.append(self.sum["manufacturing"][m])
        self.sum["factory"] = sum(list)

    def power_sum_l(self) -> Dict[str, Any]:
        for key, values in self.line.items():
            self.sum["line"][key] = sum(values)

    def publish_sum_power_m(self):
        # print("publish sum_power_m")
        self.power_sum_m()
        manu = self.sum["manufacturing"]
        # print("manu: ", manu)

        # print("factory: ", self.sum["factory"])
        topic = f"EPD/energy/E/realtime"
        data = PublishEnergy(
            LineCode=self.division,
            LineName=self.division,
            ProductionDate=dt.now().strftime("%Y-%m-%d %H:%M:%S"),
            value=self.sum["factory"],
            timestamp=dt.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        # print("topic: ", topic)
        json_data = json.dumps(data.__dict__)
        # print("data: ", json_data)
        self.pub_client.publish(topic=topic, payload=json_data)

        for m in manu:
            topic = f"{self.division}/{m}/energy/{self.type}/realtime"
            data = PublishEnergy(
                LineCode=m,
                LineName=m,
                ProductionDate=dt.now().strftime("%Y-%m-%d %H:%M:%S"),
                value=manu[m],
                timestamp=dt.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
            json_data = json.dumps(data.__dict__)
            # print("topic: ", topic)
            # print("data: ", json_data)
            self.pub_client.publish(topic=topic, payload=json_data)
        # print("•______________________________________________________________•")

    def publish_sum_power_l(self):
        # print("publish sum_power_l")
        self.power_sum_l()
        master = self.linkage
        line = self.sum["line"]

        for l in line:
            manf = master["lineCode"][l]["manufacturing_name"]
            sect = master["lineCode"][l]["section_code"]
            topic = f"{self.division}/{manf}/{sect}/{l}/energy/{self.type}/realtime"
            data = PublishEnergy(
                LineCode=l,
                LineName=master["lineCode"][l]["line_name"],
                ProductionDate=dt.now().strftime("%Y-%m-%d %H:%M:%S"),
                value=line[l],
                timestamp=dt.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
            json_data = json.dumps(data.__dict__)
            # print("topic: ", topic)
            # print("data: ", json_data)
            self.pub_client.publish(
                topic=topic,
                payload=json_data,
            )
        # print("________________________________________________________________")

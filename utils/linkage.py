import os

from utils.functions import read_json


def get_json_data(path: str):
    return read_json(path)


def get_linkage_data(path: str):
    energy_power_list = read_json(path)
    return {"energy_power_list": energy_power_list}

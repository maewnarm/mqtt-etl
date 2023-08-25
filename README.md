# MQTT ETL

Python script for extract a raw data and transform to required data form then load (publish, in this MQTT case) to server

## Available services

- Energy visualize
```
received real-time energy usage value via LoRa gateway with node_id to identify,
then grouping to division, department, line by node_id and publish to MQTT broker
which specific topic of each category will follow standard MQTT topic format.
```
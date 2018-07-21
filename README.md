## MQTT Coordinated consumer.

Kafka Motivated Coordinated Consumer for MQTT

## Auto Rebalance 


## Instruction to Run

```
Run multiple instances of consumer. They will divide number of partitions between them.

python consumer.py
python consumer.py

python producer.py   # Produce some messages using producer, and check if those reflects in consumers. Right now it will produce messages on random topics.

```

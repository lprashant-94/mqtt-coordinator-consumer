## MQTT Coordinated consumer.

Kafka Motivated Coordinated Consumer for MQTT

**First Cut (With partition number as arguments)**


## Instruction to Run

```
# Start Consumer in 2 terminal, one will read from first 5 partitions and other from remaining

python consumer.py 0 5
python consumer.py 5 10

python producer.py   # Produce some messages using producer, and check if those reflects in consumers. Right now it will produce messages on random topics.

```

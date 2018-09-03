## MQTT Coordinated consumer.

Kafka Motivated Coordinated Consumer for MQTT

`pip install mqtt-coordinated`

## MQTT Consumer CoordinatorManager

CoordinatorManager class is manager class for mqtt consumer. It lets you connect to a MQTT server and subscribe to multiple topics. It gives you on_message callback for actions after receiving new message.

```
>>> from mqtt import CoordinatorManager
>>> 
>>> manager = CoordinatorManager('my-manager', 'iot.eclipse.org')
>>> manager.start()
>>> 
>>> consumer = manager.coordinated_consumer
>>> consumer.on_message = on_message  # Pass callback name here.
>>> consumer.subscribe("house/bulb")
>>> consumer.poll(100)    # Batch message reading construct for streaming purpose
```

```
# Disconnect and stop consuming
>>> consumer.disconnect()
>>> manager.stop()
```

There are 2 methods for consuming events, 
- registering for on_message
- Reading messages in batches. Batches are internally stored in memory, and not stored on persistent disc for now.


## MQTT Producer CoordinatedProducer

CoordinatedProducer class is MQTT producer which will create number of partitions on MQTT topic. you can pass partition number or partition_key to this producer. Messages with same partition_key are garuanteed to be produced on same partition. 

```
>>> from mqtt import CoordinatedProducer
>>> producer = CoordinatedProducer('iot.eclipse.org')
>>> producer.publish_on_partition("house/bulb", "on") # Message will be published on random partition
>>> producer.publish_on_partition("house/bulb", "on", partition=5) # Message will be published on 5th partition
>>> producer.publish_on_partition("house/bulb", "on", partition_key='message_key') # All messages with partition_key will be published on same partition.
```

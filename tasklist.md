## TaskList

- [x] Auto Rebalance Number of partitions, whenever someone joins or dies.
- [ ] Documentation for coordinated producer and consumer. Write it inside classes itself.
- [ ] ClientId should be called groupid instead. Correct these concepts.
- [ ] Make current implementation exactly like paho mqtt classes. Share its interface so that it will be easy use and migrate.
- [ ] It needed write logic for primary node.

- [ ] Consumer can subscribe to multiple topics.
- [ ] Store Number of partitions on mqtt server on some topic.
- [ ] Test properly with network mosquitto
- [ ] Add partition Number into on message callback.
- [ ] Handle on disconnect event. Dont reconnect consumers after disconnect, just start rebalance. 
- [ ] Rebalance is still in doubt. It needs correct handling of messages while rebalancing.

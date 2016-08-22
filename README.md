# DistriMqtt
Distributed MQTT service in nodejs based on mosca and mqtt

Start any number of MQTT services (Peer Nodes).
Each peer publishes their information to all other peers.
When a peer starts, it resyncs with all other peers.

You end up with
- any number of MQTT servers
- the data of all servers is synchronized between all nodes
- in case of network split the data is resynchronized on reconnect based on publish date
- each peer saves it's local copy - on restart it will preload with local copy then resync with all other nodes

Todo:
- better storage for synchronization / saving between restarts


# toy-social-network
Social network backend in Scala/Spark

## Working with kafka
To start a kafka server and a zookeeper server from the kafka repertory:
```
> bin/zookeeper-server-start.sh config/zookeeper.properties

  INFO Reading configuration from: config/zookeeper.properties(org.apache.zookeeper.server.quorum.QuorumPeerConfig)
  ...

> bin/kafka-server-start.sh config/server.properties
  INFO Verifying properties (kafka.utils.VerifiableProperties)
  ...
```
Assuming kafka (port 9092) and zookeeper (port 2181) servers are up and running:
```
> kafka-topics.sh --create --zookeeper localhost:9092 --replication-factor 1 --partitions 1 --topic post

Created topic "post".
```

Check that the topic has been created:
```
> kafka-topics.sh --list --zookeeper localhost:2181

 post
```

Read data wrote to the topic (after running main.scala to write some datas):
```
> kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic post --from-beginning

 "id":"post0","updatedOn":"2018-07-01T22:39:42.390Z","author":"user0","text":"Some Text","image":"http://i.prntscr.com/XXS-8L2tR7id1MSgJDywoQ.png","deleted":false
```

## Working with Cassandra
Assuming Cassandra is up and running:
```
> cqlsh     # Starting cql shell
cqlsh> CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
```
To create a test keyspace.

Or, use this file this way:
```
> cqlsh -f src/main/scala/core/cassandra/create_users.cql
```

# Setting up Kafka 

## Set up topic

```
cd /usr/hdp/2.6.5.0-292/kafka/
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 2 --topic graph-in1
bin/kafka-topics.sh --list --zookeeper localhost:2181
bin/kafka-topics.sh --zookeeper localhost:2181 --describe --topic graph-in1
bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list ip-10-0-141-91.us-west-2.compute.internal:6667 --topic graph-in1 --time -1

```

## Ingest sample telemetry and test

```
cat /etc/graph/StormGraph/TestFiles/asaInput.log | bin/kafka-console-producer.sh --broker-list ip-10-0-141-91.us-west-2.compute.internal:6667 --topic graph-in1

```

## Test to make sure both partitions got filled

```
bin/kafka-console-consumer.sh --bootstrap-server ip-10-0-141-91.us-west-2.compute.internal:6667 --topic graph-in1 --offset 0 --partition 0
bin/kafka-console-consumer.sh --bootstrap-server ip-10-0-141-91.us-west-2.compute.internal:6667 --topic graph-in1 --offset 0 --partition 1
```

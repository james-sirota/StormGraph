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
## Connecting to Tinkerpop

```
:remote connect tinkerpop.server conf/remote-objects.yaml session
:remote console
```
## Setting up the configuration graph

```
map = new HashMap<String, Object>();

map.put("storage.backend","hbase");
map.put("storage.hostname","ip-10-0-141-91.us-west-2.compute.internal,ip-10-0-190-41.us-west-2.compute.internal");
map.put("storage.hbase.table","metron_graph");
map.put("storage.hbase.ext.zookeeper.znode.parent","/hbase-unsecure");
map.put("cache.db-cache "," true");
map.put("cache.db-cache-clean-wait "," 20");
map.put("cache.db-cache-time "," 180000");
map.put("cache.db-cache-size "," 0.5");
map.put("index.search.backend","solr");
map.put("index.search.solr.http-urls","http://ip-10-0-141-91.us-west-2.compute.internal:8983/solr,http://ip-10-0-190-41.us-west-2.compute.internal:8983/solr");
map.put("index.search.solr.mode","cloud");
map.put("index.search.solr.zookeeper-url","ip-10-0-141-91.us-west-2.compute.internal:2181/solr,ip-10-0-190-41.us-west-2.compute.internal:2181/solr");
map.put("index.search.solr.configset","janusgraph");

ConfiguredGraphFactory.createTemplateConfiguration(new MapConfiguration(map));
```

## Test on the graph of the gods

```
GraphOfTheGodsFactory.create(graph)
GraphOfTheGodsFactory.load(graph)
```

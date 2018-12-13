# StormGraph
This project is meant to be a capability extension on top of Apache Metron.  It plugs into the indexing topics, maps incoming messages to ontologies, and ingests them into JanusGraph

# Getting Started

First you need to make sure you are starting with a clear graph.  Connec to to the gremmelin server and type

```
g = JanusGraphFactory.open("conf/janusgraph-cassandra-es.properties")
JanusGraphFactory.drop(g)
g = JanusGraphFactory.open("conf/janusgraph-cassandra-es.properties")
g.traversal().V().count()
g.traversal().E().count()
```
This should create an empty graph with 0 edges and 0 nodes

Next you need to create an index

```
g.tx().rollback()
mgmt = g.openManagement()
name = mgmt.getOrCreatePropertyKey('valueKey')
mgmt.buildIndex('byNameComposite', Vertex.class).addKey(name).buildCompositeIndex()
mgmt.commit()
ManagementSystem.awaitGraphIndexStatus(g, 'byNameComposite').call()
```

The index creation will take a few mins.  Next you want to test and make sure the index is working

```
g.traversal().V().has('valueKey', 'user_82')
```

This traversal will not find anything since the DB is empty at this point.  If the index is not working you will get a message that says

```
WARN  o.j.g.t.StandardJanusGraphTx - Query requires iterating over all vertices
```

If you get the above DO NOT GO FURTHER.  Something went wrong. Re-create your index.  If you continue performance problems will make everything unusable

Here is how you sanity check to make sure everything is working

Lets say we have the following message

```
{"ip_src":"9.71.7.24","ip_dst":"108.54.40.12","username":"user_53"}
```

and the folowing ontology set

```
top.mapperbolt.mappings=ip_src,ip_dst,connectsTo,host,host;username,ip_src,uses,user,host
```

Two graph ontologies will be generated.  9.71.7.24 connectsTo 108.54.40.12 and user_53 uses 9.71.7.24

```
g.traversal().V().has('valueKey', 'user_53').hasLabel('user').outE('uses').inV().has('valueKey', '9.71.7.24')
g.traversal().V().has('valueKey', '9.71.7.24').hasLabel('host').outE('connectsTo').inV().has('valueKey', '108.54.40.12')
```
# Starting the topology

Run the start script:
```
startGraphTopology.sh
```

Or to run it locally (mostly for testing):
```
startGraphTopology_local.sh
```
And change the pom dependency for Storm artifacts to compile

# Graph Topology

The topology has 2 spouts, but only one can be specified as active.  First, there is a generatorSpout, which is used for load testing.  Second, there is a kafkaSpout which is an unmodified Storm kafka spout.  A message emmitted from a spout is assumed to be a JSON.  Then there is a mapper bolt that will map the JSON fields to an ontology with a relation.  Last, there is a JanusBolt that will write the ontologies into Janus 


# Important Settings

Topology settings are in graphtopology_config.conf

If deploying into a dev cluster make sure the following settings are set

```
top.localDeploy=false
```
if this is set to true the topology will be deployed locally, which is used for testing and debugging

Also if you want it to pull from kafka set:

```
top.generatorSpoutEnabled=false
```
otherwise it will generate synthetic data with the generatorSpout and put a load on your system

If you are connecting to kafka, make sure you give the kafkaSpout a list of valid brokers:

```
top.spout.kafka.bootStrapServers=[hostname]:6667
```

The most important part is mapping messages to ontologies.  Lets say we have this example

```
{"ip_src":"9.71.7.24","ip_dst":"108.54.40.12","username":"user_53"}
```

and we want to define 2 ontologies from this: ip_src connectsTo ip_dst and username uses ip_src


```
top.mapperbolt.allowedEdges=connectsTo,uses
top.mapperbolt.mappings=ip_src,ip_dst,connectsTo,host,host;username,ip_src,uses,user,host
```

Lastly you need to configure the indexer and the graph store.  Janus ships with a variety of configs.  For example, to use Casandra with ES you would:

```
top.graphbolt.backEndConfigLocation=src/test/resources/janusgraph-cassandra-es.properties
```
You can also use Hbase and Solr with appropriate configs.  This is a Janus config item and all the bolt is doing is propagating it to the writer.  There is no custom code here to connect. 


# DISCLAMER

This is a POC-grade proof of capability.  At this point it is not meant to run in production nor be included with the Metron project.  Absolutely no support is provided. USE AT YOUR OWN RISK

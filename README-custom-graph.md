# How to build a custom graph?

Here is an example of how to build a custom graph for your dataset. Lets say we have following data:

```
Event -> FlowTuple
FlowTuple -> SourceIP
FlowTuple -> DestinationIP
```

In the above exapmle we have some JSON data that has properties for an Event, FlowTuple, and IP Addresses.

Checkout following file: [azurensg_sample_record.json](./src/main/resources/azurensg_sample_record.json)


So now we can store the relationship among these entities as follows:

```
    Event     -> hasFlowTuple -> FlowTuple (via jsonId)
    FlowTuple -> hasSourceIp  -> IP (via ipSrcAddr)
    FlowTuple -> hasDestIp    -> IP (via ipDstAddr)
```

Checkout following file for more details: [GlobalSchema.scala](./src/main/scala/metron/graph/GlobalSchema.scala)


# How to import sample data into graph?

## Setup a Janus Graph instance locally

Steps defined here: https://gist.github.com/tuxdna/166dd41902c59ca0470252b5bf4f3dcd


## Build and run the Graph importer

Build the project

```
export CP=`mvn dependency:build-classpath | grep -A1 "Dependencies classpath:" | tail -1`
mvn clean compile
java -cp $CP:target/classes:target/test-classes metron.graph.TestMain ./src/main/config/graph-hbase-config.properties
```


[metron.graph.TestMain](./src/main/scala/metron/graph/TestMain.scala) will perform following steps:

 * Connect to graph as specified in [graph-hbase-config.properties](./src/main/config/graph-hbase-config.properties)
 * Initialize Graph Schema with properties and indices as defined in [GraphManager.scala](./src/main/scala/metron/graph/GraphManager.scala)
 * Read a single JSON record from [azurensg_sample_record.json](./src/main/resources/azurensg_sample_record.json), and add it to graph as defined in [AzureNSGDataImporter.scala](./src/main/scala/metron/graph/AzureNSGDataImporter.scala)


## Query some data from Graph


[QueryURL](http://localhost:8182/?gremlin=g.V().has('data_source_type','azurensg').hasLabel('flow_tuple').has('ipDstAddr','100.10.2.6').valueMap('ipSrcAddr','ipDstAddr','protocol'))


```
$ wget -q -O - "http://localhost:8182/?gremlin=g.V().has('data_source_type','azurensg').hasLabel('flow_tuple').has('ipDstAddr','100.10.2.6').valueMap('ipSrcAddr','ipDstAddr','protocol')" | python -mjson.tool
{
    "requestId": "db04e13c-15a2-4ccb-9f3b-57a822490712",
    "result": {
        "data": [
            {
                "ipDstAddr": [
                    "100.10.2.6"
                ],
                "ipSrcAddr": [
                    "100.10.1.2"
                ],
                "protocol": [
                    "T"
                ]
            }
        ],
        "meta": {}
    },
    "status": {
        "attributes": {},
        "code": 200,
        "message": ""
    }
}
```


# TODOs

Integrating with Apache Storm Bolt

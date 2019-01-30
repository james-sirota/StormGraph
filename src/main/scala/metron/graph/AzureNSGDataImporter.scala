package metron.graph

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.janusgraph.core.{JanusGraph, JanusGraphTransaction}
import org.json.simple.JSONObject

import metron.graph.ConstantsAndUtils._

case class AzureNSGRecord(ipDstPort: Integer,
                          rule: String,
                          mac: String,
                          protocol: String,
                          originalString: String,
                          ipDstAddr: String,
                          action: String,
                          ipSrcAddr: String,
                          timestamp: java.util.Date,
                          direction: String,
                          d: String,
                          systemId: String,
                          m: String,
                          version: String,
                          sourceType: String,
                          operationName: String,
                          jsonId: String,
                          tupleId: String,
                          ipSrcPort: Integer,
                          y: String,
                          resourceId: String,
                          guid: String,
                          time: java.util.Date,
                          category: String,
                          internalVersion: String
                         )

object AzureNSGRecord {

  def loadFromJson(o: JSONObject): AzureNSGRecord = {
    val ip_dst_port: java.lang.Integer = if (o.containsKey("ip_dst_port")) Integer.parseInt(o.get("ip_dst_port").toString) else 0
    val ip_src_port: java.lang.Integer = if (o.containsKey("ip_src_port")) Integer.parseInt(o.get("ip_src_port").toString) else 0
    val rule = if (o.containsKey("rule")) o.get("rule").toString else ""
    val mac = if (o.containsKey("mac")) o.get("mac").toString else ""
    val protocol = if (o.containsKey("protocol")) o.get("protocol").toString else ""
    val original_string = if (o.containsKey("original_string")) o.get("original_string").toString else ""
    val ip_dst_addr = if (o.containsKey("ip_dst_addr")) o.get("ip_dst_addr").toString else ""
    val action = if (o.containsKey("action")) o.get("action").toString else ""
    val ip_src_addr = if (o.containsKey("ip_src_addr")) o.get("ip_src_addr").toString else ""
    val timestamp = if (o.containsKey("timestamp")) o.get("timestamp").toString else ""
    val direction = if (o.containsKey("direction")) o.get("direction").toString else ""
    val d = if (o.containsKey("d")) o.get("d").toString else ""
    val system_id = if (o.containsKey("system_id")) o.get("system_id").toString else ""
    val m = if (o.containsKey("m")) o.get("m").toString else ""
    val version = if (o.containsKey("version")) o.get("version").toString else ""
    val source_type = if (o.containsKey("source_type")) o.get("source_type").toString else ""
    val operation_name = if (o.containsKey("operation_name")) o.get("operation_name").toString else ""
    val json_id = if (o.containsKey("json_id")) o.get("json_id").toString else ""
    val tuple_id = if (o.containsKey("tuple_id")) o.get("tuple_id").toString else ""
    val y = if (o.containsKey("y")) o.get("y").toString else ""
    val resource_id = if (o.containsKey("resource_id")) o.get("resource_id").toString else ""
    val guid = if (o.containsKey("guid")) o.get("guid").toString else ""
    val time = if (o.containsKey("time")) o.get("time").toString else ""
    val category = if (o.containsKey("category")) o.get("category").toString else ""
    val internal_version = if (o.containsKey("`_version_`")) o.get("`_version_`").toString else ""

    val r = AzureNSGRecord(
      ipDstPort = ip_dst_port,
      rule = rule,
      mac = mac,
      protocol = protocol,
      originalString = original_string,
      ipDstAddr = ip_dst_addr,
      action = action,
      ipSrcAddr = ip_src_addr,
      timestamp = parseDate(timestamp),
      direction = direction,
      d = d,
      systemId = system_id,
      m = m,
      version = version,
      sourceType = source_type,
      operationName = operation_name,
      jsonId = json_id,
      tupleId = tuple_id,
      ipSrcPort = ip_src_port,
      y = y,
      resourceId = resource_id,
      guid = guid,
      time = parseDate(time),
      category = category,
      internalVersion = internal_version
    )

    r

  }
}

object AzureNSGDataImporter {

  def extractEntities(r: AzureNSGRecord, tenantId: String = "demo") = {

    val event = Event(
      tenantId = tenantId,
      jsonId = r.jsonId,
      systemId = r.systemId,
      eventId = r.jsonId,
      resourceId = r.resourceId,
      eventTime = r.time,
      category = r.category,
      operationName = r.operationName,
      // other fields not available
      eventName = "",
      eventSource = "",
      eventType = "",
      eventVersion = "",
      region = "",
      accountId = "",
      threatsName = "",
      data_source_type = AZURE_NSG)

    val srcIpAddr = IPAddress(
      ip = r.ipSrcAddr,
      port = r.ipSrcPort,
      ipType = "ipv4",
      data_source_type = AZURE_NSG)

    val dstIpAddr = IPAddress(
      ip = r.ipDstAddr,
      port = r.ipDstPort,
      ipType = "ipv4",
      data_source_type = AZURE_NSG)

    val flowTuple = FlowTuple(
      jsonId = r.jsonId,
      tupleId = r.tupleId,
      mac = r.mac,
      timestamp = r.timestamp,
      protocol = r.protocol,
      direction = r.direction,
      action = r.action,
      rule = r.rule,
      version = r.version,
      ipSrcAddr = r.ipSrcAddr,
      sourcePort = r.ipSrcPort,
      destPort = r.ipDstPort,
      ipDstAddr = r.ipDstAddr,
      data_source_type = AZURE_NSG
    )

    (event, srcIpAddr, dstIpAddr, flowTuple)

  }


  def importAzureNSG(graph: JanusGraph, tenantId: String, dataSourceType: String, o: JSONObject) = {
    println(tenantId)
    println(dataSourceType)

    val r = AzureNSGRecord.loadFromJson(o)
    println(r)
    val (event, srcIpAddr, dstIpAddr, flowTuple) = AzureNSGDataImporter.extractEntities(r)
    println(event)
    println(flowTuple)
    println(srcIpAddr)
    println(dstIpAddr)


    val tx: JanusGraphTransaction = graph.newTransaction()

    val g: GraphTraversalSource = graph.traversal()
    println(g.V().count().next())

    AzureNSGDataImporter.importData(graph, tx, r, tenantId)

    g.tx.commit()
    println(g.V().count().next())
  }


  def importData(graph: JanusGraph, tx: JanusGraphTransaction, r: AzureNSGRecord, tenantId: String) = {

    /*
    Event     -> hasFlowTuple -> FlowTuple (via jsonId)
    FlowTuple -> hasSourceIp  -> IP (via ipSrcAddr)
    FlowTuple -> hasDestIp    -> IP (via ipDstAddr)
     */

    val (event, srcIpAddr, dstIpAddr, flowTuple) = extractEntities(r, tenantId)
    val eventVertex = GraphDAO.addEventToGraph(graph, tx, event, AZURE_NSG)
    val srcIpAddrVertex = GraphDAO.addIpAddrToGraph(graph, tx, srcIpAddr, AZURE_NSG)
    val dstIpAddrVertex = GraphDAO.addIpAddrToGraph(graph, tx, dstIpAddr, AZURE_NSG)
    val flowTupleVertex = GraphDAO.addFlowTupleToGraph(graph, tx, flowTuple, AZURE_NSG)

    addEdge(tx, eventVertex, flowTupleVertex, "hasFlowTuple")
    addEdge(tx, flowTupleVertex, srcIpAddrVertex, "hasSourceIp")
    addEdge(tx, flowTupleVertex, dstIpAddrVertex, "hasDestIp")
    tx.commit()
  }

  def currentTime = String.valueOf(System.currentTimeMillis)

  def addEdge(tx: JanusGraphTransaction, v1: Vertex, v2: Vertex, edgeLabel: String) = {
    println(s"Add edge: $edgeLabel")
    tx.getVertex(v1.id().toString.toLong).addEdge(edgeLabel, tx.getVertex(v2.id().toString.toLong), "createdTime", currentTime)
  }

}

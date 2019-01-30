package metron.graph

import org.apache.tinkerpop.gremlin.structure
import org.janusgraph.core.JanusGraphTransaction


object GraphDAO {

  import org.apache.tinkerpop.gremlin.structure.T

  def addFlowTupleToGraph(graph: structure.Graph, tx: JanusGraphTransaction, o: FlowTuple, source_type: String) = {
    println("Add FlowTuple")
    val g = graph.traversal()

    val v = g.V().hasLabel(o.vertexLabel)
      .has("data_source_type", source_type)
      .has("tupleId", o.tupleId)
      .tryNext().orElseGet { () =>
      tx.addVertex(T.label, o.vertexLabel,
        "jsonId", o.jsonId,
        "tupleId", o.tupleId,
        "mac", o.mac,
        "timestamp", o.timestamp,
        "protocol", o.protocol,
        "direction", o.direction,
        "action", o.action,
        "rule", o.rule,
        "version", o.version,
        "sourcePort", o.sourcePort,
        "destPort", o.destPort,
        "ipSrcAddr", o.ipSrcAddr,
        "ipDstAddr", o.ipDstAddr,
        "data_source_type", source_type)
    }
    v
  }

  def addIpAddrToGraph(graph: structure.Graph, tx: JanusGraphTransaction, o: IPAddress, source_type: String) = {
    println("Add IPAddress")
    val g = graph.traversal()

    val v = g.V().hasLabel(o.vertexLabel)
      .has("data_source_type", source_type)
      .has("ip", o.ip)
      .tryNext().orElseGet { () =>
      tx.addVertex(T.label, o.vertexLabel,
        "ip", o.ip,
        "port", o.port.asInstanceOf[java.lang.Integer],
        "ipType", o.ipType,
        "data_source_type", source_type)
    }
    v
  }


  def addEventToGraph(graph: structure.Graph, tx: JanusGraphTransaction, o: Event, source_type: String) = {
    println("Add Event")
    val g = graph.traversal()

    val v = g.V().hasLabel(o.vertexLabel)
      .has("data_source_type", source_type)
      .has("event_id", o.eventId)
      .tryNext().orElseGet { () =>
      tx.addVertex(T.label, o.vertexLabel,
        "type", o.eventType,
        "event_id", o.eventId,
        "event_name", o.eventName,
        "event_source", o.eventSource,
        "event_time", o.eventTime,
        "event_type", o.eventType,
        "event_version", o.eventVersion,
        "timestamp", o.timestamp,
        "region", o.region,
        "accountId", o.accountId,
        "threatsName", o.threatsName,
        "resourceId", o.resourceId,
        "severity", o.severity.asInstanceOf[java.lang.Double],
        "count", o.count.asInstanceOf[java.lang.Integer],
        "isAlert", o.isAlert.asInstanceOf[java.lang.Boolean],
        "data_source_type", source_type)
    }
    v
  }

}

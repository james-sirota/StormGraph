package metron.graph

import java.lang
import java.time.Instant
import java.util.{Date, UUID}

import org.apache.tinkerpop.gremlin.structure.Vertex
import org.janusgraph.core.attribute.Geoshape
import org.janusgraph.core.schema.JanusGraphManagement
import org.janusgraph.core.{JanusGraph, JanusGraphFactory}

import scala.reflect.runtime.universe._

object GraphManager {

  def getMemberFields[T: TypeTag] = {
    typeOf[T].members.collect {
      case m: MethodSymbol if m.isCaseAccessor => m
    }.map { x =>
      val g = x.getter
      val vtype = g.typeSignature.typeSymbol.getClass
      (g.name.toString, g.typeSignature.typeSymbol.name.toString)
    }.toList
  }


  def initializeSchema(graph: JanusGraph) = {
    //Never create new indexes while a transaction is active
    graph.tx().rollback()

    val mgmt = graph.openManagement()

    for {(fieldName, fieldType) <- getMemberFields[Event]} {
      GraphManager.addPropertyKeyAndIndex(graph, mgmt, fieldName, fieldType, Event.vertexLabel)
    }

    for {(fieldName, fieldType) <- getMemberFields[IPAddress]} {
      GraphManager.addPropertyKeyAndIndex(graph, mgmt, fieldName, fieldType, IPAddress.vertexLabel)
    }

    for {(fieldName, fieldType) <- getMemberFields[FlowTuple]} {
      GraphManager.addPropertyKeyAndIndex(graph, mgmt, fieldName, fieldType, FlowTuple.vertexLabel)
    }

    mgmt.commit()
  }

  /**
    *
    * @param mgmt      JanusGraphManagement instance
    * @param fieldName Name of the field
    * @param fieldType Type of the field which could be one of:
    *                  "Byte","Short","Int","Integer","Long","Float","Double","Date","UUID","Geoshape","Instant","String"
    */
  def addPropertyKeyAndIndex(graph: JanusGraph, mgmt: JanusGraphManagement, fieldName: String, fieldType: String, vertexLabel: String) = {

    // Only following fields can be indexed. Keep everything else as String index as a fallback.
    //    java.lang.Byte
    //    java.lang.Short
    //    java.lang.Integer
    //    java.lang.Long
    //    java.lang.Float
    //    java.lang.Double
    //    java.util.Date
    //    java.util.UUID
    //    org.janusgraph.core.attribute.Geoshape
    //    java.time.Instant
    //    java.lang.String

    val clazz = fieldType match {
      case "Byte" => classOf[lang.Byte]
      case "Short" => classOf[lang.Short]
      case "Int" => classOf[Integer]
      case "Integer" => classOf[Integer]
      case "Long" => classOf[lang.Long]
      case "Float" => classOf[lang.Float]
      case "Double" => classOf[lang.Double]
      case "Date" => classOf[Date]
      case "UUID" => classOf[UUID]
      case "Geoshape" => classOf[Geoshape]
      case "Instant" => classOf[Instant]
      case _ => classOf[String]
    }

    println(s"Property for field: $fieldName and type $fieldType ( $clazz )")
    val field = if (mgmt.containsPropertyKey(fieldName)) {
      println(s"Load existing property: $fieldName")
      mgmt.getPropertyKey(fieldName)
    } else {
      println(s"Create property: $fieldName")
      val k = mgmt.makePropertyKey(fieldName).dataType(classOf[String]).make()
      k
    }

    val idxWithlLabel = s"index_${vertexLabel}_${fieldName}"
    val idx1 = if (mgmt.containsGraphIndex(idxWithlLabel)) {
      println(s"Load existing label constraint index: $idxWithlLabel")
      mgmt.getGraphIndex(idxWithlLabel)
    } else {
      println(s"Create label constraint index: $idxWithlLabel")
      val idx = mgmt
        .buildIndex(idxWithlLabel, classOf[Vertex])
        .addKey(field)
        .indexOnly(mgmt.getOrCreateVertexLabel(vertexLabel))
        .buildCompositeIndex()
      idx
    }

    val idxName = s"index_$fieldName"
    val idx = if (mgmt.containsGraphIndex(idxName)) {
      println(s"Load existing index: $idxName")
      mgmt.getGraphIndex(idxName)
    } else {
      println(s"Create index: $idxName")
      val idx = mgmt.buildIndex(idxName, classOf[Vertex]).addKey(field).buildCompositeIndex()
      idx
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.size >= 1) {
      val conf = args(0)
      val graph: JanusGraph = JanusGraphFactory.open(conf)
      GraphManager.initializeSchema(graph)
      graph.close()
    } else {
      println("No configuration file for JanusGraph provided as CLI argument")
    }
  }
}

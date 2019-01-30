package metron.graph

import org.janusgraph.core.{JanusGraph, JanusGraphFactory}
import org.json.simple.JSONObject
import org.json.simple.parser.JSONParser

import scala.io.Source

object TestMain {

  def graphConnection(configFile: String) = {
    JanusGraphFactory.open(configFile)
  }

  def runAzureNsg(configFile: String) = {
    val graph: JanusGraph = graphConnection(configFile)

    val allData = getClass.getResource("/azurensg_sample_record.json")
    val parser = new JSONParser()
    val in = Source.fromURL(allData).bufferedReader()
    val jsonObject = parser.parse(in).asInstanceOf[JSONObject]

    GraphManager.initializeSchema(graph)
    GraphImporter.importJsonObject(graph, jsonObject)
    graph.close()
  }

  def main(args: Array[String]): Unit = {
    if (args.length >= 1) {
      val configFile = args(0)
      runAzureNsg(configFile)
    } else {
      println("Insufficient argument. Please provide janusgraph config file  as CLI argument.")
    }
  }
}

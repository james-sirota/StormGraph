package metron.graph

import org.janusgraph.core.JanusGraph
import org.json.simple.JSONObject


object GraphImporter {

  def importJsonObject(graph: JanusGraph, o: JSONObject) = {
    val sourceType = o.get("source.type").toString
    val parts = sourceType.split("_")
    val tenantId = parts(0)
    val dataSourceType = parts(1)

    dataSourceType match {
      case "azurensg" =>
        AzureNSGDataImporter.importAzureNSG(graph, tenantId, dataSourceType, o)
      case _ =>
        println("Unknown record type")
    }
  }

}

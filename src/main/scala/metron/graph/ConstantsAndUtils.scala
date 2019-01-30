package metron.graph

import java.text.ParseException
import java.util.Date

object VertexLabels {
  val EVENT = "event"
  val IP_ADDRESS = "ip_address"
  val FLOW_TUPLE = "flow_tuple"
}

object ConstantsAndUtils {
  val AZURE_NSG = "azurensg"

  val EVENT_DATE_FORMAT = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

  def parseDate(dateString: String): java.util.Date = {
    try {
      EVENT_DATE_FORMAT.parse(dateString)
    } catch {
      case _: ParseException => new Date()
    }
  }
}

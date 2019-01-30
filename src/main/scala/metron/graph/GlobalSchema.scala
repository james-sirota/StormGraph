package metron.graph

import java.util.Date

import VertexLabels._

trait VertexEntity {
  def vertexLabel: String
}

case class Event(
                  tenantId: String,
                  eventId: String,
                  eventName: String,
                  eventSource: String,
                  eventTime: Date,
                  eventType: String,
                  eventVersion: String,
                  region: String,
                  accountId: String,
                  threatsName: String,
                  resourceId: String,
                  severity: Double = 0,
                  count: Int = 0,
                  accountType: String = "",
                  connectionDirection: String = "",
                  api: String = "",
                  serviceName: String = "",
                  protocol: String = "",
                  location: String = "",
                  sourceData: String = "",
                  ip: String = "",
                  domain: String = "",
                  instanceId: String = "",
                  orgName: String = "",
                  userName: String = "",
                  userId: String = "",
                  accessKey: String = "",
                  url: String = "",
                  timestamp: Date = new Date(),
                  messageId: String = "",
                  topicArn: String = "",
                  subject: String = "",
                  message: String = "",
                  signatureVersion: String = "",
                  signature: String = "",
                  signingCertUrl: String = "",
                  unsubscribeUrl: String = "",
                  messageAttributes: String = "",
                  awsAccountId: String = "",
                  azureCloudAccountid: String = "",
                  entityAccountType: String = "",
                  entityRegion: String = "",
                  extraData: String = "",
                  friendlyType: String = "",
                  groupName: String = "",
                  groupTags: String = "",
                  isProtected: String = "",
                  msg: String = "",
                  resourceGroup: String = "",
                  ruleComplianceTag: String = "",
                  ruleDescription: String = "",
                  ruleIsDefault: String = "",
                  ruleLogic: String = "",
                  ruleName: String = "",
                  ruleRemediation: String = "",
                  ruleSeverity: String = "",
                  subscriptionId: String = "",
                  tags: String = "",
                  targetUserId: String = "",
                  actionType: String = "",
                  userAgent: String = "",
                  sourceIpAddrr: String = "",
                  errorCode: String = "",
                  errorMsg: String = "",
                  responseElement: String = "",
                  additionalEventData: String = "",
                  requestID: String = "",
                  apiVersion: String = "",
                  mgmtEvent: String = "",
                  readOnly: String = "",
                  resources: String = "",
                  recepientAcctId: String = "",
                  serviceEventDetails: String = "",
                  sharedEventID: String = "",
                  vpcEndPoint: String = "",
                  isAlert: Boolean = false,
                  jsonId: String = "",
                  systemId: String = "",
                  category: String = "",
                  operationName: String = "",
                  data_source_type: String
                ) extends VertexEntity {
  def vertexLabel = EVENT
}

object Event extends VertexEntity {
  def vertexLabel = EVENT
}

case class IPAddress(
                      ip: String,
                      port: Int,
                      ipType: String,
                      data_source_type: String
                    ) extends VertexEntity {
  def vertexLabel = IP_ADDRESS
}

object IPAddress extends VertexEntity {
  def vertexLabel = IP_ADDRESS
}


case class FlowTuple(
                      jsonId: String,
                      tupleId: String,
                      mac: String,
                      timestamp: java.util.Date,
                      protocol: String,
                      direction: String,
                      action: String,
                      rule: String,
                      version: String,
                      sourcePort: Integer,
                      destPort: Integer,
                      ipSrcAddr: String,
                      ipDstAddr: String,
                      data_source_type: String
                    ) extends VertexEntity {
  def vertexLabel = FLOW_TUPLE
}

object FlowTuple extends VertexEntity {
  def vertexLabel = FLOW_TUPLE
}

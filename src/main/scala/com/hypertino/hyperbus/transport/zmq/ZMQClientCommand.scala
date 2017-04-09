package com.hypertino.hyperbus.transport.zmq

import com.hypertino.hyperbus.model.{RequestBase, ResponseBase}
import com.hypertino.hyperbus.serialization.ResponseBaseDeserializer
import com.hypertino.hyperbus.transport.api.ServiceEndpoint
import monix.eval.Callback


sealed trait ZMQClientCommand
case object ZMQClientThreadStop extends ZMQClientCommand

case class ZMQClientAsk(message: String,
                        correlationId: String,
                        responseDeserializer: ResponseBaseDeserializer,
                        serviceEndpoint: ServiceEndpoint,
                        ttl: Long,
                        callback: Callback[ResponseBase]
                       ) extends ZMQClientCommand {
  def isExpired: Boolean = ttlRemaining < 0
  def ttlRemaining: Long = ttl - System.currentTimeMillis()
}

package com.hypertino.hyperbus.transport.zmq

import com.hypertino.hyperbus.model.{RequestBase, ResponseBase}
import com.hypertino.hyperbus.serialization.ResponseBaseDeserializer
import com.hypertino.hyperbus.transport.api.ServiceEndpoint
import monix.eval.Callback


sealed trait ZMQClientCommand
case object ZMQClientThreadStop extends ZMQClientCommand

class ZMQClientAsk(val message: String,
                   val correlationId: String,
                   val responseDeserializer: ResponseBaseDeserializer,
                   val serviceEndpoint: ServiceEndpoint,
                   val ttl: Long,
                   val callback: Callback[ResponseBase]
                  ) extends ZMQClientCommand with CancelableCommand {
  def isExpired: Boolean = ttlRemaining < 0

  def ttlRemaining: Long = ttl - System.currentTimeMillis()
}

package com.hypertino.hyperbus.transport.zmq

import scala.concurrent.duration.FiniteDuration

sealed trait ZMQServerCommand
case class ZMQServerThreadStop(timeout: FiniteDuration) extends ZMQServerCommand

case class ZMQServerResponse(
                              clientId: Array[Byte],
                              replyId: Array[Byte],
                              message: String
                            ) extends ZMQServerCommand

case class ZMQServerRequest(clientId: Array[Byte], replyId: Array[Byte], message: String) extends ZMQServerCommand
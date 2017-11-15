package com.hypertino.hyperbus.transport.zmq.utils

object ErrorCode {
  final val ZMQ_ASK_TIMEOUT = "zmq_ask_timeout"
  final val ZMQ_TRANSPORT_SHUTDOWN_REQUESTED = "zmq_transport_shutdown_requested"
  final val ZMQ_TRANSPORT_SHUTDOWN = "zmq_transport_shutdown"
  final val ZMQ_SUBSCRIPTION_NOT_FOUND = "zmq_subscription_not_found"
  final val ZMQ_UNHANDLED = "zmq_unhandled"
  final val ZMQ_OUTPUT_QUE_LIMIT_REACHED = "zmq_output_que_limit_reached"
}

/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperbus.transport.zmq.utils

object ErrorCode {
  final val ZMQ_ASK_TIMEOUT = "zmq_ask_timeout"
  final val ZMQ_ASK_FAILURE = "zmq_ask_failure"
  final val ZMQ_TRANSPORT_SHUTDOWN_REQUESTED = "zmq_transport_shutdown_requested"
  final val ZMQ_TRANSPORT_SHUTDOWN = "zmq_transport_shutdown"
  final val ZMQ_SUBSCRIPTION_NOT_FOUND = "zmq_subscription_not_found"
  final val ZMQ_UNHANDLED = "zmq_unhandled"
  final val ZMQ_OUTPUT_QUE_LIMIT_REACHED = "zmq_output_que_limit_reached"
}

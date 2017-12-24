/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperbus.transport.zmq
import java.nio.ByteBuffer
import java.nio.channels.Pipe
import java.util.concurrent.ConcurrentLinkedQueue

import com.typesafe.scalalogging.Logger
import org.zeromq.ZMQ.Socket

import scala.collection.mutable

trait ZMQCommandsConsumer[C] {
  protected val commandsPipe = Pipe.open()
  protected val commandsSink = commandsPipe.sink()
  commandsSink.configureBlocking(false)
  protected val commandsQueue = new ConcurrentLinkedQueue[C]
  protected def logger: Logger

  protected def fetchNewCommands(commandSource: Pipe.SourceChannel): Seq[C] = {
    val MAX_COUNT=1024
    val buffer = ByteBuffer.allocateDirect(MAX_COUNT)
    val commandCount = commandSource.read(buffer)
    if (commandCount > 0) {
      val commands = mutable.MutableList[C]()
      for (i ← 0 until commandCount) {
        commands += commandsQueue.poll()
      }
      commands
    } else {
      Seq.empty[C]
    }
  }

  protected def sendCommand(command: C): Unit = {
    val hash = (command.hashCode() % 0xFF).toByte
    val array = Array[Byte](hash)
    val buf = ByteBuffer.wrap(array)
    commandsQueue.add(command)
    commandsSink.write(buf)
  }

  protected def close(): Unit = {
    commandsSink.close()
  }

  protected def skipInvalidMessage(expecting: String, nullFrame: Array[Byte], socket: Socket): Unit = {
    var totalSkippedBytes: Int = nullFrame.length
    logger.trace(s"Got ${new String(nullFrame)} instead of null frame")
    while (socket.hasReceiveMore) {
      val frame = socket.recv()
      totalSkippedBytes += frame.length
      logger.trace(s"Skipped frame: ${new String(frame)}")
    }
    logger.warn(s"Got frame with ${nullFrame.length} bytes while expecting $expecting from $socket. Ignored message with $totalSkippedBytes bytes.")
  }
}

trait CancelableCommand {
  @volatile  private var _isCanceled: Boolean = false
  def isCanceled: Boolean = _isCanceled
  def cancel(): Unit = {
    _isCanceled = true
  }
}
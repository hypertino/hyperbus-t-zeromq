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
    val newCommands = mutable.MutableList[C]()
    var command: Option[C] = None
    var commandCount = 0
    do {
      command = Option(commandsQueue.poll())
      command.foreach { c ⇒
        commandCount += 1
        newCommands += c
      }
    } while(command.isDefined)

    if (commandCount > 0) {
      val buffer = ByteBuffer.allocateDirect(commandCount)
      commandSource.read(buffer)
    }
    newCommands
  }

  protected def sendCommand(command: C): Unit = {
    commandsQueue.add(command)
    commandsSink.write(ByteBuffer.wrap(Array[Byte](0)))
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
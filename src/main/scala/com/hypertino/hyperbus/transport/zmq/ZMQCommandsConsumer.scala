package com.hypertino.hyperbus.transport.zmq
import java.nio.ByteBuffer
import java.nio.channels.Pipe
import java.util.concurrent.ConcurrentLinkedQueue

import org.slf4j.Logger

import scala.collection.mutable

trait ZMQCommandsConsumer[C] {
  protected def commandsPipe: Pipe
  protected def commandsQueue: ConcurrentLinkedQueue[C]
  protected def log: Logger

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
}

trait ZMQCommandsProducer[C] {
  protected def commandsSink: Pipe.SinkChannel
  protected def commandsQueue: ConcurrentLinkedQueue[C]
  protected def log: Logger

  protected def sendCommand(command: C): Unit = {
    commandsQueue.add(command)
    commandsSink.write(ByteBuffer.wrap(Array[Byte](0)))
  }
}

trait CancelableCommand {
  @volatile  private var _isCanceled: Boolean = false
  def isCanceled: Boolean = _isCanceled
  def cancel(): Unit = {
    _isCanceled = true
  }
}
package com.hypertino.hyperbus.transport.zmq
import java.nio.ByteBuffer
import java.nio.channels.Pipe
import java.util.concurrent.ConcurrentLinkedQueue

import org.slf4j.Logger

import scala.collection.mutable

trait ZMQThreadBase[C] {
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
        if (log.isTraceEnabled) {
          log.trace(s"Got new command: $c")
        }
      }
    } while(command.isDefined)

    val buffer = ByteBuffer.allocateDirect(commandCount + 100)
    commandSource.read(buffer)
    newCommands
  }
}

package com.hypertino.hyperbus.transport.zmq

import java.nio.channels.Pipe
import java.util.concurrent.{ConcurrentLinkedQueue, LinkedBlockingQueue}

import monix.eval.Task
import monix.execution.Scheduler
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import org.zeromq.ZMQ.{Context, Poller}

import scala.collection.mutable
import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.control.NonFatal

private[transport] class ZMQServerThread(context: Context,
                                         processor: (ZMQServerRequest) ⇒ Task[Any],
                                         interface: String,
                                         port: Int,
                                         responseTimeout: FiniteDuration
                                        )
                                        (implicit scheduler: Scheduler) extends ZMQCommandsConsumer[ZMQServerCommand] {

  protected val log = LoggerFactory.getLogger(getClass)

  private val thread = new Thread(() ⇒ run(), "zmq-commands")
  thread.start()

  def reply(clientId: Array[Byte], replyId: Array[Byte], responseString: String): Unit = {
    sendCommand(new ZMQServerResponse(clientId, replyId, responseString))
  }

  def stop(duration: FiniteDuration): Unit = {
    sendCommand(new ZMQServerThreadStop(duration))
    commandsSink.close()
    thread.join(duration.toMillis)
  }

  protected def run(): Unit = {
    try {
      val processorCommandQueue = new LinkedBlockingQueue[ZMQServerCommand]()
      val processorThread = new Thread(() ⇒ runRequestProcessor(processorCommandQueue), "zmq-commands-processor")
      processorThread.start()

      var shutdown = false
      val commandsSource = commandsPipe.source()
      commandsSource.configureBlocking(false)

      val expectingCommands = mutable.MutableList[ZMQServerCommand]()
      val waitTimeout = 60000

      val socket = context.socket(ZMQ.ROUTER)
      socket.bind(s"tcp://$interface:$port") // todo: handle when port is busy
      log.info(s"Socket $socket bound to $interface:$port")

      var shutdownTimeout: FiniteDuration = 10.seconds
      val poller = context.poller(2)
      val commandsIndex = poller.register(commandsSource, Poller.POLLIN)
      val frontendIndex = poller.register(socket, Poller.POLLIN)

      do {
        if (poller.poll(waitTimeout) > 0) {
          if (poller.pollin(commandsIndex)) { // consume new commands
            expectingCommands ++= fetchNewCommands(commandsSource)
          }

          if (poller.pollin(frontendIndex)) {
            while ((socket.getEvents & Poller.POLLIN) != 0) {
              val clientId = socket.recv()
              if (socket.hasReceiveMore) {
                val nullFrame = socket.recv()
                if (nullFrame.nonEmpty) {
                  skipInvalidMessage("null frame", nullFrame, socket)
                  // log.warn("a null frame is" + new String(nullFrame))
                } else if (socket.hasReceiveMore) {
                  val requestId = socket.recv()
                  if (socket.hasReceiveMore) {
                    val message = socket.recvStr()
                    processorCommandQueue.put(new ZMQServerRequest(clientId, requestId, message))
                  } else {
                    log.warn(s"Got requestId frame but didn't get the following frame with message from $clientId.")
                  }
                } else {
                  log.warn(s"Got null frame but didn't get the following frame with requestId from $clientId.")
                }
              }
              else {
                log.warn(s"Got frame with clientId $clientId but no null frame from $socket.")
              }
            }
          }
        }

        expectingCommands.foreach {
          case stop: ZMQServerThreadStop ⇒
            processorCommandQueue.put(stop)
            shutdownTimeout = stop.timeout
            shutdown = true

          case response: ZMQServerResponse ⇒
            socket.send(response.clientId, ZMQ.SNDMORE)
            socket.send(null: Array[Byte], ZMQ.SNDMORE)
            socket.send(response.replyId, ZMQ.SNDMORE)
            socket.send(response.message)
        }
        expectingCommands.clear()
      } while (!shutdown)

      socket.setLinger(shutdownTimeout.toMillis)
      socket.close()
      log.info(s"Socket $socket is closed")
      processorThread.join(shutdownTimeout.toMillis)
    }
    catch {
      case NonFatal(e) ⇒
        log.error("Unhandled exception", e)
    }
  }

  protected def runRequestProcessor(commandsQueue: LinkedBlockingQueue[ZMQServerCommand]): Unit = {
    try {
      var shutdown = false
      while (!shutdown) {
        commandsQueue.take() match {
          case _: ZMQServerThreadStop ⇒ shutdown = true
          case request: ZMQServerRequest ⇒
            processor(request).timeout(responseTimeout).runAsync
        }
      }
    }
    catch {
      case NonFatal(e) ⇒
        log.error("Unhandled exception", e)
    }
  }
}

private [zmq] sealed trait ZMQServerCommand
private [zmq] class ZMQServerThreadStop(val timeout: FiniteDuration) extends ZMQServerCommand

private [zmq] class ZMQServerResponse(
                                       val clientId: Array[Byte],
                                       val replyId: Array[Byte],
                                       val message: String
                                     ) extends ZMQServerCommand

private [transport] class ZMQServerRequest(
                                            val clientId: Array[Byte],
                                            val replyId: Array[Byte],
                                            val message: String) extends ZMQServerCommand

/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperbus.transport.zmq

import java.util.concurrent.LinkedBlockingQueue

import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Scheduler
import org.zeromq.ZMQ
import org.zeromq.ZMQ.{Context, Poller, Socket}

import scala.collection.mutable
import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.control.NonFatal

private[transport] class ZMQServerThread(context: Context,
                                         processor: (ZMQServerRequest) ⇒ Task[Any],
                                         interface: String,
                                         port: Int,
                                         responseTimeout: FiniteDuration
                                        )
                                        (implicit scheduler: Scheduler) extends ZMQCommandsConsumer[ZMQServerCommand] with StrictLogging {

  private val thread = new Thread(new Runnable {
    override def run(): Unit = {
      ZMQServerThread.this.run()
    }
  }, "zmq-commands")
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
    logger.info(s"Running ZMQ-Server $thread/$this on $interface:$port")
    try {
      val processorCommandQueue = new LinkedBlockingQueue[ZMQServerCommand]()
      val processorThread = new Thread(new Runnable {
        override def run(): Unit = {
          runRequestProcessor(processorCommandQueue)
        }
      }, "zmq-commands-processor")
      processorThread.start()

      var shutdown = false
      val commandsSource = commandsPipe.source()
      commandsSource.configureBlocking(false)

      val expectingCommands = mutable.MutableList[ZMQServerCommand]()
      var waitTimeout = 5000

      var socketOption: Option[Socket] = None

      var shutdownTimeout: FiniteDuration = 10.seconds
      val poller = context.poller(1)
      val commandsIndex = poller.register(commandsSource, Poller.POLLIN)
      var lastBindTry = 0l
      var socketPollerIndexOption: Option[Int] = None

      do {
        if (socketOption.isEmpty && (lastBindTry + waitTimeout) < System.currentTimeMillis() ) {
          lastBindTry = System.currentTimeMillis()
          val s = context.socket(ZMQ.ROUTER)
          try {
            s.bind(s"tcp://$interface:$port")
            logger.info(s"Socket $s bound to $interface:$port")
            socketOption = Some(s)
            socketPollerIndexOption = Some(poller.register(s, Poller.POLLIN))
            waitTimeout = 60000
          }
          catch {
            case NonFatal(e) ⇒
              logger.error(s"Can't bind to $interface:$port", e)
              s.close()
          }
        }

        if (poller.poll(waitTimeout) > 0) {
          if (poller.pollin(commandsIndex)) { // consume new commands
            expectingCommands ++= fetchNewCommands(commandsSource)
          }

          socketPollerIndexOption.foreach { socketPollerIndex ⇒
            val socket = socketOption.get
            if (poller.pollin(socketPollerIndex)) {
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
                      val r = new ZMQServerRequest(clientId, requestId, message)
                      logger.trace(s"Received $r" )
                      processorCommandQueue.put(r)
                    } else {
                      logger.warn(s"Got requestId frame but didn't get the following frame with message from $clientId.")
                    }
                  } else {
                    logger.warn(s"Got null frame but didn't get the following frame with requestId from $clientId.")
                  }
                }
                else {
                  logger.warn(s"Got frame with clientId $clientId but no null frame from $socket.")
                }
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
            socketOption.foreach { socket ⇒
              logger.trace(s"Replying with $response" )
              socket.send(response.clientId, ZMQ.SNDMORE)
              socket.send(null: Array[Byte], ZMQ.SNDMORE)
              socket.send(response.replyId, ZMQ.SNDMORE)
              socket.send(response.message)
            }

          case other ⇒
            logger.error(s"Unexpected command $other")
        }
        expectingCommands.clear()
      } while (!shutdown)

      socketOption.foreach { socket ⇒
        socket.setLinger(shutdownTimeout.toMillis)
        socket.close()
        logger.info(s"Socket $socket is closed")
      }
      processorThread.join(shutdownTimeout.toMillis)
    }
    catch {
      case NonFatal(e) ⇒
        logger.error("Unhandled exception", e)
    }
  }

  protected def runRequestProcessor(commandsQueue: LinkedBlockingQueue[ZMQServerCommand]): Unit = {
    try {
      var shutdown = false
      while (!shutdown) {
        commandsQueue.take() match {
          case _: ZMQServerThreadStop ⇒ shutdown = true
          case request: ZMQServerRequest ⇒
            processor(request)
              .timeout(responseTimeout)
              .runAsync
              .recover {
                case NonFatal(e) ⇒
                  logger.error("Unhandled exception", e)
              }
          case other ⇒
            logger.error(s"Unexpected command $other")
        }
      }
    }
    catch {
      case NonFatal(e) ⇒
        logger.error("Unhandled exception", e)
    }
  }
}

private [zmq] sealed trait ZMQServerCommand
private [zmq] class ZMQServerThreadStop(val timeout: FiniteDuration) extends ZMQServerCommand

private [zmq] class ZMQServerResponse(
                                       val clientId: Array[Byte],
                                       val replyId: Array[Byte],
                                       val message: String
                                     ) extends ZMQServerCommand {
  override def toString: String = s"ZMQServerResponse($clientId,$replyId,$message)"
}

private [transport] class ZMQServerRequest(
                                            val clientId: Array[Byte],
                                            val replyId: Array[Byte],
                                            val message: String) extends ZMQServerCommand {
  override def toString: String = s"ZMQServerRequest($clientId,$replyId,$message)"
}

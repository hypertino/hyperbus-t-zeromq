package com.hypertino.hyperbus.transport.zmq

import java.nio.channels.Pipe
import java.util.concurrent.ConcurrentLinkedQueue

import com.hypertino.hyperbus.model.{ErrorBody, GatewayTimeout, MessagingContext, ResponseBase, ServiceUnavailable}
import com.hypertino.hyperbus.serialization.{MessageReader, ResponseBaseDeserializer}
import com.hypertino.hyperbus.transport.api.ServiceResolver
import monix.eval.{Callback, Task}
import monix.execution.Scheduler
import monix.execution.atomic.AtomicLong
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import org.zeromq.ZMQ.{Context, Poller, Socket}

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

// todo: rename
// todo: max out-queue size
private[transport] class ZMQClientThread(context: Context,
                                         serviceResolver: ServiceResolver,
                                         protected val commandsPipe: Pipe,
                                         protected val commandsQueue: ConcurrentLinkedQueue[ZMQClientCommand],
                                         keepAliveTimeout: FiniteDuration,
                                         defaultPort: Int,
                                         maxSockets: Int)
                                        (implicit scheduler: Scheduler) extends ZMQThreadBase[ZMQClientCommand] {

  // todo: rename
  var allocatedSockets = mutable.Map[(String, Int), SocketWithTtl]()

  // todo: rename
  val expectingReplySockets = mutable.Map[Long, ExpectingReply]()

  val log = LoggerFactory.getLogger(getClass)

  val thread = new Thread(() ⇒ run(), "zmq-ask")
  thread.start()

  def join(millis: Long): Unit = thread.join(millis)

  protected def run(): Unit = try {
    var shutdown = false
    val commandsSource = commandsPipe.source()
    commandsSource.configureBlocking(false)

    val expectingCommands = mutable.MutableList[ZMQClientCommand]()
    var waitTimeout = keepAliveTimeout.toMillis / 2
    var requestId: Long = 0
    val poller = context.poller(maxSockets)
    val commandsIndex = poller.register(commandsSource, Poller.POLLIN)

    do {
      // allocatedSockets.foreach(s ⇒ poller.register(s._2.socket, Poller.POLLIN))
      val ready = poller.poll(waitTimeout)
      if (ready > 0) {
        val s = (0 to allocatedSockets.size).map { i ⇒
          (poller.pollin(i),
            poller.pollerr(i),
            poller.pollout(i)
          )
        }.mkString
        log.trace(s"Ready: $ready $s")

        if (poller.pollin(commandsIndex)) { // consume new commands
          expectingCommands ++= fetchNewCommands(commandsSource)
        }
        else {
          log.debug("no commands")
        }

        log.debug(s"allocated: ${allocatedSockets.size}. timeout: $waitTimeout")
        allocatedSockets.values.foreach { e ⇒
          log.debug(s"pollin: ${e.pollerIndex},${e.socket}")
          if (poller.pollin(e.pollerIndex)) {
            // todo: handle & log if no full message was received

            while ((e.socket.getEvents & Poller.POLLIN) != 0) {
              val nullFrame = e.socket.recv() // todo: check it's null
              if (nullFrame.nonEmpty) {
                log.warn("null frame is" + new String(nullFrame))
              }
              if (e.socket.hasReceiveMore) {
                val requestId = e.socket.recv()
                if (e.socket.hasReceiveMore) {
                  val message = e.socket.recvStr()
                  if (requestId != null && requestId.size == 8) {
                    val lRequestId = java.nio.ByteBuffer.wrap(requestId).getLong
                    log.debug(s"Incoming: $lRequestId Currently expecting: ${expectingReplySockets.size}. Poller index = ${e.pollerIndex}")

                    // todo: move to function
                    expectingReplySockets.get(lRequestId) match {
                      case Some(expecting) ⇒
                        log.debug(s"Removing: $lRequestId")
                        expectingReplySockets.remove(lRequestId)
                        Task.fork {
                          Task.eval {
                            e.updateTtl(keepAliveTimeout.toMillis)
                            val result = Try {
                              MessageReader.from(message, expecting.responseDeserializer) match {
                                case NonFatal(error) ⇒ Failure(error)
                                case other ⇒ Success(other)
                              }
                            }.flatten
                            expecting.callback(result)
                          }
                        }.runAsync


                      case None ⇒ // todo: log
                        log.debug(s"444, not found for $lRequestId. Currently expecting: ${expectingReplySockets.size}, ${message}")

                    }
                  } else {
                    log.warn("333")
                  }
                } else {
                  log.warn("222")
                }
              }
              else {
                log.warn("111")
              }
            }
          }
        }
      }

      expectingReplySockets.clone().filter(i ⇒ i._2.isCommandExpired) foreach { e ⇒
        log.debug(s"Removing Xpred: ${e._1}")
        expectingReplySockets.remove(e._1)
        val expectingReply = e._2
        implicit val msx = MessagingContext(expectingReply.correlationId)
        expectingReply.callback(Failure(GatewayTimeout(ErrorBody("ask_timeout"))))
      }

      allocatedSockets.clone().foreach { case (k, a) ⇒
        if (a.isExpired) {
          a.socket.close()
          poller.unregister(a.socket)
          allocatedSockets.remove(k)
        }
      }

      expectingCommands.foreach {
        case ask: ZMQClientAsk ⇒
          requestId += 1
          allocateSocketAndSend(ask, requestId, poller)

        case ZMQClientThreadStop ⇒
          shutdown = true
      }
      expectingCommands.clear()
      waitTimeout = keepAliveTimeout.toMillis / 2
      val now = System.currentTimeMillis()
      expectingReplySockets.map(_._2.commandTtl) foreach { ttl ⇒
        val delta = 100 + ttl - now
        waitTimeout = Math.max(Math.min(waitTimeout, delta), 100)
      }
    } while (!shutdown)

    allocatedSockets.foreach { case (_, v) ⇒
      v.socket.close()
        poller.unregister(v.socket)
    }

    expectingReplySockets.foreach { case (_, v) ⇒
      implicit val msx = MessagingContext(v.correlationId)
      v.callback(Failure(ServiceUnavailable(ErrorBody("transport_shutdown"))))
    }
  }
  catch {
    case NonFatal(e) ⇒
      log.error("Unhandled", e)
      println(e)
      e.printStackTrace()
  }

  protected def allocateSocketAndSend(ask: ZMQClientAsk, requestId: Long, poller: Poller): Unit = {
    val key = (ask.serviceEndpoint.hostname, ask.serviceEndpoint.port.getOrElse(defaultPort))
    val socketTry: Try[Socket] = Try {
      allocatedSockets.get(key) match {
        case Some(allocated) ⇒
          allocated.updateTtl(keepAliveTimeout.toMillis)
          allocated.socket

        case None ⇒
          val s = context.socket(ZMQ.DEALER)
          s.connect(s"tcp://${ask.serviceEndpoint.hostname}:${ask.serviceEndpoint.port.getOrElse(defaultPort)}")
          val newSocket = new SocketWithTtl(
            s,
            AtomicLong(keepAliveTimeout.toMillis + System.currentTimeMillis()),
            poller.register(s, Poller.POLLIN)
          )
          log.debug(s"Allocated new socket: ${newSocket.pollerIndex}, ${newSocket.socket}")
          allocatedSockets += key → newSocket
          newSocket.socket
      }
    }

    socketTry.map { socket ⇒
      val aRequestId = java.nio.ByteBuffer.allocate(8)
      aRequestId.putLong(requestId)
      aRequestId.flip()
      val e = new ExpectingReply(ask.responseDeserializer, ask.ttl, ask.callback, ask.correlationId)

      socket.send(null: Array[Byte], ZMQ.SNDMORE)
      socket.send(aRequestId.array(), ZMQ.SNDMORE)
      socket.send(ask.message)

      log.trace(s"New expecting: $requestId, ${e.correlationId}")
      expectingReplySockets += requestId → e
    } recover {
      case NonFatal(e) ⇒
        implicit val mcx = MessagingContext(ask.correlationId)
        ask.callback(Failure(ServiceUnavailable(ErrorBody(e.getMessage, Some(e.toString)))))
    }
  }

}

private[transport] class SocketWithTtl(
                                        val socket: Socket,
                                        val ttl: AtomicLong,
                                        val pollerIndex: Int) {
  def updateTtl(keepAliveTimeout: Long): Unit = {
    ttl.set(keepAliveTimeout + System.currentTimeMillis())
  }

  def isExpired: Boolean = ttl.get < System.currentTimeMillis()
}

// todo: rename
private[transport] class ExpectingReply(
                                         val responseDeserializer: ResponseBaseDeserializer,
                                         val commandTtl: Long,
                                         val callback: Callback[ResponseBase],
                                         val correlationId: String
                                       ) {
  def isCommandExpired: Boolean = commandTtl < System.currentTimeMillis()
}

package com.hypertino.hyperbus.transport.zmq

import java.nio.ByteBuffer
import java.nio.channels.Pipe
import java.util.concurrent.ConcurrentLinkedQueue

import com.hypertino.hyperbus.model.{ErrorBody, GatewayTimeout, InternalServerError, MessagingContext, ResponseBase, ServiceUnavailable}
import com.hypertino.hyperbus.serialization.{MessageReader, ResponseBaseDeserializer}
import com.hypertino.hyperbus.transport.api.{ServiceEndpoint, ServiceResolver}
import monix.eval.{Callback, Task}
import monix.execution.Scheduler
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import org.zeromq.ZMQ.{Context, Poller, Socket}

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

// todo: rename
private[transport] class ZMQClientThread(context: Context,
                                         serviceResolver: ServiceResolver,
                                         protected val commandsPipe: Pipe,
                                         protected val commandsQueue: ConcurrentLinkedQueue[ZMQClientCommand],
                                         keepAliveTimeout: FiniteDuration,
                                         maxSocketsPerServer: Int,
                                         defaultPort: Int)
                                        (implicit scheduler: Scheduler) extends ZMQThreadBase[ZMQClientCommand] {

  val allocatedSockets = mutable.Map[(String, Int), Vector[SocketWithTtl]]()
  val log = LoggerFactory.getLogger(getClass)

  val thread = new Thread(() ⇒ run(), "zmq-ask")
  thread.start()

  def join(millis: Long): Unit = thread.join(millis)

  protected def run(): Unit = try {
    var shutdown = false
    val commandsSource = commandsPipe.source()
    commandsSource.configureBlocking(false)

    var expectingReplySockets = Vector.empty[SocketWithTtl]
    val expectingCommands = mutable.MutableList[ZMQClientCommand]()
    var waitTimeout = keepAliveTimeout.toMillis / 2

    do {
      val poller = context.poller(1 + expectingReplySockets.size)
      poller.register(commandsSource, Poller.POLLIN)
      expectingReplySockets.foreach(s ⇒ poller.register(s.socket, Poller.POLLIN))

      if (poller.poll(waitTimeout) > 0) {
        if (poller.pollin(0)) { // consume new commands
          expectingCommands ++= fetchNewCommands(commandsSource)
        }

        expectingReplySockets.zipWithIndex.foreach { case (e, i) ⇒
          if (poller.pollin(i + 1)) {
            val s = e.socket.recvStr()
            Task.fork {
              Task.fromTry {
                Try {
                  MessageReader.from(s, e.expectingReply.get.responseDeserializer) match {
                    case NonFatal(error) ⇒ Failure(error)
                    case other ⇒ Success(other)
                  }
                }.flatten
              }
            }.runOnComplete(e.expectingReply.get.callback)
            e.clear()
          }
        }
      }

      poller.unregister(commandsSource)

      expectingReplySockets.filter(i ⇒ i.expectingReply.isDefined && i.expectingReply.get.isCommandExpired) foreach { e ⇒
        val expectingReply = e.expectingReply.get
        e.close()
        implicit val msx = MessagingContext(expectingReply.correlationId)
        expectingReply.callback(Failure(GatewayTimeout(ErrorBody("ask_timeout"))))
      }

      val newlyexpectingReplySockets = expectingCommands.flatMap {
        case ask: ZMQClientAsk ⇒
          allocateSocketAndSend(ask)

        case ZMQClientThreadStop ⇒
          shutdown = true
          None
      }

      expectingCommands.clear()
      closeAndRemoveExpiredSockets()
      expectingReplySockets = expectingReplySockets.filterNot(i ⇒ i.isClosed || i.expectingReply.isEmpty) ++ newlyexpectingReplySockets

      waitTimeout = keepAliveTimeout.toMillis / 2
      val now = System.currentTimeMillis()
      expectingReplySockets.map(_.expectingReply.get.commandTtl) ++ allocatedSockets.values.flatMap(_.map(_.ttl)) foreach { ttl ⇒
        val delta = 100 + ttl - now
        waitTimeout = Math.max(Math.min(waitTimeout, delta), 100)
      }
    } while (!shutdown)

    allocatedSockets.foreach { case (_, v) ⇒
      v.filter(_.expectingReply.isDefined).foreach { socket ⇒
        socket.close()
        socket.expectingReply.foreach { er ⇒
          implicit val msx = MessagingContext(er.correlationId)
          er.callback(Failure(ServiceUnavailable(ErrorBody("transport_shutdown"))))
        }
      }
    }
  }
  catch {
    case NonFatal(e) ⇒
      log.error("Unhandled", e)
      println(e)
      e.printStackTrace()
  }

  protected def allocateSocketAndSend(ask: ZMQClientAsk): Option[SocketWithTtl] = {
    val key = (ask.serviceEndpoint.hostname, ask.serviceEndpoint.port.getOrElse(defaultPort))
    val v = allocatedSockets.getOrElse(key, Vector.empty[SocketWithTtl])
    val free = v.find(_.expectingReply.isEmpty)
    if (free.isEmpty && v.size >= maxSocketsPerServer) {
      implicit val mcx = MessagingContext(ask.correlationId)
      ask.callback(Failure(ServiceUnavailable(ErrorBody("ask_queue_is_full"))))
      None
    }
    else {
      try {
        val socket = free.getOrElse {
          val s = context.socket(ZMQ.REQ)
          s.connect(s"tcp://${ask.serviceEndpoint.hostname}:${ask.serviceEndpoint.port.getOrElse(defaultPort)}")
          val newSocket = new SocketWithTtl(
            s,
            keepAliveTimeout.toMillis + System.currentTimeMillis()
          )
          val newVector = allocatedSockets.getOrElse(key, Vector.empty) :+ newSocket
          allocatedSockets += key → newVector
          newSocket
        }

        try {
          socket.socket.send(ask.message)
          val e = new SocketExpectingReply(socket, ask.serviceEndpoint, ask.responseDeserializer,
            ask.ttl, ask.callback, ask.correlationId
          )
          socket.waitFor(e)
          Some(socket)
        }
        catch {
          case NonFatal(e) ⇒
            socket.close()
            throw e
        }
      }
      catch {
        case NonFatal(e) ⇒
          implicit val mcx = MessagingContext(ask.correlationId)
          ask.callback(Failure(ServiceUnavailable(ErrorBody(e.getMessage, Some(e.toString)))))
          None
      }
    }
  }

  protected def closeAndRemoveExpiredSockets(): Unit = {
    allocatedSockets.foreach { case (se, v) ⇒
      v.filter(i ⇒ !i.isClosed && i.isExpired).foreach(_.close())
      val newVector = v.filterNot(_.isClosed)
      if (v.size != newVector.size) {
        allocatedSockets += se → newVector
      }
    }
    allocatedSockets.retain((k,v) ⇒ v.nonEmpty)
  }
}

private[transport] class SocketWithTtl(
                               val socket: Socket,
                               val ttl: Long,
                               private var _expectingReply: Option[SocketExpectingReply] = None,
                               private var _isClosed: Boolean = false
                               ) {
  def isExpired: Boolean = ttl < System.currentTimeMillis()

  def isClosed: Boolean = _isClosed

  def expectingReply: Option[SocketExpectingReply] = _expectingReply

  def waitFor(reply: SocketExpectingReply): Unit = {
    _expectingReply = Some(reply)
  }

  def clear(): Unit = {
    _expectingReply = None
  }

  def close(): Unit = {
    socket.close()
    _isClosed = true
    clear()
  }
}

// todo: rename
private[transport] class SocketExpectingReply(
                                               val parent: SocketWithTtl,
                                               val serviceEndpoint: ServiceEndpoint,
                                               val responseDeserializer: ResponseBaseDeserializer,
                                               val commandTtl: Long,
                                               val callback: Callback[ResponseBase],
                                               val correlationId: String
                                             ) {
  def isCommandExpired: Boolean = commandTtl < System.currentTimeMillis()
}

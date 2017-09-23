package com.hypertino.hyperbus.transport.zmq

import java.util.concurrent.LinkedBlockingQueue

import com.hypertino.hyperbus.model.{ErrorBody, GatewayTimeout, MessagingContext, ResponseBase, ServiceUnavailable}
import com.hypertino.hyperbus.serialization.{MessageReader, ResponseBaseDeserializer}
import com.hypertino.hyperbus.transport.api.{ServiceEndpoint, ServiceResolver}
import com.typesafe.scalalogging.{Logger, StrictLogging}
import monix.eval.{Callback, Task}
import monix.execution.{Cancelable, Scheduler}
import monix.execution.atomic.{AtomicInt, AtomicLong}
import org.zeromq.ZMQ
import org.zeromq.ZMQ.{Context, Poller, Socket}

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

// todo: subscribe to ServiceResolver
// todo: 1. test socket ttl expiration, 2. test maxOutputQueueSize limit

private[transport] class ZMQClientThread(context: Context,
                                         serviceResolver: ServiceResolver,
                                         keepAliveTimeout: FiniteDuration,
                                         defaultPort: Int,
                                         maxSockets: Int,
                                         maxOutputQueueSize: Int
                                        )
                                        (implicit scheduler: Scheduler) extends ZMQCommandsConsumer[ZMQClientCommand] with StrictLogging {

  private val actualClientSocketMap = mutable.Map[(String, Int), SocketWithTtl]()

  private val expectingReplyMap = mutable.Map[Int, mutable.Map[Long, ExpectingReply]]()
  
  private val responseProcessorCommandQueue = new LinkedBlockingQueue[ZMQResponseProcessorCommand]()
  private val responseProcessorThread = new Thread(new Runnable {
    override def run(): Unit = {
      runResponseProcessor(responseProcessorCommandQueue)
    }
  }, "zmq-ask-processor")

  private val thread = new Thread(new Runnable {
    override def run(): Unit = {
      ZMQClientThread.this.run()
    }
  }, "zmq-ask")
  thread.start()

  def ask(message: String,
          correlationId: String,
          responseDeserializer: ResponseBaseDeserializer,
          serviceEndpoint: ServiceEndpoint,
          ttl: Long,
          callback: Callback[ResponseBase]
         ): Cancelable = {

    if (thread.isAlive) {
      val askCommand = new ZMQClientAsk(message, correlationId, responseDeserializer, serviceEndpoint, ttl, callback)
      sendCommand(askCommand)
      new Cancelable {
        override def cancel(): Unit = askCommand.cancel()
      }
    }
    else {
      implicit val mcx = MessagingContext(correlationId)
      callback(Failure(ServiceUnavailable(ErrorBody("transport_shutdown"))))
      Cancelable.empty
    }
  }

  def stop(millis: Long): Unit = {
    clearAndShutdownAskCommands()
    sendCommand(ZMQClientThreadStop)
    thread.join(millis)
    clearAndShutdownAskCommands() // second time if something comes while we're stopping
    close()
  }

  protected def clearAndShutdownAskCommands(): Unit = {
    var cmd: ZMQClientCommand = null
    do {
      cmd = commandsQueue.poll()
      cmd match {
        case ask: ZMQClientAsk ⇒
          implicit val mcx = MessagingContext(ask.correlationId)
          ask.callback(Failure(ServiceUnavailable(ErrorBody("shutdown_requested"))))
        case _ ⇒
      }
    } while (cmd != null)
  }

  protected def run(): Unit = {
    try {
      responseProcessorThread.start()

      var shutdown = false
      val commandsSource = commandsPipe.source()
      commandsSource.configureBlocking(false)

      val expectingCommands = mutable.MutableList[ZMQClientCommand]()
      var waitTimeout = keepAliveTimeout.toMillis / 2
      var requestId: Long = 0
      val poller = context.poller(maxSockets)
      val commandsIndex = poller.register(commandsSource, Poller.POLLIN)

      do {
        val ready = poller.poll(waitTimeout)
        if (ready > 0) {

          if (poller.pollin(commandsIndex)) { // consume new commands
            expectingCommands ++= fetchNewCommands(commandsSource)
          }

          actualClientSocketMap.values.foreach { a ⇒
            if (poller.pollin(a.pollerIndex)) {
              consumeReply(a)
            }
          }
        }

        handleExpiredReplies()
        handleExpiredSockets(poller)

        // process new commands
        expectingCommands.foreach {
          case ask: ZMQClientAsk ⇒
            requestId += 1
            allocateSocketAndSend(ask, requestId, poller)

          case ZMQClientThreadStop ⇒
            shutdown = true
        }
        expectingCommands.clear()

        // get new timeout
        waitTimeout = keepAliveTimeout.toMillis / 2
        val now = System.currentTimeMillis()
        expectingReplyMap.foreach { case (_, v) ⇒
          v.foreach { case (_, e) ⇒
            val delta = 100 + e.commandTtl - now
            waitTimeout = Math.max(Math.min(waitTimeout, delta), 100)
          }
        }

      } while (!shutdown)

      expectingReplyMap.foreach { case (_, v) ⇒
        v.foreach { case (_, e) ⇒
          implicit val msx = MessagingContext(e.correlationId)
          e.callback(Failure(ServiceUnavailable(ErrorBody("transport_shutdown"))))
          e.socketWithTtl.release(logger)
        }
      }

      actualClientSocketMap.values.foreach(_.release(logger))
    }
    catch {
      case NonFatal(e) ⇒
        logger.error("Unhandled", e)
    }
    finally {
      responseProcessorCommandQueue.put(ZMQResponseProcessorStop)
      responseProcessorThread.join()
    }
  }

  private def handleExpiredSockets(poller: Poller) = {
    actualClientSocketMap.filter(_._2.isExpired).foreach { case (k, a) ⇒
      a.release(logger)
      actualClientSocketMap.remove(k)
    }
  }

  private def removeExpectingReply(replyId: Long, e: ExpectingReply): Unit = {
    e.socketWithTtl.release(logger)
    expectingReplyMap.get(e.socketWithTtl.pollerIndex).foreach { map ⇒
      map.remove(replyId)
      if (map.isEmpty) {
        expectingReplyMap.remove(e.socketWithTtl.pollerIndex)
      }
    }
  }

  private def handleExpiredReplies(): Unit = {
    expectingReplyMap.values.flatten.filter(i ⇒ i._2.isCommandExpired).foreach { case (replyId, expectingReply) ⇒
      removeExpectingReply(replyId, expectingReply)
      implicit val msx = MessagingContext(expectingReply.correlationId)
      expectingReply.callback(Failure(GatewayTimeout(ErrorBody("ask_timeout"))))
    }
  }

  protected def callResponseHandler(pollerIndex: Int, replyId: Long, message: String) {
    expectingReplyMap.get(pollerIndex).foreach { map ⇒
      map.get(replyId).foreach { expecting ⇒
        expecting.socketWithTtl.updateTtl(keepAliveTimeout.toMillis)
        removeExpectingReply(replyId, expecting)
        responseProcessorCommandQueue.put(new ZMQResponseProcessReply(message, expecting.responseDeserializer, expecting.callback))
      }
    }
  }

  protected def consumeReply(a: SocketWithTtl): Unit = {
    while ((a.socket.getEvents & Poller.POLLIN) != 0) {
      val nullFrame = a.socket.recv()
      if (nullFrame.nonEmpty) {
        skipInvalidMessage("null frame", nullFrame, a.socket)
      } else if (a.socket.hasReceiveMore) {
        val requestId = a.socket.recv()
        if (a.socket.hasReceiveMore) {
          val message = a.socket.recvStr()
          if (requestId != null && requestId.size == 8) {
            val lRequestId = java.nio.ByteBuffer.wrap(requestId).getLong
            callResponseHandler(a.pollerIndex, lRequestId, message)
          }
          else {
            logger.warn(s"Got frame ${requestId.size} bytes while expecting 8 bytes replyId frame from ${a.socket}.")
          }
        } else {
          logger.warn(s"Got frame ${requestId.size} bytes but didn't get the following frame with message body from ${a.socket}.")
        }
      } else {
        logger.warn(s"Got null frame but didn't get the following frame with replyId from ${a.socket}.")
      }
    }
  }

  protected def allocateSocketAndSend(ask: ZMQClientAsk, requestId: Long, poller: Poller): Unit = {
    val key = (ask.serviceEndpoint.hostname, ask.serviceEndpoint.port.getOrElse(defaultPort))
    val socketTry: Try[SocketWithTtl] = Try {
      actualClientSocketMap.get(key) match {
        case Some(a) ⇒
          a.updateTtl(keepAliveTimeout.toMillis)
          a

        case None ⇒
          val socket = context.socket(ZMQ.DEALER)
          socket.connect(s"tcp://${ask.serviceEndpoint.hostname}:${ask.serviceEndpoint.port.getOrElse(defaultPort)}")
          val a = new SocketWithTtl(
            socket,
            AtomicLong(keepAliveTimeout.toMillis + System.currentTimeMillis()),
            poller
          )
          logger.debug(s"Allocated new socket: ${a.socket}/${a.pollerIndex} to $key")
          actualClientSocketMap += key → a
          a
      }
    }

    socketTry.map { a ⇒
      val map = expectingReplyMap.get(a.pollerIndex) match {
        case Some(amap) ⇒ amap
        case None ⇒
          val newMap = mutable.Map[Long, ExpectingReply]()
          expectingReplyMap.put(a.pollerIndex, newMap)
          newMap
      }
      if (map.size >= maxOutputQueueSize) {
        implicit val mcx = MessagingContext(ask.correlationId)
        ask.callback(Failure(ServiceUnavailable(ErrorBody("output_queue_limit_reached", Some(s"Queue size limit to $key is reached ($maxOutputQueueSize)")))))
      } else {
        val aRequestId = java.nio.ByteBuffer.allocate(8)
        aRequestId.putLong(requestId)
        aRequestId.flip()
        val e = new ExpectingReply(ask.responseDeserializer, ask.ttl, ask.callback, ask.correlationId, a)

        a.socket.send(null: Array[Byte], ZMQ.SNDMORE)
        a.socket.send(aRequestId.array(), ZMQ.SNDMORE)
        a.socket.send(ask.message)

        map += requestId → e
        e.socketWithTtl.addRef()
      }
    } recover {
      case NonFatal(e) ⇒
        implicit val mcx = MessagingContext(ask.correlationId)
        ask.callback(Failure(ServiceUnavailable(ErrorBody(e.getMessage, Some(e.toString)))))
    }
  }

  protected def runResponseProcessor(commandsQueue: LinkedBlockingQueue[ZMQResponseProcessorCommand]): Unit = {
    try {
      var shutdown = false
      while (!shutdown) {
        commandsQueue.take() match {
          case ZMQResponseProcessorStop ⇒ shutdown = true
          case reply: ZMQResponseProcessReply ⇒
            Task.eval {
              val result = Try {
                MessageReader.fromString(reply.message, reply.responseDeserializer) match {
                  case NonFatal(error) ⇒ Failure(error)
                  case other ⇒ Success(other)
                }
              }.flatten
              reply.callback(result)
            }.runAsync
        }
      }
    }
    catch {
      case NonFatal(e) ⇒
        logger.error("Unhandled exception", e)
    }
  }
}

private[transport] class SocketWithTtl(
                                        val socket: Socket,
                                        val ttl: AtomicLong,
                                        poller: Poller
                                      ) {
  private val refCounter = AtomicInt(1)
  val pollerIndex: Int = poller.register(socket)

  def updateTtl(keepAliveTimeout: Long): Unit = {
    ttl.set(keepAliveTimeout + System.currentTimeMillis())
  }

  def isExpired: Boolean = ttl.get < System.currentTimeMillis()

  def addRef(): Unit = refCounter.increment()

  def release(logger: Logger): Boolean = {
    if (refCounter.decrementAndGet() <= 0) {
      logger.debug(s"Closing socket $socket")
      poller.unregister(socket)
      socket.close()
      false
    } else {
      true
    }
  }
}

private [zmq] class ExpectingReply(
                                         val responseDeserializer: ResponseBaseDeserializer,
                                         val commandTtl: Long,
                                         val callback: Callback[ResponseBase],
                                         val correlationId: String,
                                         val socketWithTtl: SocketWithTtl
                                       ) {
  def isCommandExpired: Boolean = commandTtl < System.currentTimeMillis()
}

private [zmq] sealed trait ZMQClientCommand
private [zmq] case object ZMQClientThreadStop extends ZMQClientCommand

private [zmq] class ZMQClientAsk(val message: String,
                   val correlationId: String,
                   val responseDeserializer: ResponseBaseDeserializer,
                   val serviceEndpoint: ServiceEndpoint,
                   val ttl: Long,
                   val callback: Callback[ResponseBase]
                  ) extends ZMQClientCommand with CancelableCommand {
  def isExpired: Boolean = ttlRemaining < 0

  def ttlRemaining: Long = ttl - System.currentTimeMillis()
}

private [zmq] sealed trait ZMQResponseProcessorCommand
private [zmq] case object ZMQResponseProcessorStop extends ZMQResponseProcessorCommand
private [zmq] class ZMQResponseProcessReply(val message: String,
                                                 val responseDeserializer: ResponseBaseDeserializer,
                                                 val callback: Callback[ResponseBase]) extends ZMQResponseProcessorCommand
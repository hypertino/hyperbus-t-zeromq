package com.hypertino.hyperbus.transport

import java.io.Reader
import java.nio.ByteBuffer
import java.nio.channels.Pipe
import java.util.concurrent.ConcurrentLinkedQueue

import com.hypertino.binders.value.Obj
import com.hypertino.hyperbus.model.{DynamicRequest, EmptyBody, ErrorBody, InternalServerError, RequestBase, RequestHeaders, ResponseBase}
import com.hypertino.hyperbus.serialization.{MessageDeserializer, MessageReader, RequestDeserializer}
import com.hypertino.hyperbus.transport.api.matchers.RequestMatcher
import com.hypertino.hyperbus.transport.api.{CommandEvent, ServerTransport}
import com.hypertino.hyperbus.transport.zmq._
import com.hypertino.hyperbus.util.ConfigUtils._
import com.hypertino.hyperbus.util.{FuzzyIndex, HyperbusSubscription, SchedulerInjector}
import com.typesafe.config.Config
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import org.zeromq.ZMQ
import scaldi.Injector

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.Random
import scala.util.control.NonFatal

class ZMQServer(
                 val port: Int,
                 val interface: String,
                 val zmqIOThreadCount: Int,
                 val socketsPerServer: Int,
                 val maxSockets: Int,
                 val serverResponseTimeout: FiniteDuration // todo: rename
               )(implicit scheduler: Scheduler) extends ServerTransport {

  def this(config: Config, inj: Injector) = this(
    config.getOptionInt("port").getOrElse(10050),
    config.getOptionString("interface").getOrElse("*"),
    config.getOptionInt("zmq-io-threads").getOrElse(1),
    config.getOptionInt("sockets-per-server").getOrElse(32),
    config.getOptionInt("max-sockets").getOrElse(16384),
    config.getOptionDuration("server-response-timeout").getOrElse(60.seconds)
  )(
    SchedulerInjector(config.getOptionString("scheduler"))(inj)
  )

  protected val context = ZMQ.context(zmqIOThreadCount)
  context.setMaxSockets(maxSockets)

  protected val serverCommandsPipe = Pipe.open()
  protected val serverCommandsSink = serverCommandsPipe.sink()
  serverCommandsSink.configureBlocking(false)
  protected val serverCommandsQueue = new ConcurrentLinkedQueue[ZMQServerCommand]

  protected val commandSubscriptions = new FuzzyIndex[CommandHyperbusSubscription]
  protected val subscriptions = TrieMap[HyperbusSubscription[_], Boolean]()

  protected val serverCommandsThread = new ZMQServerThread(
    context,
    serverCommandsPipe,
    serverCommandsQueue,
    processor, // handler
    interface,
    port
  )

  override def commands[REQ <: RequestBase](matcher: RequestMatcher, inputDeserializer: RequestDeserializer[REQ]): Observable[CommandEvent[REQ]] = {
    new CommandHyperbusSubscription(matcher, inputDeserializer)
      .observable
      .asInstanceOf[Observable[CommandEvent[REQ]]]
  }

  override def events[REQ <: RequestBase](matcher: RequestMatcher, groupName: String, inputDeserializer: RequestDeserializer[REQ]): Observable[REQ] = ???

  override def shutdown(duration: FiniteDuration): Task[Boolean] = {
    Task.eval {
      commandSubscriptions.clear()
      subscriptions.foreach(_._1.off())

      sendServerCommand(ZMQServerThreadStop(duration))
      serverCommandsSink.close()
      serverCommandsThread.join(duration.toMillis)
      // clearAndShutdownAskCommands() // if something came while were closing sink todo: cancel subscriptions?
      context.close()
      true
    }
  }

  protected def sendServerCommand(command: ZMQServerCommand): Unit = {
    serverCommandsQueue.add(command)
    serverCommandsSink.write(ByteBuffer.wrap(Array[Byte](0)))
  }

  protected class CommandHyperbusSubscription(val requestMatcher: RequestMatcher,
                                              val inputDeserializer: RequestDeserializer[RequestBase])
    extends HyperbusSubscription[CommandEvent[RequestBase]] {
    override def remove(): Unit = {
      commandSubscriptions.remove(this)
      subscriptions -= this
    }
    override def add(): Unit = {
      commandSubscriptions.add(this)
      subscriptions += this → false
    }
  }

  protected def processor(request: ZMQServerRequest): Task[Unit] = {
    Task.fork {
      Task.eval {
        MessageReader.from[RequestBase](request.message, (reader: Reader, obj: Obj) ⇒ {
          implicit val fakeRequest: RequestBase = DynamicRequest(EmptyBody, RequestHeaders(obj))

          getRandom(commandSubscriptions
            .lookupAll(fakeRequest)).map { subscription ⇒

            val message = subscription.inputDeserializer(reader, obj)
            Task.create[ResponseBase]{ (_, callback) ⇒
              val command = CommandEvent(message, callback)
              subscription.publish(command).runAsync
            } onErrorHandleWith {
              case r: ResponseBase ⇒
                Task.now(r)
              case NonFatal(e) ⇒
                // todo: log exception
                Task.now(InternalServerError(ErrorBody("unhandled", Some(e.toString))))
            } map { r: ResponseBase ⇒
              sendServerCommand(ZMQServerResponse(request.replyId, r.serializeToString))
            } runAsync // todo: log if serialization is failed
          }
          fakeRequest
        })
        Unit
      }
    }
  }

  private val random = new Random()
  protected def getRandom[T](seq: Seq[T]): Option[T] = {
    val size = seq.size
    if (size > 1)
      Some(seq(random.nextInt(size)))
    else
      seq.headOption
  }

  // ignore body, this is only for deserializing headers
  protected val emptyDeserializer: MessageDeserializer[RequestBase] = (reader: Reader, obj: Obj) ⇒ {
    DynamicRequest(EmptyBody, RequestHeaders(obj))
  }
}

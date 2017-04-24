package com.hypertino.hyperbus.transport

import java.io.Reader
import java.nio.channels.Pipe
import java.util.concurrent.ConcurrentLinkedQueue

import com.hypertino.binders.value.Obj
import com.hypertino.hyperbus.model.{DynamicRequest, EmptyBody, ErrorBody, InternalServerError, MessagingContext, NotFound, RequestBase, RequestHeaders, ResponseBase}
import com.hypertino.hyperbus.serialization.{MessageDeserializer, MessageReader, RequestDeserializer}
import com.hypertino.hyperbus.transport.api.matchers.RequestMatcher
import com.hypertino.hyperbus.transport.api.{CommandEvent, ServerTransport}
import com.hypertino.hyperbus.transport.zmq._
import com.hypertino.hyperbus.util.ConfigUtils._
import com.hypertino.hyperbus.util.{CallbackTask, FuzzyIndex, HyperbusSubscription, SchedulerInjector}
import com.typesafe.config.Config
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import org.slf4j.LoggerFactory
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
                 val maxSockets: Int,
                 val serverResponseTimeout: FiniteDuration
               )(implicit scheduler: Scheduler) extends ServerTransport {

  def this(config: Config, inj: Injector) = this(
    config.getOptionInt("port").getOrElse(10050),
    config.getOptionString("interface").getOrElse("*"),
    config.getOptionInt("zmq-io-threads").getOrElse(1),
    config.getOptionInt("max-sockets").getOrElse(16384),
    config.getOptionDuration("response-timeout").getOrElse(60.seconds)
  )(
    SchedulerInjector(config.getOptionString("scheduler"))(inj)
  )

  protected val log = LoggerFactory.getLogger(getClass)
  protected val context = ZMQ.context(zmqIOThreadCount)
  context.setMaxSockets(maxSockets)

  protected val commandSubscriptions = new FuzzyIndex[CommandHyperbusSubscription]
  protected val subscriptions = TrieMap[HyperbusSubscription[_], Boolean]()

  protected val serverCommandsThread = new ZMQServerThread(
    context,
    processor, // handler
    interface,
    port,
    serverResponseTimeout
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
      serverCommandsThread.stop(duration)
      // todo: cancel subscriptions?
      context.close()
      true
    }
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

  protected def processor(request: ZMQServerRequest): Task[Any] = {
    val deserializeTask: Task[(CommandHyperbusSubscription, RequestBase)] = Task.eval {
      var subscr: CommandHyperbusSubscription = null
      val msg = MessageReader.from[RequestBase](request.message, (reader: Reader, obj: Obj) ⇒ {
        implicit val fakeRequest: RequestBase = DynamicRequest(EmptyBody, RequestHeaders(obj))

        getRandom(commandSubscriptions.lookupAll(fakeRequest)).map { subscription ⇒
          subscr = subscription
          subscription.inputDeserializer(reader, obj)
        } getOrElse {
          throw NotFound(ErrorBody("subscription_not_found", Some(fakeRequest.headers.hri.toString)))
        }
      })
      (subscr, msg)
    }

    deserializeTask.flatMap { case (subscription, r) ⇒
      implicit val message = r

      val cb = CallbackTask[ResponseBase]
      val command = CommandEvent(message, cb)
      val tPublish = subscription.publish(command)

      Task.gatherUnordered(List(cb.task, tPublish)).map(_.head).map { response ⇒
        val responseString = response match {
          case r: ResponseBase ⇒ r.serializeToString
          case NonFatal(e) ⇒ InternalServerError(ErrorBody("unhandled", Some(e.toString))).serializeToString
        }
        serverCommandsThread.reply(request.clientId, request.replyId, responseString)
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

/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

package com.hypertino.hyperbus.transport

import java.io.Reader

import com.hypertino.hyperbus.model.{BadGateway, DynamicRequest, EmptyBody, ErrorBody, Headers, InternalServerError, NotFound, RequestBase, RequestHeaders, ResponseBase}
import com.hypertino.hyperbus.serialization.{MessageDeserializer, MessageReader, RequestDeserializer}
import com.hypertino.hyperbus.transport.api.matchers.RequestMatcher
import com.hypertino.hyperbus.transport.api.{CommandEvent, ServerTransport}
import com.hypertino.hyperbus.transport.zmq._
import com.hypertino.hyperbus.transport.zmq.utils.ErrorCode
import com.hypertino.hyperbus.util.ConfigUtils._
import com.hypertino.hyperbus.util.{CallbackTask, FuzzyIndex, SchedulerInjector, SubjectSubscription}
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import monix.reactive.subjects.ConcurrentSubject
import org.zeromq.ZMQ
import scaldi.Injector

import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.{Failure, Random, Success, Try}

class ZMQServer(
                 val port: Int,
                 val interface: String,
                 val zmqIOThreadCount: Int,
                 val maxSockets: Int,
                 val serverResponseTimeout: FiniteDuration
               )(implicit scheduler: Scheduler) extends ServerTransport with StrictLogging {

  def this(config: Config, inj: Injector) = this(
    config.getOptionInt("port").getOrElse(10050),
    config.getOptionString("interface").getOrElse("*"),
    config.getOptionInt("zmq-io-threads").getOrElse(1),
    config.getOptionInt("max-sockets").getOrElse(16384),
    config.getOptionDuration("response-timeout").getOrElse(60.seconds)
  )(
    SchedulerInjector(config.getOptionString("scheduler"))(inj)
  )

  protected val context = ZMQ.context(zmqIOThreadCount)
  context.setMaxSockets(maxSockets)

  protected val commandSubscriptions = new FuzzyIndex[CommandSubjectSubscription]

  protected val serverCommandsThread = new ZMQServerThread(
    context,
    processor, // handler
    interface,
    port,
    serverResponseTimeout
  )

  override def commands[REQ <: RequestBase](matcher: RequestMatcher, inputDeserializer: RequestDeserializer[REQ]): Observable[CommandEvent[REQ]] = {
    new CommandSubjectSubscription(matcher, inputDeserializer)
      .observable
      .asInstanceOf[Observable[CommandEvent[REQ]]]
  }

  override def events[REQ <: RequestBase](matcher: RequestMatcher, groupName: String, inputDeserializer: RequestDeserializer[REQ]): Observable[REQ] = ???

  override def startServices(): Unit = {
    serverCommandsThread.start()
  }

  override def shutdown(duration: FiniteDuration): Task[Boolean] = {
    Task.eval {
      commandSubscriptions.toSeq.foreach(_.stop())
      commandSubscriptions.clear()
      Try(serverCommandsThread.stop(duration)).recover {
        case t: Throwable ⇒ logger.error("ZMQServerThread.stop failed", t)
      }
      // todo: cancel subscriptions?
      Try(context.close()).recover {
        case t: Throwable ⇒ logger.error("ZMQ.context.close failed", t)
      }
      true
    }
  }

  protected class CommandSubjectSubscription(val requestMatcher: RequestMatcher,
                                             val inputDeserializer: RequestDeserializer[RequestBase])
    extends SubjectSubscription[CommandEvent[RequestBase]] {

    override protected val subject = ConcurrentSubject.publishToOne[CommandEvent[RequestBase]]

    override def remove(): Unit = {
      commandSubscriptions.remove(this)
    }

    override def add(): Unit = {
      commandSubscriptions.add(this)
    }
  }

  protected def processor(request: ZMQServerRequest): Task[Any] = {
    val deserializeTask: Task[(CommandSubjectSubscription, RequestBase)] = Task.eval {
      var subscr: CommandSubjectSubscription = null
      val msg = MessageReader.fromString[RequestBase](request.message, (reader: Reader, headers: Headers) ⇒ {
        implicit val fakeRequest: RequestBase = DynamicRequest(EmptyBody, RequestHeaders(headers))

        getRandom(commandSubscriptions.lookupAll(fakeRequest)).map { subscription ⇒
          subscr = subscription
          subscription.inputDeserializer(reader, headers)
        } getOrElse {
          throw BadGateway(ErrorBody(ErrorCode.ZMQ_SUBSCRIPTION_NOT_FOUND, Some(fakeRequest.headers.hrl.toString)))
        }
      })
      (subscr, msg)
    }

    deserializeTask.flatMap { case (subscription, r) ⇒
      implicit val message = r

      val cb = CallbackTask[ResponseBase]
      val command = CommandEvent(message, cb)
      val tPublish = subscription.publish(command)

      Task.zip2(cb.task, tPublish)
        .map(_._1)
        .materialize
        .map { response ⇒
          val responseString = response match {
            case Success(r: ResponseBase) ⇒
              r.serializeToString

            case Failure(e) ⇒
              InternalServerError(ErrorBody(ErrorCode.ZMQ_UNHANDLED, Some(e.toString))).serializeToString
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
  protected val emptyDeserializer: MessageDeserializer[RequestBase] = (reader: Reader, headers: Headers) ⇒ {
    DynamicRequest(EmptyBody, RequestHeaders(headers))
  }
}

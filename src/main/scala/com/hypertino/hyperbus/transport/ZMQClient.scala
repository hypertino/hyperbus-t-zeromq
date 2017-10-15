package com.hypertino.hyperbus.transport

import com.hypertino.hyperbus.model.{RequestBase, ResponseBase}
import com.hypertino.hyperbus.serialization.ResponseBaseDeserializer
import com.hypertino.hyperbus.transport.api.{ClientTransport, PublishResult, ServiceResolver}
import com.hypertino.hyperbus.transport.zmq._
import com.hypertino.hyperbus.util.ConfigUtils._
import com.hypertino.hyperbus.util.{SchedulerInjector, ServiceResolverInjector}
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Scheduler
import org.zeromq.ZMQ
import scaldi.Injector

import scala.concurrent.duration.{FiniteDuration, _}

class ZMQClient(val serviceResolver: ServiceResolver,
                val defaultPort: Int,
                val zmqIOThreadCount: Int,
                val askTimeout: FiniteDuration,
                val keepAliveTimeout: FiniteDuration,
                val maxSockets: Int,
                val maxOutputQueueSize: Int)
               (implicit val scheduler: Scheduler) extends ClientTransport with StrictLogging {

  def this(config: Config, inj: Injector) = this(
    ServiceResolverInjector(config.getOptionString("resolver"))(inj),
    config.getOptionInt("default-port").getOrElse(10050),
    config.getOptionInt("zmq-io-threads").getOrElse(1),
    config.getOptionDuration("ask-timeout").getOrElse(60.seconds),
    config.getOptionDuration("keep-alive-timeout").getOrElse(60.seconds),
    config.getOptionInt("max-sockets").getOrElse(16384),
    config.getOptionInt("max-output-queue-size").getOrElse(16384)
  )(
    SchedulerInjector(config.getOptionString("scheduler"))(inj)
  )

  protected val context = ZMQ.context(zmqIOThreadCount)
  context.setMaxSockets(maxSockets)

  protected val askThread = new ZMQClientThread(
    context,
    serviceResolver,
    keepAliveTimeout,
    defaultPort,
    maxSockets,
    maxOutputQueueSize
  )

  override def ask(message: RequestBase, responseDeserializer: ResponseBaseDeserializer): Task[ResponseBase] = {
    serviceResolver
      .lookupService(message)
      .flatMap { serviceEndpoint ⇒
      Task.create[ResponseBase] { (_, callback) ⇒
        askThread.ask(message.serializeToString,
          message.headers.correlationId,
          responseDeserializer,
          serviceEndpoint,
          System.currentTimeMillis + askTimeout.toMillis,
          callback
        )
      }
    }
  }

  override def publish(message: RequestBase): Task[PublishResult] = ???

  override def shutdown(duration: FiniteDuration): Task[Boolean] = {
    Task.eval {
      askThread.stop(duration.toMillis)
      context.close()
      true
    }
  }
}

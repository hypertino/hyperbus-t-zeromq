package com.hypertino.hyperbus.transport

import java.nio.ByteBuffer
import java.nio.channels.Pipe
import java.util.concurrent.ConcurrentLinkedQueue

import com.hypertino.hyperbus.IdGenerator
import com.hypertino.hyperbus.model.{ErrorBody, MessagingContext, RequestBase, ResponseBase, ServiceUnavailable}
import com.hypertino.hyperbus.serialization.ResponseBaseDeserializer
import com.hypertino.hyperbus.transport.api.{ClientTransport, PublishResult, ServiceResolver}
import com.hypertino.hyperbus.transport.zmq._
import com.hypertino.hyperbus.util.ConfigUtils._
import com.hypertino.hyperbus.util.{SchedulerInjector, ServiceResolverInjector}
import com.typesafe.config.Config
import monix.eval.Task
import monix.execution.Scheduler
import org.zeromq.ZMQ
import scaldi.Injector

import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.Failure

class ZMQClient(val serviceResolver: ServiceResolver,
                val zmqIOThreadCount: Int,
                val askTimeout: FiniteDuration,
                val keepAliveTimeout: FiniteDuration,
                val maxSocketsPerServer: Int,
                val maxSockets: Int,
                val defaultPort: Int)
               (implicit val scheduler: Scheduler) extends ClientTransport {

  def this(config: Config)(implicit inj: Injector) = this(
    ServiceResolverInjector(config.getOptionString("resolver")),
    config.getOptionInt("zmq-io-threads").getOrElse(1),
    config.getOptionDuration("ask-timeout").getOrElse(60.seconds),
    config.getOptionDuration("keep-alive-timeout").getOrElse(60.seconds),
    config.getOptionInt("max-sockets-per-server").getOrElse(32),
    config.getOptionInt("max-sockets").getOrElse(16384),
    config.getOptionInt("default-port").getOrElse(10050)
  )(
    SchedulerInjector(config.getOptionString("scheduler"))(inj)
  )

  protected val context = ZMQ.context(zmqIOThreadCount)
  context.setMaxSockets(maxSockets)

  protected val askPipe = Pipe.open()
  protected val askSink = askPipe.sink()
  askSink.configureBlocking(false)
  protected val askQueue = new ConcurrentLinkedQueue[ZMQClientCommand]
  protected val askThread = new ZMQClientThread(
    context,
    serviceResolver,
    askPipe,
    askQueue,
    keepAliveTimeout,
    maxSocketsPerServer,
    defaultPort
  )

  override def ask(message: RequestBase, responseDeserializer: ResponseBaseDeserializer): Task[ResponseBase] = {
    serviceResolver.lookupService(message.headers.hri.serviceAddress).flatMap { serviceEndpoint ⇒
      Task.create[ResponseBase] { (_, callback) ⇒
        val askCommand = ZMQClientAsk(message.serializeToString,
          message.headers.correlationId.getOrElse(IdGenerator.create()),
          responseDeserializer,
          serviceEndpoint,
          System.currentTimeMillis + askTimeout.toMillis,
          callback
        )
        sendClientCommand(askCommand)

        () => {
          askQueue.removeIf(_ == askCommand)
        }
      }
    }
  }

  override def publish(message: RequestBase): Task[PublishResult] = ???

  override def shutdown(duration: FiniteDuration): Task[Boolean] = {
    Task.eval {
      clearAndShutdownAskCommands()
      sendClientCommand(ZMQClientThreadStop)
      askSink.close()
      askThread.join(duration.toMillis)
      clearAndShutdownAskCommands() // if something came while were closing sink
      context.close()
      true
    }
  }

  protected def sendClientCommand(command: ZMQClientCommand): Unit = {
    askQueue.add(command)
    askSink.write(ByteBuffer.wrap(Array[Byte](0)))
  }

  protected def clearAndShutdownAskCommands(): Unit = {
    var cmd: ZMQClientCommand = null
    do {
      cmd = askQueue.poll()
      cmd match {
        case ask: ZMQClientAsk ⇒
          implicit val mcx = MessagingContext(ask.correlationId)
          ask.callback(Failure(ServiceUnavailable(ErrorBody("shutdown_requested"))))
        case _ ⇒
      }
    } while (cmd != null)
  }
}

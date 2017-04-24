package com.hypertino.hyperbus.transport

import java.nio.channels.Pipe
import java.util.concurrent.ConcurrentLinkedQueue

import com.hypertino.hyperbus.model.{ErrorBody, MessagingContext, RequestBase, ResponseBase, ServiceUnavailable}
import com.hypertino.hyperbus.serialization.ResponseBaseDeserializer
import com.hypertino.hyperbus.transport.api.{ClientTransport, PublishResult, ServiceResolver}
import com.hypertino.hyperbus.transport.zmq._
import com.hypertino.hyperbus.util.ConfigUtils._
import com.hypertino.hyperbus.util.{SchedulerInjector, SeqGenerator, ServiceResolverInjector}
import com.typesafe.config.Config
import monix.eval.Task
import monix.execution.Scheduler
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import scaldi.Injector

import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.Failure

class ZMQClient(val serviceResolver: ServiceResolver,
                val defaultPort: Int,
                val zmqIOThreadCount: Int,
                val askTimeout: FiniteDuration,
                val keepAliveTimeout: FiniteDuration,
                val maxSockets: Int,
                val maxOutputQueueSize: Int)
               (implicit val scheduler: Scheduler) extends ClientTransport with ZMQCommandsProducer[ZMQClientCommand] {

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

  protected val log = LoggerFactory.getLogger(getClass)
  protected val context = ZMQ.context(zmqIOThreadCount)
  context.setMaxSockets(maxSockets)

  protected val askPipe = Pipe.open()
  protected val commandsSink = askPipe.sink()
  commandsSink.configureBlocking(false)
  protected val commandsQueue = new ConcurrentLinkedQueue[ZMQClientCommand]
  protected val askThread = new ZMQClientThread(
    context,
    serviceResolver,
    askPipe,
    commandsQueue,
    keepAliveTimeout,
    defaultPort,
    maxSockets,
    maxOutputQueueSize
  )

  override def ask(message: RequestBase, responseDeserializer: ResponseBaseDeserializer): Task[ResponseBase] = {
    serviceResolver.lookupService(message.headers.hri.serviceAddress).flatMap { serviceEndpoint ⇒
      Task.create[ResponseBase] { (_, callback) ⇒
        val askCommand = new ZMQClientAsk(message.serializeToString,
          message.headers.correlationId.getOrElse(SeqGenerator.create()),
          responseDeserializer,
          serviceEndpoint,
          System.currentTimeMillis + askTimeout.toMillis,
          callback
        )
        sendCommand(askCommand)

        () => {
          askCommand.cancel()
        }
      }
    }
  }

  override def publish(message: RequestBase): Task[PublishResult] = ???

  override def shutdown(duration: FiniteDuration): Task[Boolean] = {
    Task.eval {
      clearAndShutdownAskCommands()
      sendCommand(ZMQClientThreadStop)
      commandsSink.close()
      askThread.join(duration.toMillis)
      clearAndShutdownAskCommands() // if something came while were closing sink
      context.close()
      true
    }
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
}

package com.hypertino.hyperbus.transport.zmq

import java.nio.channels.Pipe
import java.util.concurrent.ConcurrentLinkedQueue

import monix.eval.Task
import monix.execution.Scheduler
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import org.zeromq.ZMQ.{Context, Poller}

import scala.collection.mutable
import scala.concurrent.duration.{FiniteDuration, _}

private[transport] class ZMQServerThread(context: Context,
                                         protected val commandsPipe: Pipe,
                                         protected val commandsQueue: ConcurrentLinkedQueue[ZMQServerCommand],
                                         processor: (ZMQServerRequest) ⇒ Task[Unit],
                                         interface: String,
                                         port: Int
                                        )
                                        (implicit scheduler: Scheduler) extends ZMQThreadBase[ZMQServerCommand] {

  protected val log = LoggerFactory.getLogger(getClass)

  val thread = new Thread(() ⇒ run(), "zmq-ask")
  thread.start()

  def join(millis: Long): Unit = thread.join(millis)

  protected def run(): Unit = {
    var shutdown = false
    val commandsSource = commandsPipe.source()
    commandsSource.configureBlocking(false)

    val expectingCommands = mutable.MutableList[ZMQServerCommand]()
    var waitTimeout = 60000

    val frontend = context.socket(ZMQ.ROUTER)
    frontend.bind(s"tcp://$interface:$port") // todo: handle when port is busy

    var shutdownTimeout: FiniteDuration = 10.seconds
    val poller = context.poller(2)
    poller.register(commandsSource, Poller.POLLIN)
    poller.register(frontend, Poller.POLLIN)

    do {
      if (poller.poll(waitTimeout) > 0) {
        if (poller.pollin(0)) { // consume new commands
          expectingCommands ++= fetchNewCommands(commandsSource)
        }

        if (poller.pollin(1)) {
          // todo: log if no full message was received
          val requestId = frontend.recv()
          if (frontend.hasReceiveMore) {
            val nullFrame = frontend.recv()
            if (frontend.hasReceiveMore) {
              val message = frontend.recvStr()
              processor(ZMQServerRequest(requestId, message)).runAsync
            }
          }
        }
      }

      expectingCommands.foreach {
        case ZMQServerThreadStop(timeout) ⇒
          shutdownTimeout = timeout
          shutdown = true

        case response: ZMQServerResponse ⇒
          frontend.send(response.replyId, ZMQ.SNDMORE)
          frontend.send(null: Array[Byte], ZMQ.SNDMORE)
          frontend.send(response.message)
      }
      //val waitingResponse = waitingResponse

    } while (!shutdown)

    frontend.setLinger(shutdownTimeout.toMillis)
    frontend.close()
  }
}

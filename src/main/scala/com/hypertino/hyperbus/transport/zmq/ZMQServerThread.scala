package com.hypertino.hyperbus.transport.zmq

import java.nio.channels.Pipe
import java.util.concurrent.{ConcurrentLinkedQueue, LinkedBlockingQueue}

import monix.eval.Task
import monix.execution.Scheduler
import org.slf4j.LoggerFactory
import org.zeromq.ZMQ
import org.zeromq.ZMQ.{Context, Poller}

import scala.collection.mutable
import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.control.NonFatal

private[transport] class ZMQServerThread(context: Context,
                                         protected val commandsPipe: Pipe,
                                         protected val commandsQueue: ConcurrentLinkedQueue[ZMQServerCommand],
                                         processor: (ZMQServerRequest) ⇒ Task[Any],
                                         interface: String,
                                         port: Int,
                                         responseTimeout: FiniteDuration
                                        )
                                        (implicit scheduler: Scheduler) extends ZMQThreadBase[ZMQServerCommand] {

  protected val log = LoggerFactory.getLogger(getClass)

  val thread = new Thread(() ⇒ run(), "zmq-commands")
  thread.start()

  def join(millis: Long): Unit = thread.join(millis)

  protected def run(): Unit = {
    try {
      val processorCommandQueue = new LinkedBlockingQueue[ZMQServerCommand]()
      val processorThread = new Thread(() ⇒ runProcessor(processorCommandQueue), "zmq-commands-processor")
      processorThread.start()

      var shutdown = false
      val commandsSource = commandsPipe.source()
      commandsSource.configureBlocking(false)

      val expectingCommands = mutable.MutableList[ZMQServerCommand]()
      var waitTimeout = 60000

      val frontend = context.socket(ZMQ.ROUTER)
      frontend.bind(s"tcp://$interface:$port") // todo: handle when port is busy

      var shutdownTimeout: FiniteDuration = 10.seconds
      val poller = context.poller(2)
      val commandsIndex = poller.register(commandsSource, Poller.POLLIN)
      val frontendIndex = poller.register(frontend, Poller.POLLIN)

      do {
        if (poller.poll(waitTimeout) > 0) {
          if (poller.pollin(commandsIndex)) { // consume new commands
            expectingCommands ++= fetchNewCommands(commandsSource)
          }

          if (poller.pollin(frontendIndex)) {
            // todo: handle & log if no full message was received
            // todo: use zsocket.getsockopt(zmq.EVENTS) !!!!

            while ((frontend.getEvents & Poller.POLLIN) != 0) {
              val clientId = frontend.recv()
              if (frontend.hasReceiveMore) {
                val nullFrame = frontend.recv() // todo: test that this is a null frame
                if (nullFrame.nonEmpty) {
                  log.warn("a null frame is" + new String(nullFrame))
                }
                if (frontend.hasReceiveMore) {
                  val requestId = frontend.recv()
                  if (frontend.hasReceiveMore) {
                    val message = frontend.recvStr()
                    processorCommandQueue.put(ZMQServerRequest(clientId, requestId, message))
                    //processor(ZMQServerRequest(clientId, requestId, message)).timeout(responseTimeout).runAsync
                  } else {
                    log.warn("a333")
                  }
                } else {
                  log.warn("a222")
                }
              }
              else {
                log.warn("a111")
              }
            }
          }
        }

        expectingCommands.foreach {
          case ZMQServerThreadStop(timeout) ⇒
            processorCommandQueue.put(ZMQServerThreadStop(timeout))
            shutdownTimeout = timeout
            shutdown = true

          case response: ZMQServerResponse ⇒
            frontend.send(response.clientId, ZMQ.SNDMORE)
            frontend.send(null: Array[Byte], ZMQ.SNDMORE)
            frontend.send(response.replyId, ZMQ.SNDMORE)
            frontend.send(response.message)
        }
        //val waitingResponse = waitingResponse

        expectingCommands.clear()
      } while (!shutdown)

      frontend.setLinger(shutdownTimeout.toMillis)
      frontend.close()
      processorThread.join(shutdownTimeout.toMillis)
    }
    catch {
      case NonFatal(e) ⇒
        log.error("Unhandled exception", e)
    }
  }

  protected def runProcessor(commandsQueue: LinkedBlockingQueue[ZMQServerCommand]): Unit = {
    try {
      var shutdown = false
      while (!shutdown) {
        commandsQueue.take() match {
          case ZMQServerThreadStop(timeout) ⇒ shutdown = true
          case ZMQServerRequest(clientId, requestId, message) ⇒
            processor(ZMQServerRequest(clientId, requestId, message)).timeout(responseTimeout).runAsync
        }
      }
    }
    catch {
      case NonFatal(e) ⇒
        log.error("Unhandled exception", e)
    }
  }
}

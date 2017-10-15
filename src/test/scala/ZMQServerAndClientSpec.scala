import java.io.Reader

import com.hypertino.hyperbus.model.{Headers, MessagingContext, RequestBase, ResponseBase, ResponseHeaders}
import com.hypertino.hyperbus.serialization.{RequestDeserializer, ResponseBaseDeserializer}
import com.hypertino.hyperbus.transport.api.matchers.RequestMatcher
import com.hypertino.hyperbus.transport.{ZMQClient, ZMQServer}
import monix.eval.Task
import monix.execution.Ack.Continue
import monix.execution.atomic.AtomicInt
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.Matcher
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._
import scala.util.Success

class ZMQServerAndClientSpec extends FlatSpec with ScalaFutures with Matchers {
  implicit val mcx = MessagingContext("123")
  val mockResolver = MockResolver(None)
  var port = 11050
  val requestDeserializer: RequestDeserializer[MockRequest] = MockRequest.apply(_: Reader, _: Headers)
  val responseDeserializer : ResponseBaseDeserializer = (reader: Reader, headers: Headers) ⇒ {
    MockResponse(MockBody(reader, ResponseHeaders(headers).contentType), ResponseHeaders(headers))
  }
  implicit val scheduler = monix.execution.Scheduler.Implicits.global
  implicit var defaultPatience = PatienceConfig(timeout = Span(40, Seconds), interval = Span(1, Seconds))

  "Server" should "handle client command" in {
    val serverTransport = new ZMQServer(
      port,
      "127.0.0.1",
      zmqIOThreadCount = 1,
      maxSockets = 150,
      serverResponseTimeout = 5.seconds
    )
    Thread.sleep(500)

    try {
      val clientTransport = new ZMQClient(
        mockResolver,
        defaultPort = port,
        zmqIOThreadCount = 1,
        askTimeout = 5.seconds,
        keepAliveTimeout = 20.seconds,
        maxSockets = 150,
        maxOutputQueueSize = 10
      )
      Thread.sleep(500)

      serverTransport.commands[MockRequest](
        RequestMatcher("hb://mock", "post"),
        requestDeserializer
      ).subscribe { implicit c ⇒
        c.reply(Success(
          MockResponse(MockBody(c.request.body.test.reverse))
        ))
        Continue
      }
      Thread.sleep(500)

      val f = clientTransport.ask(MockRequest(MockBody("yey Maga")), responseDeserializer).runAsync
      f.futureValue should equalResp(MockResponse(MockBody("agaM yey")))

      clientTransport.shutdown(1.second).runAsync.futureValue
    }
    finally {
      serverTransport.shutdown(1.second).runAsync.futureValue
      Thread.sleep(500)
    }
  }

  "Single server" should "handle multiple clients commands" in {
    manyToMany(1,100,3200,32)
  }

  "Multiple servers" should "handle multiple clients commands" in {
    manyToMany(15,20,3200,32)
  }

  "Multiple servers" should "handle single clients commands" in {
    manyToMany(15,1,3200,32)
  }

  "Single server" should "handle single clients commands" in {
    manyToMany(1,1,3200,32)
  }

  def manyToMany(serverCount: Int, clientCount: Int, messageCount: Int, batchSize: Int): Unit = {
    messageCount % batchSize shouldBe 0
    val servers = 0 until serverCount map { i ⇒
      val serverPort = i + port + 1
      new ZMQServer(
        serverPort,
        "127.0.0.1",
        zmqIOThreadCount = 1,
        maxSockets = 20,
        serverResponseTimeout = 30.seconds
      )
    }
    Thread.sleep(2000)
    try {
      val clients = 0 until clientCount map { i ⇒
        val server = servers(i % servers.size)

        new ZMQClient(
          CyclicResolver(servers.map(_.port)),
          defaultPort = server.port,
          zmqIOThreadCount = 1,
          askTimeout = 30.seconds,
          keepAliveTimeout = 40.seconds,
          maxSockets = 20,
          maxOutputQueueSize = 16384
        )
      }
      Thread.sleep(2000)

      val subscriptions = servers.map { s ⇒
        s.commands[MockRequest](
          RequestMatcher("hb://mock", "post"),
          requestDeserializer
        ).subscribe { implicit c ⇒
          c.reply(Success(
            MockResponse(MockBody(c.request.body.test.reverse))
          ))
          Continue
        }
      }
      Thread.sleep(2000)

      val total = AtomicInt(0)

      val allTasks = 0 until (messageCount / batchSize) map { batch ⇒
        val tasks = 0 until batchSize map { i ⇒
          val idx = batch*batchSize + i
          val client = clients(idx % clients.size)
          val port = client.defaultPort
          implicit val mcx = MessagingContext(idx.toString)
          client.ask(MockRequest(MockBody(s"${port}/yey${idx.toString}")), responseDeserializer).map(r ⇒ (r, idx))
        }

        Task.fromFuture(Task
          .gatherUnordered(tasks)
          .map { results ⇒
            results.foreach { case (response: MockResponse[MockBody@unchecked], i) ⇒
              response.body.test should startWith(s"${i.toString.reverse}yey")
              total.increment()
            }
          }
          .runAsync)
      }
      Task.gatherUnordered(allTasks).runAsync.futureValue
      total.get should equal(messageCount)
      subscriptions.foreach(_.cancel)
      Task.gatherUnordered(clients.map(_.shutdown(20.seconds))).runAsync.futureValue
    }
    finally {
      Task.gatherUnordered(servers.map(_.shutdown(20.seconds))).runAsync.futureValue
      Thread.sleep(2000)
    }
  }
  def equalReq(other: RequestBase): Matcher[RequestBase] = EqualsMessage[RequestBase](other)
  def equalResp(other: ResponseBase): Matcher[ResponseBase] = EqualsMessage[ResponseBase](other)
}


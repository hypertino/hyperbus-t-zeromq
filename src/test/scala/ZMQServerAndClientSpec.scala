import java.io.Reader

import com.hypertino.binders.value.Obj
import com.hypertino.hyperbus.model.{HeadersMap, MessagingContext, RequestBase, ResponseBase, ResponseHeaders}
import com.hypertino.hyperbus.serialization.{MessageReader, RequestDeserializer, ResponseBaseDeserializer}
import com.hypertino.hyperbus.transport.{ZMQClient, ZMQServer}
import com.hypertino.hyperbus.transport.api.matchers.RequestMatcher
import monix.eval.Task
import monix.execution.Ack.Continue
import monix.execution.atomic.AtomicInt
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.Matcher
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FlatSpec, Matchers}
import org.zeromq.ZMQ

import scala.concurrent.duration._
import scala.util.Success

class ZMQServerAndClientSpec extends FlatSpec with ScalaFutures with Matchers {
  implicit val mcx = MessagingContext("123")
  val mockResolver = MockResolver(None)
  var port = 11050
  val requestDeserializer: RequestDeserializer[MockRequest] = MockRequest.apply(_: Reader, _: HeadersMap)
  val responseDeserializer : ResponseBaseDeserializer = (reader: Reader, headersMap: HeadersMap) ⇒ {
    MockResponse(MockBody(reader, ResponseHeaders(headersMap).contentType), ResponseHeaders(headersMap))
  }
  implicit val scheduler = monix.execution.Scheduler.Implicits.global
  implicit var defaultPatience = PatienceConfig(timeout = Span(5000, Millis), interval = Span(30, Millis))

  "Server" should "handle client command" in {
    val serverTransport = new ZMQServer(
      port,
      "127.0.0.1",
      zmqIOThreadCount = 1,
      maxSockets = 150,
      serverResponseTimeout = 100.milliseconds
    )

    val clientTransport = new ZMQClient(
      mockResolver,
      defaultPort = port,
      zmqIOThreadCount = 1,
      askTimeout = 5000.milliseconds,
      keepAliveTimeout = 10.seconds,
      maxSockets = 150,
      maxOutputQueueSize = 10
    )

    serverTransport.commands[MockRequest](
      RequestMatcher("hb://mock", "post"),
      requestDeserializer
    ).subscribe{ implicit c ⇒
      c.reply(Success(
        MockResponse(MockBody(c.request.body.test.reverse))
      ))
      Continue
    }


    val f = clientTransport.ask(MockRequest(MockBody("yey Maga")), responseDeserializer).runAsync
    f.futureValue should equalResp(MockResponse(MockBody("agaM yey")))

    clientTransport.shutdown(1.second).runAsync.futureValue
    serverTransport.shutdown(1.second).runAsync.futureValue
  }

  "Multiple servers" should "handle multiple clients commands" in {
    manyToMany(15,20,3000)
  }

  "Multiple servers" should "handle single clients commands" in {
    manyToMany(15,1,3000)
  }

  "Single server" should "handle multiple clients commands" in {
    manyToMany(1,100,3000)
  }

  def manyToMany(serverCount: Int, clientCount: Int, messageCount: Int): Unit = {
    val servers = 0 until serverCount map { i ⇒
      val serverPort = i + port + 1
      new ZMQServer(
        serverPort,
        "127.0.0.1",
        zmqIOThreadCount = 1,
        maxSockets = 20,
        serverResponseTimeout = 100.milliseconds
      )
    }

    val clients = 0 until clientCount map { i ⇒
      val server = servers(i % servers.size)

      new ZMQClient(
        CyclicResolver(servers.map(_.port)),
        defaultPort = server.port,
        zmqIOThreadCount = 1,
        askTimeout = 5000.milliseconds,
        keepAliveTimeout = 10.seconds,
        maxSockets = 20,
        maxOutputQueueSize = 16384
      )
    }

    val subscriptions = servers.map { s ⇒
      s.commands[MockRequest](
        RequestMatcher("hb://mock", "post"),
        requestDeserializer
      ).subscribe{ implicit c ⇒
        c.reply(Success(
          MockResponse(MockBody(c.request.body.test.reverse))
        ))
        Continue
      }
    }

    val tasks = 0 until messageCount map { i ⇒
      val client = clients(i % clients.size)
      client.ask(MockRequest(MockBody(s"yey${i.toString}")), responseDeserializer).map(r ⇒ (r, i))
    }

    val total = AtomicInt(0)
    val f = Task.gatherUnordered(tasks).runAsync
    f.futureValue.foreach { case (response: MockResponse[MockBody@unchecked], i) ⇒
      response.body.test should equal (s"${i.toString.reverse}yey")
      total.increment()
    }
    total.get should equal(messageCount)
    subscriptions.foreach(_.cancel)
    Task.gatherUnordered(clients.map(_.shutdown(10.seconds))).runAsync.futureValue
    Task.gatherUnordered(servers.map(_.shutdown(10.seconds))).runAsync.futureValue
  }
  def equalReq(other: RequestBase): Matcher[RequestBase] = EqualsMessage[RequestBase](other)
  def equalResp(other: ResponseBase): Matcher[ResponseBase] = EqualsMessage[ResponseBase](other)
}


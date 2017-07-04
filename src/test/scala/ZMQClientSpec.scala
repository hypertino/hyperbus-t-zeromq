import java.io.Reader

import com.hypertino.binders.value.Obj
import com.hypertino.hyperbus.model.annotations.{body, request, response}
import com.hypertino.hyperbus.model.{Body, ErrorBody, GatewayTimeout, HRL, HeadersMap, MessagingContext, Method, Request, RequestBase, Response, ResponseHeaders}
import com.hypertino.hyperbus.serialization.{MessageReader, RequestDeserializer, ResponseBaseDeserializer}
import com.hypertino.hyperbus.transport.ZMQClient
import com.hypertino.hyperbus.transport.api.{ServiceEndpoint, ServiceResolver}
import com.hypertino.hyperbus.transport.resolvers.PlainEndpoint
import monix.eval.Task
import monix.execution.atomic.{AtomicInt, AtomicLong}
import monix.reactive.Observable
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FlatSpec, Matchers}
import org.zeromq.ZMQ

import scala.concurrent.duration._

@body("mock")
case class MockBody(test: String) extends Body

@request(Method.POST, "hb://mock")
case class MockRequest(body: MockBody) extends Request[MockBody]

@response(200)
case class MockResponse[B <: MockBody](body: B) extends Response[B]

object MockRequest {
  def apply(s: String): MockRequest = {
    MessageReader.fromString[MockRequest](s, MockRequest.apply)
  }
}

object MockResponse

case class MockResolver(port: Option[Int]) extends ServiceResolver {
  override def serviceObservable(message: RequestBase): Observable[Seq[ServiceEndpoint]] = {
    Observable.now(Seq(PlainEndpoint("localhost", MockResolver.this.port)))
  }
}

case class CyclicResolver(ports: IndexedSeq[Int]) extends ServiceResolver {
  val current = AtomicInt(0)

  override def serviceObservable(message: RequestBase): Observable[Seq[ServiceEndpoint]] = {
    Observable.now(Seq(PlainEndpoint("localhost", Some(ports(current.incrementAndGet() % ports.size)))))
  }
}

class ZMQClientSpec extends FlatSpec with ScalaFutures with Matchers {
  implicit val mcx = MessagingContext("123")

  val responseDeserializer : ResponseBaseDeserializer = (reader: Reader, headersMap: HeadersMap) ⇒ {
    MockResponse(MockBody(reader, ResponseHeaders(headersMap).contentType), ResponseHeaders(headersMap))
  }

  val requestDeserializer: RequestDeserializer[MockRequest] = MockRequest.apply(_: Reader, _: HeadersMap)
  var port = 10050
  val mockResolver = MockResolver(None)
  implicit val scheduler = monix.execution.Scheduler.Implicits.global
  implicit var defaultPatience = PatienceConfig(timeout = Span(1000, Millis), interval = Span(30, Millis))

  "ask without server" should "timeout" in {
    port += 1
    val clientTransport = new ZMQClient(
      mockResolver,
      defaultPort = port,
      zmqIOThreadCount = 1,
      askTimeout = 500.milliseconds,
      keepAliveTimeout = 10.seconds,
      maxSockets = 150,
      maxOutputQueueSize = 10
    )

    val f = clientTransport.ask(MockRequest(MockBody("yey")), responseDeserializer).runAsync
    f.failed.futureValue shouldBe a[GatewayTimeout[_]]
  }

  "ask" should "send request and return response" in {
    port += 1

    val ctx = ZMQ.context(1)
    val response = MockResponse(MockBody("got-you"))

    val clientTransport = new ZMQClient(
      mockResolver,
      defaultPort = port,
      zmqIOThreadCount = 1,
      askTimeout = 5000.milliseconds,
      keepAliveTimeout = 10.seconds,
      maxSockets = 150,
      maxOutputQueueSize = 10
    )

    val repSocket = ctx.socket(ZMQ.REP)
    repSocket.setLinger(1000)
    repSocket.bind(s"tcp://*:$port")

    val f = clientTransport.ask(MockRequest(MockBody("yey")), responseDeserializer).runAsync

    val requestId = repSocket.recv()
    requestId.length should equal(8)
    val msg = MockRequest(repSocket.recvStr())
    msg.body.test should equal("yey")
    repSocket.send(requestId, ZMQ.SNDMORE)
    repSocket.send(response.serializeToString)
    f.futureValue should equal(response)

    repSocket.close()
    ctx.close()
  }

  "ask" should "send request and return multiple response to the same endpoint" in {
    port += 1

    val ctx = ZMQ.context(1)
    val response = MockResponse(MockBody("got-you"))
    val response2 = MockResponse(MockBody("got-you Julia"))

    val clientTransport = new ZMQClient(
      mockResolver,
      defaultPort = port,
      zmqIOThreadCount = 1,
      askTimeout = 50000.milliseconds,
      keepAliveTimeout = 100.seconds,
      maxSockets = 150,
      maxOutputQueueSize = 10
    )

    val repSocket = ctx.socket(ZMQ.REP)
    repSocket.setLinger(1000)
    repSocket.bind(s"tcp://*:$port")

    val f = clientTransport.ask(MockRequest(MockBody("yey")), responseDeserializer).runAsync
    val f2 = clientTransport.ask(MockRequest(MockBody("Julia?")), responseDeserializer).runAsync

    val requestId = repSocket.recv()
    requestId.length should equal(8)
    val msg = MockRequest(repSocket.recvStr())
    msg.body.test should equal("yey")
    repSocket.send(requestId, ZMQ.SNDMORE)
    repSocket.send(response.serializeToString)
    f.futureValue should equal(response)

    val requestId2 = repSocket.recv()
    requestId2.length should equal(8)
    val msg2 = MockRequest(repSocket.recvStr())
    msg2.body.test should equal("Julia?")
    repSocket.send(requestId2, ZMQ.SNDMORE)
    repSocket.send(response2.serializeToString)
    f2.futureValue should equal(response2)

    repSocket.close()
    ctx.close()
  }
}

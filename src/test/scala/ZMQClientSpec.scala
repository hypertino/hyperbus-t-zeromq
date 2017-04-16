import java.io.Reader

import com.hypertino.binders.value.Obj
import com.hypertino.hyperbus.model.annotations.{body, request, response}
import com.hypertino.hyperbus.model.{Body, ErrorBody, GatewayTimeout, MessagingContext, Method, Request, Response, ResponseHeaders}
import com.hypertino.hyperbus.serialization.{MessageReader, RequestDeserializer, ResponseBaseDeserializer}
import com.hypertino.hyperbus.transport.ZMQClient
import com.hypertino.hyperbus.transport.api.{ServiceEndpoint, ServiceResolver}
import monix.eval.Task
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
    MessageReader.from[MockRequest](s, MockRequest.apply(_,_))
  }
}

object MockResponse

case class MockResolver(port: Option[Int]) extends ServiceResolver {
  override def lookupService(serviceName: String): Task[ServiceEndpoint] = Task.now {
    new ServiceEndpoint {
      override def hostname: String = "localhost"
      override def port: Option[Int] = MockResolver.this.port
    }
  }
}

class ZMQClientSpec extends FlatSpec with ScalaFutures with Matchers {
  implicit val mcx = MessagingContext("123")

  val responseDeserializer : ResponseBaseDeserializer = (reader: Reader, obj: Obj) ⇒ {
    MockResponse(MockBody(reader, ResponseHeaders(obj).contentType), ResponseHeaders(obj))
  }

  val requestDeserializer: RequestDeserializer[MockRequest] = MockRequest.apply(_: Reader, _: Obj)
  var port = 10050
  val mockResolver = MockResolver(None)
  implicit val scheduler = monix.execution.Scheduler.Implicits.global
  implicit var defaultPatience = PatienceConfig(timeout = Span(1000, Millis), interval = Span(30, Millis))

  "ask without server" should "timeout" in {
    port += 1
    val clientTransport = new ZMQClient(
      mockResolver,
      zmqIOThreadCount = 1,
      askTimeout = 100.milliseconds,
      keepAliveTimeout = 10.seconds,
      maxSocketsPerServer = 10,
      maxSockets = 150,
      defaultPort = port
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
      zmqIOThreadCount = 1,
      askTimeout = 5000.milliseconds,
      keepAliveTimeout = 10.seconds,
      maxSocketsPerServer = 10,
      maxSockets = 150,
      defaultPort = port
    )

    val repSocket = ctx.socket(ZMQ.REP)
    repSocket.setLinger(1000)
    repSocket.bind(s"tcp://*:$port")

    val f = clientTransport.ask(MockRequest(MockBody("yey")), responseDeserializer).runAsync

    val msg = MockRequest(repSocket.recvStr())
    msg.body.test should equal("yey")
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
      zmqIOThreadCount = 1,
      askTimeout = 50000.milliseconds,
      keepAliveTimeout = 100.seconds,
      maxSocketsPerServer = 10,
      maxSockets = 150,
      defaultPort = port
    )

    val repSocket = ctx.socket(ZMQ.REP)
    repSocket.setLinger(1000)
    repSocket.bind(s"tcp://*:$port")

    val f = clientTransport.ask(MockRequest(MockBody("yey")), responseDeserializer).runAsync
    val f2 = clientTransport.ask(MockRequest(MockBody("Julia?")), responseDeserializer).runAsync

    val msg = MockRequest(repSocket.recvStr())
    msg.body.test should equal("yey")
    repSocket.send(response.serializeToString)
    f.futureValue should equal(response)

    val msg2 = MockRequest(repSocket.recvStr())
    msg2.body.test should equal("Julia?")
    repSocket.send(response2.serializeToString)
    f2.futureValue should equal(response2)

    repSocket.close()
    ctx.close()
  }
//
//  "XXXask" should "YYY" in {
//    port += 1
//
//    val ctx = ZMQ.context(1)
//    val response = MockResponse(MockBody("got-you"))
//
//    val clientTransport = new ZMQClient(
//      mockResolver,
//      zmqIOThreadCount = 1,
//      askTimeout = 5000.milliseconds,
//      keepAliveTimeout = 10.seconds,
//      maxSocketsPerServer = 10,
//      maxSockets = 150,
//      defaultPort = port
//    )
//
//    val repSocket = ctx.socket(ZMQ.ROUTER)
//    repSocket.setLinger(1000)
//    repSocket.bind(s"tcp://*:$port")
//
//    val f = clientTransport.ask(MockRequest(MockBody("yey")), responseDeserializer).runAsync
//
//    val a1 = repSocket.recv()
//    println(repSocket.hasReceiveMore)
//    val a2 = repSocket.recv()
//    println(repSocket.hasReceiveMore)
//    val a3 = repSocket.recv()
//    println(repSocket.hasReceiveMore)
//
//    val msg = MockRequest(repSocket.recvStr())
//    msg.body.test should equal("yey")
//    repSocket.send(response.serializeToString)
//    f.futureValue should equal(response)
//
//    repSocket.close()
//    ctx.close()
//  }
}

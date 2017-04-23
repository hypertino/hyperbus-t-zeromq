import java.io.Reader

import com.hypertino.binders.value.Obj
import com.hypertino.hyperbus.model.{MessagingContext, RequestBase, ResponseBase, ResponseHeaders}
import com.hypertino.hyperbus.serialization.{MessageReader, RequestDeserializer, ResponseBaseDeserializer}
import com.hypertino.hyperbus.transport.{ZMQClient, ZMQServer}
import com.hypertino.hyperbus.transport.api.matchers.RequestMatcher
import monix.execution.Ack.Continue
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
  val requestDeserializer: RequestDeserializer[MockRequest] = MockRequest.apply(_: Reader, _: Obj)
  val responseDeserializer : ResponseBaseDeserializer = (reader: Reader, obj: Obj) ⇒ {
    MockResponse(MockBody(reader, ResponseHeaders(obj).contentType), ResponseHeaders(obj))
  }
  implicit val scheduler = monix.execution.Scheduler.Implicits.global
  implicit var defaultPatience = PatienceConfig(timeout = Span(60000, Millis), interval = Span(30, Millis))

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
      zmqIOThreadCount = 1,
      askTimeout = 5000.milliseconds,
      keepAliveTimeout = 10.seconds,
      maxSockets = 150,
      defaultPort = port
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

    clientTransport.shutdown(1.second).runAsync
    serverTransport.shutdown(1.second).runAsync
  }

  def equalReq(other: RequestBase): Matcher[RequestBase] = EqualsMessage[RequestBase](other)
  def equalResp(other: ResponseBase): Matcher[ResponseBase] = EqualsMessage[ResponseBase](other)
}


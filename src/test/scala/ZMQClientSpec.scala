/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

import java.io.Reader

import com.hypertino.binders.value.Text
import com.hypertino.hyperbus.model.annotations.{body, request, response}
import com.hypertino.hyperbus.model.{Body, DefinedResponse, GatewayTimeout, Headers, MessagingContext, Method, Request, RequestBase, RequestHeaders, RequestMetaCompanion, Response, ResponseBase, ResponseHeaders, ResponseMeta}
import com.hypertino.hyperbus.serialization.{MessageReader, RequestDeserializer, ResponseBaseDeserializer}
import com.hypertino.hyperbus.transport.{ZMQClient, ZMQHeader}
import com.hypertino.hyperbus.transport.api.{ServiceEndpoint, ServiceResolver}
import com.hypertino.hyperbus.transport.resolvers.{PlainEndpoint, PlainResolver}
import monix.execution.atomic.AtomicInt
import monix.reactive.Observable
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FlatSpec, Matchers}
import org.zeromq.ZMQ

import scala.concurrent.duration._

@body("mock")
case class MockBody(test: String) extends Body

@request(Method.POST, "hb://mock")
case class MockRequest(body: MockBody) extends Request[MockBody] with DefinedResponse[MockResponse[MockBody]]

@response(200)
case class MockResponse[B <: MockBody](body: B) extends Response[B]
object MockResponse extends ResponseMeta[MockBody, Response[MockBody]]

trait MockRequestMetaCompanion {
  def apply(
             body: MockBody,
             headers: com.hypertino.hyperbus.model.Headers = com.hypertino.hyperbus.model.Headers.empty,
             query: com.hypertino.binders.value.Value = com.hypertino.binders.value.Null
           )(implicit mcx: MessagingContext): MockRequest
}

object MockRequest extends MockRequestMetaCompanion with RequestMetaCompanion[MockRequest] {
  def fromString(s: String): MockRequest = {
    MessageReader.fromString[MockRequest](s, MockRequest.apply)
  }

  type ResponseType = MockResponse[MockBody]
  implicit val meta = this
}

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

  val responseDeserializer : ResponseBaseDeserializer = (reader: Reader, headers: Headers) ⇒ {
    MockResponse(MockBody(reader, ResponseHeaders(headers).contentType), ResponseHeaders(headers))
  }

  val requestDeserializer: RequestDeserializer[MockRequest] = MockRequest.apply(_: Reader, _: Headers)
  var port = 10050
  val mockResolver = MockResolver(None)
  implicit val scheduler = monix.execution.Scheduler.Implicits.global
  implicit var defaultPatience = PatienceConfig(timeout = Span(1000, Millis), interval = Span(30, Millis))

  "ask" should "timeout if no server is found" in {
    port += 1
    val clientTransport = new ZMQClient(
      mockResolver,
      defaultPort = port,
      zmqIOThreadCount = 1,
      askTimeout = 1000.milliseconds,
      keepAliveTimeout = 10.seconds,
      maxSockets = 150,
      maxOutputQueueSize = 10
    )

    val f = clientTransport.ask(MockRequest(MockBody("yey")), responseDeserializer).runAsync
    f.failed.futureValue shouldBe a[GatewayTimeout[_]]
  }

  it should "send request and return response" in {
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
    val msg = MockRequest.fromString(repSocket.recvStr())
    msg.body.test should equal("yey")
    repSocket.send(requestId, ZMQ.SNDMORE)
    repSocket.send(response.serializeToString)
    f.futureValue should equal(response)

    repSocket.close()
    ctx.close()
  }

  it should "send request and return multiple response to the same endpoint" in {
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
    val msg = MockRequest.fromString(repSocket.recvStr())
    msg.body.test should equal("yey")
    repSocket.send(requestId, ZMQ.SNDMORE)
    repSocket.send(response.serializeToString)
    f.futureValue should equal(response)

    val requestId2 = repSocket.recv()
    requestId2.length should equal(8)
    val msg2 = MockRequest.fromString(repSocket.recvStr())
    msg2.body.test should equal("Julia?")
    repSocket.send(requestId2, ZMQ.SNDMORE)
    repSocket.send(response2.serializeToString)
    f2.futureValue should equal(response2)

    repSocket.close()
    ctx.close()
  }

  it should "send request with ZMQ-Send-To directly without resolving endpoint" in {
    port += 1

    val ctx = ZMQ.context(1)
    val response = MockResponse(MockBody("got-you"))

    val clientTransport = new ZMQClient(
      new PlainResolver(Map.empty),
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

    val f = clientTransport.ask(MockRequest(MockBody("yey"), headers=Headers(ZMQHeader.ZMQ_SEND_TO → s"127.0.0.1:$port")),
      responseDeserializer).runAsync

    val requestId = repSocket.recv()
    requestId.length should equal(8)
    val msg = MockRequest.fromString(repSocket.recvStr())
    msg.body.test should equal("yey")
    repSocket.send(requestId, ZMQ.SNDMORE)
    repSocket.send(response.serializeToString)
    f.futureValue should equal(response)

    repSocket.close()
    ctx.close()
  }
}

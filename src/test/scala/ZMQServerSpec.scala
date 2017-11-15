/*
 * Copyright (c) 2017 Magomed Abdurakhmanov, Hypertino
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */

import java.io.Reader

import com.hypertino.hyperbus.model.{Body, Header, Headers, Message, MessageHeaders, MessagingContext, RequestBase, ResponseBase, ResponseHeaders}
import com.hypertino.hyperbus.serialization.{MessageReader, RequestDeserializer, ResponseBaseDeserializer}
import com.hypertino.hyperbus.transport.ZMQServer
import com.hypertino.hyperbus.transport.api.matchers.RequestMatcher
import monix.execution.Ack.Continue
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.time.{Millis, Span}
import org.zeromq.ZMQ

import scala.concurrent.duration._
import scala.util.Success

class ZMQServerSpec extends FlatSpec with ScalaFutures with Matchers {
  implicit val mcx = MessagingContext("123")
  var port = 11050
  val requestDeserializer: RequestDeserializer[MockRequest] = MockRequest.apply(_: Reader, _: Headers)
  val responseDeserializer : ResponseBaseDeserializer = (reader: Reader, headers: Headers) ⇒ {
    MockResponse(MockBody(reader, ResponseHeaders(headers).contentType), ResponseHeaders(headers))
  }
  implicit val scheduler = monix.execution.Scheduler.Implicits.global
  implicit var defaultPatience = PatienceConfig(timeout = Span(60000, Millis), interval = Span(30, Millis))

  "Server" should "handle command" in {
    val serverTransport = new ZMQServer(
      port,
      "127.0.0.1",
      zmqIOThreadCount = 1,
      maxSockets = 150,
      serverResponseTimeout = 1000.milliseconds
    )
    try {
      serverTransport.commands[MockRequest](
        RequestMatcher("hb://mock", "post"),
        requestDeserializer
      ).subscribe { implicit c ⇒
        c.reply(Success(
          MockResponse(MockBody(c.request.body.test.reverse))
        ))
        Continue
      }

      val ctx = ZMQ.context(1)
      val clientSocket = ctx.socket(ZMQ.REQ)
      //clientSocket.setIdentity("123".getBytes())
      clientSocket.connect(s"tcp://localhost:$port")

      val ri = getRequestId(100500)
      clientSocket.send(ri, ZMQ.SNDMORE)
      clientSocket.send(MockRequest(MockBody("yey Maga")).serializeToString)

      val rr = clientSocket.recv()
      rr.toSeq should equal(ri.toSeq)
      val result = MessageReader.fromString(clientSocket.recvStr(), responseDeserializer)
      result should equalResp(MockResponse(MockBody("agaM yey")))

      clientSocket.close()
      ctx.close()
    }
    finally {
      serverTransport.shutdown(1.second).runAsync
    }
  }

  "Server" should "handle multiple commands" in {
    val serverTransport = new ZMQServer(
      port,
      "127.0.0.1",
      zmqIOThreadCount = 1,
      maxSockets = 150,
      serverResponseTimeout = 1000.milliseconds
    )
    try {
      serverTransport.commands[MockRequest](
        RequestMatcher("hb://mock", "post"),
        requestDeserializer
      ).subscribe { implicit c ⇒
        c.reply(Success(
          MockResponse(MockBody(c.request.body.test.reverse))
        ))
        Continue
      }

      val ctx = ZMQ.context(1)
      val clientSocket1 = ctx.socket(ZMQ.REQ)
      clientSocket1.connect(s"tcp://localhost:$port")

      val clientSocket2 = ctx.socket(ZMQ.REQ)
      clientSocket2.connect(s"tcp://localhost:$port")

      val ri = getRequestId(100500)
      clientSocket1.send(ri, ZMQ.SNDMORE)
      clientSocket1.send(MockRequest(MockBody("yey Maga")).serializeToString)
      clientSocket2.send(ri, ZMQ.SNDMORE)
      clientSocket2.send(MockRequest(MockBody("yey Alla")).serializeToString)

      val rr2 = clientSocket2.recv()
      rr2.toSeq should equal(ri.toSeq)
      val result2 = MessageReader.fromString(clientSocket2.recvStr(), responseDeserializer)
      result2 should equalResp(MockResponse(MockBody("allA yey")))

      val rr1 = clientSocket1.recv()
      rr1.toSeq should equal(ri.toSeq)
      val result1 = MessageReader.fromString(clientSocket1.recvStr(), responseDeserializer)
      result1 should equalResp(MockResponse(MockBody("agaM yey")))

      clientSocket1.close()
      clientSocket2.close()
      ctx.close()
    }
    finally {
      serverTransport.shutdown(1.second).runAsync
    }
  }

  def equalReq(other: RequestBase): Matcher[RequestBase] = EqualsMessage[RequestBase](other)
  def equalResp(other: ResponseBase): Matcher[ResponseBase] = EqualsMessage[ResponseBase](other)

  def getRequestId(requestId: Long): Array[Byte] = {
    val aRequestId = java.nio.ByteBuffer.allocate(8)
    aRequestId.putLong(requestId)
    aRequestId.flip()
    aRequestId.array()
  }
}

case class EqualsMessage[M <: Message[_ <: Body,_ <: MessageHeaders]](a: M) extends Matcher[M] {
  def apply(other: M): MatchResult = {
    if (other.getClass == a.getClass &&
      other.headers.toSet.filterNot(i ⇒ EqualsMessage.ignoredHeaders.contains(i._1)) == a.headers.toSet.filterNot(i ⇒ EqualsMessage.ignoredHeaders.contains(i._1)) &&
        //Map(other.headers.all: _*).equals(Map(a.headers.all: _*)) &&
      other.body == a.body) {
      MatchResult(true, s"$other is not equal to $a", s"$other should be equal to $a")
    }
    else {
      MatchResult(false, s"$other is not equal to $a", s"$other should be equal to $a")
    }
  }
}

object EqualsMessage {
  val ignoredHeaders = Set(Header.MESSAGE_ID, Header.CORRELATION_ID, Header.PARENT_ID)
}
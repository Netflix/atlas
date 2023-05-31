/*
 * Copyright 2014-2023 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.atlas.eval.stream

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.MediaTypes
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.headers._
import org.apache.pekko.stream.KillSwitches
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.scaladsl.Keep
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import munit.FunSuite

import scala.concurrent.Await
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.Using

class HostSourceSuite extends FunSuite {

  import scala.concurrent.duration._

  private implicit val system: ActorSystem = ActorSystem(getClass.getSimpleName)

  def source(response: => Try[HttpResponse]): Source[ByteString, NotUsed] = {
    val client = Flow[HttpRequest].map(_ => response)
    HostSource("http://localhost/api/test", client = client, delay = 1.milliseconds)
  }

  def compress(str: String): Array[Byte] = {
    import com.netflix.atlas.core.util.Streams._
    byteArray { out =>
      Using.resource(gzip(out))(_.write(str.getBytes(StandardCharsets.UTF_8)))
    }
  }

  test("ok") {
    val response = HttpResponse(StatusCodes.OK, entity = ByteString("ok"))
    val future = source(Success(response))
      .take(5)
      .map(_.decodeString(StandardCharsets.UTF_8))
      .runWith(Sink.seq[String])
    val result = Await.result(future, Duration.Inf).toList
    assertEquals(result, (0 until 5).map(_ => "ok").toList)
  }

  test("no size limit on data stream") {
    val entity = HttpEntity(ByteString("ok")).withSizeLimit(1)
    val response = HttpResponse(StatusCodes.OK, entity = entity)
    val future = source(Success(response))
      .take(5)
      .map(_.decodeString(StandardCharsets.UTF_8))
      .runWith(Sink.seq[String])
    val result = Await.result(future, Duration.Inf).toList
    assertEquals(result, (0 until 5).map(_ => "ok").toList)
  }

  test("handles decompression") {
    val headers = List(`Content-Encoding`(HttpEncodings.gzip))
    val data = ByteString(compress("ok"))
    val response = HttpResponse(StatusCodes.OK, headers = headers, entity = data)
    val future = source(Success(response))
      .take(5)
      .map(_.decodeString(StandardCharsets.UTF_8))
      .runWith(Sink.seq[String])
    val result = Await.result(future, Duration.Inf).toList
    assertEquals(result, (0 until 5).map(_ => "ok").toList)
  }

  test("retries on error response from host") {
    val response = HttpResponse(StatusCodes.BadRequest, entity = ByteString("error"))
    val latch = new CountDownLatch(5)
    val (switch, future) = source {
      latch.countDown()
      Success(response)
    }.viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.ignore)(Keep.both)
      .run()

    // If it doesn't retry successfully this should time out and fail the test
    latch.await(60, TimeUnit.SECONDS)

    switch.shutdown()
    Await.result(future, Duration.Inf)
  }

  test("retries on exception from host") {
    val latch = new CountDownLatch(5)
    val (switch, future) = source {
      latch.countDown()
      Failure(new IOException("cannot connect"))
    }.viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.ignore)(Keep.both)
      .run()

    // If it doesn't retry successfully this should time out and fail the test
    latch.await(60, TimeUnit.SECONDS)

    switch.shutdown()
    Await.result(future, Duration.Inf)
  }

  test("retries on exception from host entity source") {
    val latch = new CountDownLatch(5)
    val (switch, future) = source {
      latch.countDown()
      val source = Source.future(Future.failed[ByteString](new IOException("reset by peer")))
      val entity = HttpEntity(MediaTypes.`text/event-stream`, source)
      Success(HttpResponse(StatusCodes.OK, entity = entity))
    }.viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.ignore)(Keep.both)
      .run()

    // If it doesn't retry successfully this should time out and fail the test
    latch.await(60, TimeUnit.SECONDS)

    switch.shutdown()
    Await.result(future, Duration.Inf)
  }

  test("ref stops host source") {
    val response = Success(HttpResponse(StatusCodes.OK, entity = ByteString("ok")))
    val ref = EvaluationFlows.stoppableSource(source(response))
    ref.stop()
    val future = ref.source
      .map(_.decodeString(StandardCharsets.UTF_8))
      .runWith(Sink.seq[String])
    val result = Await.result(future, Duration.Inf).toList
    assert(result.isEmpty)
  }

  test("ref host source works until stopped") {
    val response = Success(HttpResponse(StatusCodes.OK, entity = ByteString("ok")))
    val ref = EvaluationFlows.stoppableSource(source(response))
    val future = ref.source
      .map(_.decodeString(StandardCharsets.UTF_8))
      .take(5)
      .runWith(Sink.seq[String])
    val result = Await.result(future, Duration.Inf).toList
    assertEquals(result, (0 until 5).map(_ => "ok").toList)
  }
}

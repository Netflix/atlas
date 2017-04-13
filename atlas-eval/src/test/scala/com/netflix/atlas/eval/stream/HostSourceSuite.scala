/*
 * Copyright 2014-2017 Netflix, Inc.
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

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers._
import akka.stream.ActorMaterializer
import akka.stream.KillSwitches
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.scalatest.FunSuite

import scala.concurrent.Await
import scala.concurrent.Promise

class HostSourceSuite extends FunSuite {

  import scala.concurrent.duration._

  implicit val system = ActorSystem(getClass.getSimpleName)
  implicit val materializer = ActorMaterializer()

  def source(client: HostSource.Client): Source[ByteString, NotUsed] = {
    HostSource("http://localhost/api/test", delay = 0.seconds, client = Some(client))
  }

  def compress(str: String): Array[Byte] = {
    import com.netflix.atlas.core.util.Streams._
    byteArray { out =>
      scope(gzip(out))(_.write(str.getBytes(StandardCharsets.UTF_8)))
    }
  }

  test("ok") {
    val response = HttpResponse(StatusCodes.OK, entity = ByteString("ok"))
    val client: HostSource.Client = _ => Promise.successful(response).future
    val future = source(client)
      .take(5)
      .map(_.decodeString(StandardCharsets.UTF_8))
      .runWith(Sink.seq[String])
    val result = Await.result(future, Duration.Inf).toList
    assert(result === (0 until 5).map(_ => "ok").toList)
  }

  test("handles decompression") {
    val headers = List(`Content-Encoding`(HttpEncodings.gzip))
    val data = ByteString(compress("ok"))
    val response = HttpResponse(StatusCodes.OK, headers = headers, entity = data)
    val client: HostSource.Client = _ => Promise.successful(response).future
    val future = source(client)
      .take(5)
      .map(_.decodeString(StandardCharsets.UTF_8))
      .runWith(Sink.seq[String])
    val result = Await.result(future, Duration.Inf).toList
    assert(result === (0 until 5).map(_ => "ok").toList)
  }

  test("retries on error response from host") {
    val response = HttpResponse(StatusCodes.BadRequest, entity = ByteString("error"))
    val latch = new CountDownLatch(5)
    val client: HostSource.Client = _ => {
      latch.countDown()
      Promise.successful(response).future
    }
    val (switch, future) = source(client)
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.ignore)(Keep.both)
      .run()

    // If it doesn't retry successfully this should time out and fail the test
    latch.await(60, TimeUnit.SECONDS)

    switch.shutdown()
    Await.result(future, Duration.Inf)
  }

  test("retries on exception from host") {
    val latch = new CountDownLatch(5)
    val client: HostSource.Client = _ => {
      latch.countDown()
      Promise.failed(new IOException("cannot connect")).future
    }
    val (switch, future) = source(client)
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.ignore)(Keep.both)
      .run()

    // If it doesn't retry successfully this should time out and fail the test
    latch.await(60, TimeUnit.SECONDS)

    switch.shutdown()
    Await.result(future, Duration.Inf)
  }

}

/*
 * Copyright 2014-2025 Netflix, Inc.
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
package com.netflix.atlas.pekko

import com.netflix.atlas.pekko.PekkoHttpClient.ClientConfig
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.Spectator
import com.netflix.spectator.api.Timer
import com.netflix.spectator.api.Utils
import com.netflix.spectator.ipc.IpcStatus
import munit.FunSuite
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.HttpsConnectionContext
import org.apache.pekko.http.scaladsl.model.HttpMethods
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.http.scaladsl.settings.ConnectionPoolSettings
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.stream.Collectors
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class PekkoHttpClientSuite extends FunSuite {

  import PekkoHttpClientSuite.*
  private implicit val system: ActorSystem = ActorSystem(getClass.getSimpleName)

  override def afterAll(): Unit = {
    system.terminate()
  }

  override def beforeEach(context: BeforeEach): Unit = {
    val globalRegistry = Spectator.globalRegistry()
    globalRegistry.removeAll()
    globalRegistry.add(new DefaultRegistry())
  }

  private def assertClientCall(status: IpcStatus, count: Long): Unit = {
    val timers = Spectator
      .globalRegistry()
      .timers()
      .filter { t =>
        t.id.name == "ipc.client.call" && Utils.getTagValue(t.id, status.key) == status.value
      }
      .collect(Collectors.toList[Timer])
    if (timers.isEmpty)
      fail(s"missing expected counter for status: $status")
    else
      assertEquals(timers.stream().mapToLong(_.count).sum(), count, s"status: $status")
  }

  test("singleRequest: ipc metrics on success") {
    val responses = List(
      Success(HttpResponse(StatusCodes.OK))
    )
    val client = new TestHttpClient(system, responses)
    val future = client.singleRequest(HttpRequest(HttpMethods.GET, Uri("/test")))
    Await.ready(future, Duration.Inf)
    future.value match {
      case Some(Success(_)) => assertClientCall(IpcStatus.success, 1)
      case response         => fail(s"invalid respones: $response")
    }
  }

  test("singleRequest: ipc metrics on failure") {
    val responses = List(
      Success(HttpResponse(StatusCodes.Unauthorized))
    )
    val client = new TestHttpClient(system, responses)
    val future = client.singleRequest(HttpRequest(HttpMethods.GET, Uri("/test")))
    Await.ready(future, Duration.Inf)
    future.value match {
      case Some(Success(_)) => assertClientCall(IpcStatus.access_denied, 1)
      case response         => fail(s"invalid respones: $response")
    }
  }

  test("singleRequest: ipc metrics on exception") {
    val responses = List(
      Failure(new NullPointerException())
    )
    val client = new TestHttpClient(system, responses)
    val future = client.singleRequest(HttpRequest(HttpMethods.GET, Uri("/test")))
    Await.ready(future, Duration.Inf)
    future.value match {
      case Some(Failure(_)) => assertClientCall(IpcStatus.unexpected_error, 1)
      case response         => fail(s"invalid respones: $response")
    }
  }

  private def flowRequest(client: PekkoHttpClient, request: HttpRequest): Try[HttpResponse] = {
    val settings = ConnectionPoolSettings(system)
      .withMaxRetries(3)
      .withBaseConnectionBackoff(FiniteDuration(0, TimeUnit.MILLISECONDS))
      .withMaxConnectionBackoff(FiniteDuration(0, TimeUnit.MILLISECONDS))
    val config = ClientConfig(settings = Some(settings))
    val future = Source
      .single(request)
      .map(r => r -> NotUsed)
      .via(client.superPool(config))
      .map(_._1)
      .runWith(Sink.head)
    Await.result(future, Duration.Inf)
  }

  test("superPool: ipc metrics on success") {
    val responses = List(
      Success(HttpResponse(StatusCodes.OK))
    )
    val client = new TestHttpClient(system, responses)
    flowRequest(client, HttpRequest(HttpMethods.GET, Uri("/test"))) match {
      case Success(_) => assertClientCall(IpcStatus.success, 1)
      case response   => fail(s"invalid respones: $response")
    }
  }

  test("superPool: ipc metrics on failure") {
    val responses = List(
      Success(HttpResponse(StatusCodes.Unauthorized))
    )
    val client = new TestHttpClient(system, responses)
    flowRequest(client, HttpRequest(HttpMethods.GET, Uri("/test"))) match {
      case Success(_) => assertClientCall(IpcStatus.access_denied, 1)
      case response   => fail(s"invalid respones: $response")
    }
  }

  test("superPool: ipc metrics on exception") {
    val responses = List(
      Failure(new NullPointerException()),
      Failure(new NullPointerException()),
      Failure(new NullPointerException()),
      Failure(new NullPointerException())
    )
    val client = new TestHttpClient(system, responses)
    flowRequest(client, HttpRequest(HttpMethods.GET, Uri("/test"))) match {
      case Failure(_) => assertClientCall(IpcStatus.unexpected_error, 4)
      case response   => fail(s"invalid respones: $response")
    }
  }

  test("superPool: ipc metrics on throttled") {
    val responses = List(
      Success(HttpResponse(StatusCodes.TooManyRequests)),
      Success(HttpResponse(StatusCodes.TooManyRequests)),
      Success(HttpResponse(StatusCodes.TooManyRequests)),
      Success(HttpResponse(StatusCodes.OK))
    )
    val client = new TestHttpClient(system, responses)
    flowRequest(client, HttpRequest(HttpMethods.GET, Uri("/test"))) match {
      case Success(_) =>
        assertClientCall(IpcStatus.throttled, 3)
        assertClientCall(IpcStatus.success, 1)
      case response =>
        fail(s"invalid respones: $response")
    }
  }
}

object PekkoHttpClientSuite {

  class TestHttpClient(system: ActorSystem, responses: List[Try[HttpResponse]])
      extends PekkoHttpClient.HttpClientImpl("test")(system) {

    private val queue = {
      val responseQueue = new ArrayBlockingQueue[Try[HttpResponse]](responses.size)
      responses.foreach(responseQueue.add)
      responseQueue
    }

    override protected def doSingleRequest(request: HttpRequest): Future[HttpResponse] = {
      val response = queue.poll()
      if (response == null)
        Future.failed(new IllegalStateException("no valid response"))
      else
        Future.fromTry(response)
    }

    override protected def superPoolFlow[C](
      connectionContext: HttpsConnectionContext,
      settings: ConnectionPoolSettings
    ): Flow[(HttpRequest, C), (Try[HttpResponse], C), NotUsed] = {
      Flow[(HttpRequest, C)]
        .map {
          case (_, context) =>
            val response = queue.poll()
            if (response == null)
              throw new IllegalStateException("no valid response")
            else
              response -> context
        }
    }
  }
}

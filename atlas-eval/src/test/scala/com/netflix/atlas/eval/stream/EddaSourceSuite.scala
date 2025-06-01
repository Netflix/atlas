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
package com.netflix.atlas.eval.stream

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.MediaTypes
import org.apache.pekko.http.scaladsl.model.StatusCode
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.headers.*
import org.apache.pekko.stream.ConnectionException
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import com.netflix.atlas.core.util.Streams
import com.netflix.atlas.eval.stream.EddaSource.GroupResponse
import munit.FunSuite

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.Using

class EddaSourceSuite extends FunSuite {

  private def mkResponse(data: String, status: StatusCode = StatusCodes.OK): HttpResponse = {
    mkResponse(data.getBytes(StandardCharsets.UTF_8), false, status)
  }

  private def mkResponse(data: Array[Byte], gzip: Boolean, status: StatusCode): HttpResponse = {
    val headers = List(
      `Content-Encoding`(if (gzip) HttpEncodings.gzip else HttpEncodings.identity)
    )
    HttpResponse(status, headers = headers, entity = data)
  }

  private def gzip(data: String): Array[Byte] = {
    Streams.byteArray { out =>
      Using.resource(Streams.gzip(out)) { gz =>
        gz.write(data.getBytes(StandardCharsets.UTF_8))
      }
    }
  }

  private implicit val system: ActorSystem = ActorSystem(getClass.getSimpleName)
  private implicit val mat: Materializer = Materializer(system)

  private val eddaResponseSingleGroup: String =
    """[
      |  {
      |    "instances": [
      |      {
      |        "privateIpAddress": "1.2.3.4",
      |        "instanceId": "id1"
      |      },
      |      {
      |        "privateIpAddress": "1.2.3.5",
      |        "instanceId": "id2"
      |      }
      |    ]
      |  }
      |]""".stripMargin

  private val eddaResponse2Groups: String =
    """[
      |  {
      |    "instances": [
      |      {
      |        "privateIpAddress": "1.2.3.4",
      |        "instanceId": "id1"
      |      },
      |      {
      |        "privateIpAddress": "1.2.3.5",
      |        "instanceId": "id2"
      |      }
      |    ]
      |  },
      |  {
      |    "instances": [
      |      {
      |        "privateIpAddress": "1.2.3.6",
      |        "instanceId": "id3"
      |      }
      |    ]
      |  }
      |]""".stripMargin

  private val eddaResponseOneEmptyGroup: String =
    """[
      |  {
      |    "instances": [
      |      {
      |        "privateIpAddress": "1.2.3.4",
      |        "instanceId": "id1"
      |      },
      |      {
      |        "privateIpAddress": "1.2.3.5",
      |        "instanceId": "id2"
      |      }
      |    ]
      |  },
      |  {
      |    "instances": []
      |  }
      |]""".stripMargin

  private def run(uri: String, response: Try[HttpResponse]): GroupResponse = {
    val context = TestContext.createContext(mat, response)
    val future = EddaSource(uri, context).runWith(Sink.head)
    Await.result(future, Duration.Inf)
  }

  test("supports compressed response") {
    val uri = "http://edda/v1/atuoScalingGroups/www-dev:7001"
    val res = run(uri, Success(mkResponse(gzip(eddaResponseSingleGroup), true, StatusCodes.OK)))
    assertEquals(res.uri, uri)
    assertEquals(res.instances.size, 2)
    assertEquals(res.instances.map(_.instanceId).toSet, Set("id1", "id2"))
  }

  test("invalid json response") {
    val uri = "http://edda/v1/autoScalingGroups/www-dev:7001"
    val res = run(uri, Success(mkResponse("[{\"foo\":\"bar\"}]")))
    assertEquals(res.uri, uri)
    assertEquals(res.instances.size, 0)
  }

  test("unknown asg") {
    val uri = "http://eddaa/v1/autoScalingGroups/www-dev:7001"
    intercept[NoSuchElementException] {
      run(uri, Success(mkResponse("unknown vip", StatusCodes.NotFound)))
    }
  }

  test("entity for bad status code is consumed") {
    val consumedLatch = new CountDownLatch(1)
    val source = Source
      .single(ByteString.empty)
      .map { data =>
        consumedLatch.countDown()
        data
      }
    val entity = HttpEntity(MediaTypes.`application/json`, source)
    val response = HttpResponse(StatusCodes.BadRequest, entity = entity)
    val uri = "http://edda/v1/autoScalingGroups/www-dev:7001"
    intercept[NoSuchElementException] {
      run(uri, Success(response))
    }
    assert(consumedLatch.await(30, TimeUnit.SECONDS))
  }

  test("failure to connect") {
    val uri = "http://edda/v1/autoScalingGroups/www-dev:7001"
    intercept[NoSuchElementException] {
      run(uri, Failure(new ConnectionException("timeout")))
    }
  }

  test("failed response stream") {
    val uri = "http://edda/v1/autoScalingGroups/www-dev:7001"
    intercept[NoSuchElementException] {
      val source = Source.future(Future.failed[ByteString](new IOException("peer reset")))
      val entity = HttpEntity(MediaTypes.`application/json`, source)
      val response = HttpResponse(StatusCodes.OK, entity = entity)
      run(uri, Success(response))
    }
  }

  test("handles edda uri, 1 group") {
    val uri = "http://edda/api/v2/group/autoScalingGroups;cluster=atlas_lwcapi-main;_expand"
    val res = run(uri, Success(mkResponse(eddaResponseSingleGroup)))
    assertEquals(res.uri, uri)
    assertEquals(res.instances.size, 2)
    assertEquals(res.instances.map(_.instanceId).toSet, Set("id1", "id2"))
    assertEquals("http://1.2.3.4:7101", res.instances(0).substitute("http://{local-ipv4}:{port}"))
    assertEquals("http://1.2.3.5:7101", res.instances(1).substitute("http://{local-ipv4}:{port}"))
  }

  test("handles edda uri, 2 groups") {
    val uri = "http://edda/api/v2/group/autoScalingGroups;cluster=atlas_lwcapi-main;_expand"
    val res = run(uri, Success(mkResponse(eddaResponse2Groups)))
    assertEquals(res.uri, uri)
    assertEquals(res.instances.size, 3)
    assertEquals(res.instances.map(_.instanceId).toSet, Set("id1", "id2", "id3"))
    assertEquals("http://1.2.3.4:7101", res.instances(0).substitute("http://{local-ipv4}:{port}"))
    assertEquals("http://1.2.3.5:7101", res.instances(1).substitute("http://{local-ipv4}:{port}"))
    assertEquals("http://1.2.3.6:7101", res.instances(2).substitute("http://{local-ipv4}:{port}"))
  }

  test("handles edda uri, 1 empty 1 not") {
    val uri = "http://edda/api/v2/group/autoScalingGroups;cluster=atlas_lwcapi-main;_expand"
    val res = run(uri, Success(mkResponse(eddaResponseOneEmptyGroup)))
    assertEquals(res.uri, uri)
    assertEquals(res.instances.size, 2)
    assertEquals(res.instances.map(_.instanceId).toSet, Set("id1", "id2"))
    assertEquals("http://1.2.3.4:7101", res.instances(0).substitute("http://{local-ipv4}:{port}"))
    assertEquals("http://1.2.3.5:7101", res.instances(1).substitute("http://{local-ipv4}:{port}"))
  }

  test("substitute, IPv6 preferred if available") {
    val instance = EddaSource.Instance("i-1", Some("1.2.3.4"), Some("::1"))
    assertEquals(instance.substitute("http://{ip}:{port}"), "http://[::1]:7101")
  }
}

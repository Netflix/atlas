/*
 * Copyright 2014-2019 Netflix, Inc.
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

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers._
import akka.stream.ActorMaterializer
import akka.stream.ConnectionException
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.netflix.atlas.akka.AccessLogger
import com.netflix.atlas.core.util.Streams
import com.netflix.atlas.eval.stream.EurekaSource.GroupResponse
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class EurekaSourceSuite extends AnyFunSuite {

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
      Streams.scope(Streams.gzip(out)) { gz =>
        gz.write(data.getBytes(StandardCharsets.UTF_8))
      }
    }
  }

  private val innerAppJson =
    """
      |{
      |  "name": "test",
      |  "instance": [
      |    {
      |      "instanceId": "i-12345",
      |      "status": "UP",
      |      "port": {
      |        "$": 7001
      |      },
      |      "dataCenterInfo": {
      |        "name": "Amazon",
      |        "metadata": {
      |          "public-hostname": "ec2-12345",
      |          "local-ipv4": "1.2.3.4"
      |        }
      |      }
      |    }
      |  ]
      |}
    """.stripMargin

  private val appJson = s"""{"application": $innerAppJson}"""

  private val vipJson = s"""{"applications": {"application": [$innerAppJson]}}"""

  private implicit val system = ActorSystem(getClass.getSimpleName)
  private implicit val mat = ActorMaterializer()

  private def run(uri: String, response: Try[HttpResponse]): GroupResponse = {
    val client = Flow[(HttpRequest, AccessLogger)].map {
      case (_, logger) => response -> logger
    }
    val context = TestContext.createContext(mat, client)
    val future = EurekaSource(uri, context).runWith(Sink.head)
    Await.result(future, Duration.Inf)
  }

  test("handles vip uri") {
    val uri = "http://eureka/v1/vips/www-dev:7001"
    val res = run(uri, Success(mkResponse(vipJson)))
    assert(res.uri === uri)
    assert(res.instances.size === 1)
    assert(res.instances.map(_.instanceId).toSet === Set("i-12345"))
  }

  test("supports compressed response") {
    val uri = "http://eureka/v1/vips/www-dev:7001"
    val res = run(uri, Success(mkResponse(gzip(vipJson), true, StatusCodes.OK)))
    assert(res.uri === uri)
    assert(res.instances.size === 1)
    assert(res.instances.map(_.instanceId).toSet === Set("i-12345"))
  }

  test("handles app uri") {
    val uri = "http://eureka/v1/apps/www-dev:7001"
    val res = run(uri, Success(mkResponse(appJson)))
    assert(res.uri === uri)
    assert(res.instances.size === 1)
    assert(res.instances.map(_.instanceId).toSet === Set("i-12345"))
  }

  test("invalid json response") {
    val uri = "http://eureka/v1/vips/www-dev:7001"
    intercept[IllegalArgumentException] {
      run(uri, Success(mkResponse(appJson)))
    }
  }

  test("unknown vip") {
    val uri = "http://eureka/v1/vips/www-dev:7001"
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
    val uri = "http://eureka/v1/vips/www-dev:7001"
    intercept[NoSuchElementException] {
      run(uri, Success(response))
    }
    assert(consumedLatch.await(30, TimeUnit.SECONDS))
  }

  test("failure to connect") {
    val uri = "http://eureka/v1/vips/www-dev:7001"
    intercept[NoSuchElementException] {
      run(uri, Failure(new ConnectionException("timeout")))
    }
  }

  test("failed response stream") {
    val uri = "http://eureka/v1/vips/www-dev:7001"
    intercept[NoSuchElementException] {
      val source = Source.fromFuture(Future.failed[ByteString](new IOException("peer reset")))
      val entity = HttpEntity(MediaTypes.`application/json`, source)
      val response = HttpResponse(StatusCodes.OK, entity = entity)
      run(uri, Success(response))
    }
  }

  test("handles edda uri, 1 group") {
    val uri = "http://edda/api/v2/group/autoScalingGroups;cluster=atlas_lwcapi-main;_expand"
    val res = run(uri, Success(mkResponse(eddaResponseSingleGroup)))
    assert(res.uri === uri)
    assert(res.instances.size === 2)
    assert(res.instances.map(_.instanceId).toSet === Set("id1", "id2"))
    assert("http://1.2.3.4:7101" === res.instances(0).substitute("http://{local-ipv4}:{port}"))
    assert("http://1.2.3.5:7101" === res.instances(1).substitute("http://{local-ipv4}:{port}"))
  }

  test("handles edda uri, 2 groups") {
    val uri = "http://edda/api/v2/group/autoScalingGroups;cluster=atlas_lwcapi-main;_expand"
    val res = run(uri, Success(mkResponse(eddaResponse2Groups)))
    assert(res.uri === uri)
    assert(res.instances.size === 3)
    assert(res.instances.map(_.instanceId).toSet === Set("id1", "id2", "id3"))
    assert("http://1.2.3.4:7101" === res.instances(0).substitute("http://{local-ipv4}:{port}"))
    assert("http://1.2.3.5:7101" === res.instances(1).substitute("http://{local-ipv4}:{port}"))
    assert("http://1.2.3.6:7101" === res.instances(2).substitute("http://{local-ipv4}:{port}"))
  }

  test("handles edda uri, 1 empty 1 not") {
    val uri = "http://edda/api/v2/group/autoScalingGroups;cluster=atlas_lwcapi-main;_expand"
    val res = run(uri, Success(mkResponse(eddaResponseOneEmptyGroup)))
    assert(res.uri === uri)
    assert(res.instances.size === 2)
    assert(res.instances.map(_.instanceId).toSet === Set("id1", "id2"))
    assert("http://1.2.3.4:7101" === res.instances(0).substitute("http://{local-ipv4}:{port}"))
    assert("http://1.2.3.5:7101" === res.instances(1).substitute("http://{local-ipv4}:{port}"))
  }

  val eddaResponseSingleGroup =
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

  val eddaResponse2Groups =
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

  val eddaResponseOneEmptyGroup =
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
}

/*
 * Copyright 2014-2022 Netflix, Inc.
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
package com.netflix.atlas.akka

import akka.actor.ActorSystem

import java.io.ByteArrayOutputStream
import java.util.zip.GZIPOutputStream
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.testkit.RouteTestTimeout
import com.netflix.atlas.akka.testkit.MUnitRouteSuite
import com.netflix.atlas.json.Json
import com.netflix.iep.service.DefaultClassFactory
import com.netflix.spectator.api.NoopRegistry
import com.typesafe.config.ConfigFactory

import java.lang.reflect.Type

class RequestHandlerNoCompressionSuite extends MUnitRouteSuite {

  import scala.concurrent.duration._
  implicit val routeTestTimeout = RouteTestTimeout(5.second)

  private val config = ConfigFactory.parseString(
    """
      |atlas.akka.api-endpoints = [
      |  "com.netflix.atlas.akka.TestApi"
      |]
      |atlas.akka.cors-host-patterns = []
      |atlas.akka.diagnostic-headers = []
      |atlas.akka.request-handler {
      |  cors = false
      |  compression = false
      |  access-log = false
      |  close-probability = 0.0
      |}
    """.stripMargin
  )

  private val bindings: java.util.function.Function[Type, AnyRef] = {
    case c: Class[_] if c.isAssignableFrom(classOf[ActorSystem]) =>
      system
    case _ =>
      null.asInstanceOf[AnyRef]
  }

  private val handler =
    new RequestHandler(config, new NoopRegistry, new DefaultClassFactory(bindings))
  private val routes = handler.routes

  private def gzip(data: Array[Byte]): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val out = new GZIPOutputStream(baos)
    out.write(data)
    out.close()
    baos.toByteArray
  }

  private def gzip(s: String): Array[Byte] = gzip(s.getBytes("UTF-8"))

  private val gzipHeader = `Content-Encoding`(HttpEncodings.gzip)

  test("/jsonparse") {
    Post("/jsonparse", "\"foo\"") ~> routes ~> check {
      assertEquals(response.status, StatusCodes.OK)
      assertEquals(responseAs[String], "foo")
    }
  }

  test("/jsonparse with smile content") {
    val content = HttpEntity(
      CustomMediaTypes.`application/x-jackson-smile`.toContentType,
      Json.smileEncode("foo")
    )
    Post("/jsonparse", content) ~> routes ~> check {
      assertEquals(response.status, StatusCodes.OK)
      assertEquals(responseAs[String], "foo")
    }
  }

  test("/jsonparse with smile but wrong content-type") {
    val content = HttpEntity(Json.smileEncode("foo"))
    Post("/jsonparse", content) ~> routes ~> check {
      assertEquals(response.status, StatusCodes.BadRequest)
    }
  }

  test("/jsonparse with gzipped request") {
    val content = HttpEntity(gzip("\"foo\""))
    Post("/jsonparse", content).addHeader(gzipHeader) ~> routes ~> check {
      assertEquals(response.status, StatusCodes.OK)
      assertEquals(responseAs[String], "foo")
    }
  }

  test("/jsonparse with smile content and gzipped") {
    val content = HttpEntity(
      CustomMediaTypes.`application/x-jackson-smile`.toContentType,
      gzip(Json.smileEncode("foo"))
    )
    Post("/jsonparse", content).addHeader(gzipHeader) ~> routes ~> check {
      assertEquals(response.status, StatusCodes.OK)
      assertEquals(responseAs[String], "foo")
    }
  }

  test("cors preflight") {
    // With CORS disabled there shouldn't be a route to handle the OPTIONS request
    Options("/api/v2/ip") ~> routes ~> check {
      assertEquals(response.status, StatusCodes.NotFound)
    }
  }
}

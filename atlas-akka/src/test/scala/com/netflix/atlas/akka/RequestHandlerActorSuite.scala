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
package com.netflix.atlas.akka

import java.io.ByteArrayOutputStream
import java.util.zip.GZIPOutputStream

import akka.actor.ActorSystem
import akka.testkit.ImplicitSender
import akka.testkit.TestActorRef
import akka.testkit.TestKit
import com.netflix.atlas.json.Json
import com.netflix.iep.service.DefaultClassFactory
import com.netflix.spectator.api.DefaultRegistry
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuiteLike
import spray.http.HttpMethods._
import spray.http.StatusCodes._
import spray.http._


class RequestHandlerActorSuite extends TestKit(ActorSystem())
    with ImplicitSender
    with FunSuiteLike
    with BeforeAndAfterAll {

  private val config = ConfigFactory.parseString(
    """
      |atlas.akka.api-endpoints = [
      |  "com.netflix.atlas.akka.TestApi"
      |]
    """.stripMargin)

  private val ref = TestActorRef(new RequestHandlerActor(config, new DefaultClassFactory()))

  override def afterAll(): Unit = {
    system.terminate()
  }

  test("/not-found") {
    ref ! HttpRequest(GET, Uri("/not-found"))
    expectMsgPF() {
      case HttpResponse(NotFound, _, _, _) =>
    }
  }

  test("/ok") {
    ref ! HttpRequest(GET, Uri("/ok"))
    expectMsgPF() {
      case HttpResponse(OK, _, _, _) =>
    }
  }

  test("cors preflight") {
    ref ! HttpRequest(OPTIONS, Uri("/api/v2/ip"))
    expectMsgPF() {
      case HttpResponse(OK, _, _, _) =>
    }
  }

  private def gzip(data: Array[Byte]): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val out = new GZIPOutputStream(baos)
    out.write(data)
    out.close()
    baos.toByteArray
  }

  private def gzip(s: String): Array[Byte] = gzip(s.getBytes("UTF-8"))

  private val gzipHeaders = List(HttpHeaders.`Content-Encoding`(HttpEncodings.gzip))

  test("/jsonparse") {
    ref ! HttpRequest(POST, Uri("/jsonparse"), entity = "\"foo\"")
    expectMsgPF() {
      case HttpResponse(OK, entity, _, _) =>
        assert(entity.asString(HttpCharsets.`UTF-8`) === "foo")
    }
  }

  test("/jsonparse with smile content") {
    val content = HttpEntity(
      ContentType(CustomMediaTypes.`application/x-jackson-smile`),
      Json.smileEncode("foo"))
    ref ! HttpRequest(POST, Uri("/jsonparse"), entity = content)
    expectMsgPF() {
      case HttpResponse(OK, entity, _, _) =>
        assert(entity.asString(HttpCharsets.`UTF-8`) === "foo")
    }
  }

  test("/jsonparse with smile but wrong content-type") {
    val content = HttpEntity(Json.smileEncode("foo"))
    ref ! HttpRequest(POST, Uri("/jsonparse"), entity = content)
    expectMsgPF() {
      case HttpResponse(BadRequest, _, _, _) =>
    }
  }

  test("/jsonparse with gzipped request") {
    ref ! HttpRequest(POST, Uri("/jsonparse"), headers = gzipHeaders, entity = gzip("\"foo\""))
    expectMsgPF() {
      case HttpResponse(OK, entity, _, _) =>
        assert(entity.asString(HttpCharsets.`UTF-8`) === "foo")
    }
  }

  test("/jsonparse with smile content and gzipped") {
    val content = HttpEntity(
      ContentType(CustomMediaTypes.`application/x-jackson-smile`),
      gzip(Json.smileEncode("foo")))
    ref ! HttpRequest(POST, Uri("/jsonparse"), headers = gzipHeaders, entity = content)
    expectMsgPF() {
      case HttpResponse(OK, entity, _, _) =>
        assert(entity.asString(HttpCharsets.`UTF-8`) === "foo")
    }
  }

  test("/chunked with wrong method") {
    ref ! HttpRequest(POST, Uri("/chunked"))
    expectMsgPF() {
      case HttpResponse(MethodNotAllowed, _, _, _) =>
    }
  }
}

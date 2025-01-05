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

import org.apache.pekko.actor.ActorRefFactory
import org.apache.pekko.http.scaladsl.model.ContentTypes
import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.http.scaladsl.model.HttpMethods
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.MediaTypes
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.http.scaladsl.model.headers.*
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.testkit.RouteTestTimeout
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import com.netflix.atlas.json.Json
import com.netflix.atlas.pekko.testkit.MUnitRouteSuite
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.Spectator
import com.netflix.spectator.ipc.IpcMetric
import com.netflix.spectator.ipc.NetflixHeader

import scala.concurrent.duration.*

class CustomDirectivesSuite extends MUnitRouteSuite {

  import CustomDirectives.*
  import CustomDirectivesSuite.*

  // Some of the tests were a bit flakey with default of 1 second on slower machines
  private implicit val timeout: RouteTestTimeout = RouteTestTimeout(5.seconds)

  class TestService(val actorRefFactory: ActorRefFactory) {

    private val zone = RawHeader(NetflixHeader.Zone.headerName(), "us-east-1e")

    def routes: Route = {
      accessLog(List(zone)) {
        respondWithCorsHeaders(List("*")) {
          jsonpFilter {
            path("text") {
              get {
                val entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, "text response")
                complete(HttpResponse(status = StatusCodes.OK, entity = entity))
              }
            } ~
            path("json") {
              get {
                val entity = HttpEntity(MediaTypes.`application/json`, "[1,2,3]")
                complete(HttpResponse(status = StatusCodes.OK, entity = entity))
              } ~
              post {
                parseEntity(CustomDirectives.json[Message]) { message =>
                  val entity = HttpEntity(MediaTypes.`application/json`, Json.encode(message))
                  complete(HttpResponse(status = StatusCodes.OK, entity = entity))
                }
              } ~
              put {
                entity(CustomDirectives.jsonUnmarshaller[Message]) { message =>
                  val entity = HttpEntity(MediaTypes.`application/json`, Json.encode(message))
                  complete(HttpResponse(status = StatusCodes.OK, entity = entity))
                }
              }
            } ~
            path("json-parser") {
              post {
                parseEntity(customJson(p => Json.decode[Message](p))) { message =>
                  val entity = HttpEntity(MediaTypes.`application/json`, Json.encode(message))
                  complete(HttpResponse(status = StatusCodes.OK, entity = entity))
                }
              }
            } ~
            path("binary") {
              get {
                val data = ByteString("text response")
                val entity = HttpEntity.Strict(ContentTypes.`application/octet-stream`, data)
                complete(HttpResponse(status = StatusCodes.OK, entity = entity))
              }
            } ~
            path("error") {
              get {
                val entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, "error")
                complete(HttpResponse(status = StatusCodes.BadRequest, entity = entity))
              }
            } ~
            path("empty") {
              get {
                val headers = List(RawHeader("foo", "bar"))
                complete(HttpResponse(status = StatusCodes.OK, headers = headers))
              }
            } ~
            path("vary") {
              get {
                val headers = List(RawHeader("Vary", "Host"))
                complete(HttpResponse(status = StatusCodes.OK, headers = headers))
              }
            } ~
            path("error" / IntNumber) { code =>
              get {
                val status = StatusCodes.custom(code, "Error")
                complete(HttpResponse(status = status))
              }
            } ~
            endpointPath("endpoint") {
              get {
                complete(HttpResponse(status = StatusCodes.OK))
              }
            } ~
            endpointPath("endpoint", IntNumber) { code =>
              get {
                val status = StatusCodes.custom(code, "Error")
                complete(HttpResponse(status = status))
              }
            } ~
            endpointPathPrefix("endpoint" / "v1") {
              endpointPathPrefix("foo") {
                pathPrefix(IntNumber) { _ =>
                  endpointPath("bar", Remaining) { _ =>
                    get {
                      complete(HttpResponse(status = StatusCodes.OK))
                    }
                  }
                }
              }
            } ~
            closeConnection(0.0) {
              path("close" / "0.0") {
                get {
                  complete(HttpResponse(status = StatusCodes.OK))
                }
              }
            } ~
            closeConnection(0.5) {
              path("close" / "0.5") {
                get {
                  complete(HttpResponse(status = StatusCodes.OK))
                }
              }
            } ~
            closeConnection(1.0) {
              path("close" / "1.0") {
                get {
                  complete(HttpResponse(status = StatusCodes.OK))
                }
              }
            }
          }
        } ~
        corsPreflight(Nil)
      }
    }
  }

  val endpoint = new TestService(system)

  override def beforeEach(context: BeforeEach): Unit = {
    val registry = Spectator.globalRegistry()
    registry.removeAll()
    registry.add(new DefaultRegistry())
  }

  test("text") {
    Get("/text") ~> endpoint.routes ~> check {
      val expected = """text response"""
      assertEquals(expected, responseAs[String])
    }
  }

  test("json") {
    Get("/json") ~> endpoint.routes ~> check {
      val expected = """[1,2,3]"""
      assertEquals(expected, responseAs[String])
    }
  }

  test("json post") {
    val msg = Message("foo", "bar baz")
    Post("/json", Json.encode(msg)) ~> endpoint.routes ~> check {
      val expected = Json.decode[Message](responseAs[String])
      assertEquals(expected, msg)
    }
  }

  test("json put") {
    val msg = Message("foo", "bar baz")
    Put("/json", Json.encode(msg)) ~> endpoint.routes ~> check {
      val expected = Json.decode[Message](responseAs[String])
      assertEquals(expected, msg)
    }
  }

  test("smile post") {
    val msg = Message("foo", "bar baz")
    val entity = HttpEntity(CustomMediaTypes.`application/x-jackson-smile`, Json.smileEncode(msg))
    Post("/json", entity) ~> endpoint.routes ~> check {
      val expected = Json.decode[Message](responseAs[String])
      assertEquals(expected, msg)
    }
  }

  test("smile put") {
    val msg = Message("foo", "bar baz")
    val entity = HttpEntity(CustomMediaTypes.`application/x-jackson-smile`, Json.smileEncode(msg))
    Put("/json", entity) ~> endpoint.routes ~> check {
      val expected = Json.decode[Message](responseAs[String])
      assertEquals(expected, msg)
    }
  }

  test("json-parser post") {
    val msg = Message("foo", "bar baz")
    Post("/json-parser", Json.encode(msg)) ~> endpoint.routes ~> check {
      val expected = Json.decode[Message](responseAs[String])
      assertEquals(expected, msg)
    }
  }

  test("smile-parser post") {
    val msg = Message("foo", "bar baz")
    val entity = HttpEntity(CustomMediaTypes.`application/x-jackson-smile`, Json.smileEncode(msg))
    Post("/json-parser", entity) ~> endpoint.routes ~> check {
      val expected = Json.decode[Message](responseAs[String])
      assertEquals(expected, msg)
    }
  }

  test("binary") {
    Get("/binary") ~> endpoint.routes ~> check {
      val expected = """text response"""
      assertEquals(expected, responseAs[String])
    }
  }

  test("jsonp with text") {
    Get("/text?callback=foo") ~> endpoint.routes ~> check {
      val expected =
        """foo({"status":200,"headers":{"content-type":["text/plain"]},"body":"text response"})"""
      assertEquals(expected, responseAs[String])
    }
  }

  test("jsonp with json") {
    Get("/json?callback=foo") ~> endpoint.routes ~> check {
      val expected =
        """foo({"status":200,"headers":{"content-type":["application/json"]},"body":[1,2,3]})"""
      assertEquals(expected, responseAs[String])
    }
  }

  test("jsonp with binary") {
    Get("/binary?callback=foo") ~> endpoint.routes ~> check {
      val expected =
        """foo({"status":200,"headers":{"content-type":["application/octet-stream"]},"body":"dGV4dCByZXNwb25zZQ=="})"""
      assertEquals(expected, responseAs[String])
    }
  }

  test("jsonp with 400") {
    Get("/error?callback=foo") ~> endpoint.routes ~> check {
      val expected =
        """foo({"status":400,"headers":{"content-type":["text/plain"]},"body":"error"})"""
      assertEquals(response.status, StatusCodes.OK)
      assertEquals(expected, responseAs[String])
    }
  }

  test("jsonp with empty response body and custom header") {
    Get("/empty?callback=foo") ~> endpoint.routes ~> check {
      val expected =
        """foo({"status":200,"headers":{"foo":["bar"],"content-type":["text/plain"]},"body":""})"""
      assertEquals(response.status, StatusCodes.OK)
      assertEquals(expected, responseAs[String])
    }
  }

  test("cors") {
    val headers = List(Origin(HttpOrigin("http://localhost")))
    val req = HttpRequest(HttpMethods.GET, Uri("/json"), headers)
    req ~> endpoint.routes ~> check {
      assert(response.headers.nonEmpty)
      response.headers.foreach {
        case `Access-Control-Allow-Origin`(v) =>
          assertEquals("http://localhost", v.toString)
        case `Access-Control-Allow-Methods`(vs) =>
          assertEquals("GET,PATCH,POST,PUT,DELETE", vs.map(_.name()).mkString(","))
        case `Access-Control-Allow-Credentials`(v) =>
          assert(v)
        case h if h.is("netflix-zone") =>
          assertEquals(h.value, "us-east-1e")
        case h if h.is("vary") =>
          assertEquals(h.value, "Origin")
        case h =>
          fail(s"unexpected header: $h")
      }
      val expected = """[1,2,3]"""
      assertEquals(expected, responseAs[String])
    }
  }

  test("cors with custom header") {
    val headers = List(Origin(HttpOrigin("http://localhost")))
    val req = HttpRequest(HttpMethods.GET, Uri("/empty"), headers)
    req ~> endpoint.routes ~> check {
      assert(headers.nonEmpty)
      response.headers.foreach {
        case `Access-Control-Allow-Origin`(v) =>
          assertEquals("http://localhost", v.toString)
        case `Access-Control-Allow-Methods`(vs) =>
          assertEquals("GET,PATCH,POST,PUT,DELETE", vs.map(_.name()).mkString(","))
        case `Access-Control-Expose-Headers`(vs) =>
          assertEquals("foo", vs.mkString(","))
        case `Access-Control-Allow-Credentials`(v) =>
          assert(v)
        case h if h.is("netflix-zone") =>
          assertEquals(h.value, "us-east-1e")
        case h if h.is("vary") =>
          assertEquals(h.value, "Origin")
        case h if h.lowercaseName == "foo" =>
          assertEquals("bar", h.value)
        case h =>
          fail(s"unexpected header: $h (${h.getClass})")
      }
      val expected = ""
      assertEquals(expected, responseAs[String])
    }
  }

  test("cors with custom request headers") {
    val headers =
      List(Origin(HttpOrigin("http://localhost")), `Access-Control-Request-Headers`("foo"))
    val req = HttpRequest(HttpMethods.GET, Uri("/json"), headers)
    req ~> endpoint.routes ~> check {
      assert(headers.nonEmpty)
      response.headers.foreach {
        case `Access-Control-Allow-Origin`(v) =>
          assertEquals("http://localhost", v.toString)
        case `Access-Control-Allow-Methods`(vs) =>
          assertEquals("GET,PATCH,POST,PUT,DELETE", vs.map(_.name()).mkString(","))
        case `Access-Control-Allow-Headers`(vs) =>
          assertEquals("foo", vs.mkString(","))
        case `Access-Control-Allow-Credentials`(v) =>
          assert(v)
        case h if h.is("netflix-zone") =>
          assertEquals(h.value, "us-east-1e")
        case h if h.is("vary") =>
          assertEquals(h.value, "Origin")
        case h =>
          fail(s"unexpected header: $h")
      }
      val expected = """[1,2,3]"""
      assertEquals(expected, responseAs[String])
    }
  }

  // Some browsers send this when a request is made from a file off the local filesystem
  test("cors null origin") {
    val headers = List(RawHeader("Origin", "null"))
    val req = HttpRequest(HttpMethods.GET, Uri("/json"), headers)
    req ~> endpoint.routes ~> check {
      assert(headers.nonEmpty)
      response.headers.foreach {
        case `Access-Control-Allow-Origin`(v) =>
          assertEquals("*", v.toString)
        case `Access-Control-Allow-Methods`(vs) =>
          assertEquals("GET,PATCH,POST,PUT,DELETE", vs.map(_.name()).mkString(","))
        case `Access-Control-Allow-Credentials`(v) =>
          assert(v)
        case h if h.is("netflix-zone") =>
          assertEquals(h.value, "us-east-1e")
        case h if h.is("vary") =>
          assertEquals(h.value, "Origin")
        case h =>
          fail(s"unexpected header: $h")
      }
      val expected = """[1,2,3]"""
      assertEquals(expected, responseAs[String])
    }
  }

  test("cors with additional vary headers") {
    val headers = List(Origin(HttpOrigin("http://localhost")))
    val req = HttpRequest(HttpMethods.GET, Uri("/vary"), headers)
    req ~> endpoint.routes ~> check {
      assert(headers.nonEmpty)
      response.headers.foreach {
        case `Access-Control-Allow-Origin`(v) =>
          assertEquals("http://localhost", v.toString)
        case `Access-Control-Allow-Methods`(vs) =>
          assertEquals("GET,PATCH,POST,PUT,DELETE", vs.map(_.name()).mkString(","))
        case `Access-Control-Expose-Headers`(vs) =>
          assertEquals("Vary", vs.mkString(","))
        case `Access-Control-Allow-Credentials`(v) =>
          assert(v)
        case h if h.is("netflix-zone") =>
          assertEquals(h.value, "us-east-1e")
        case h if h.is("vary") =>
          assert(Set("Origin", "Host").contains(h.value))
        case h =>
          fail(s"unexpected header: $h (${h.getClass})")
      }
      val expected = ""
      assertEquals(expected, responseAs[String])
    }
  }

  test("cors for 404") {
    val headers = List(Origin(HttpOrigin("http://localhost")))
    val req = HttpRequest(HttpMethods.GET, Uri("/error/404"), headers)
    req ~> endpoint.routes ~> check {
      assert(response.headers.nonEmpty)
      response.headers.foreach {
        case `Access-Control-Allow-Origin`(v) =>
          assertEquals("http://localhost", v.toString)
        case `Access-Control-Allow-Methods`(vs) =>
          assertEquals("GET,PATCH,POST,PUT,DELETE", vs.map(_.name()).mkString(","))
        case `Access-Control-Allow-Credentials`(v) =>
          assert(v)
        case h if h.is("netflix-zone") =>
          assertEquals(h.value, "us-east-1e")
        case h if h.is("vary") =>
          assertEquals(h.value, "Origin")
        case h =>
          fail(s"unexpected header: $h")
      }
      assertEquals("", responseAs[String])
    }
  }

  test("cors for 400") {
    val headers = List(Origin(HttpOrigin("http://localhost")))
    val req = HttpRequest(HttpMethods.GET, Uri("/error/400"), headers)
    req ~> endpoint.routes ~> check {
      assert(response.headers.nonEmpty)
      response.headers.foreach {
        case `Access-Control-Allow-Origin`(v) =>
          assertEquals("http://localhost", v.toString)
        case `Access-Control-Allow-Methods`(vs) =>
          assertEquals("GET,PATCH,POST,PUT,DELETE", vs.map(_.name()).mkString(","))
        case `Access-Control-Allow-Credentials`(v) =>
          assert(v)
        case h if h.is("netflix-zone") =>
          assertEquals(h.value, "us-east-1e")
        case h if h.is("vary") =>
          assertEquals(h.value, "Origin")
        case h =>
          fail(s"unexpected header: $h")
      }
      assertEquals("", responseAs[String])
    }
  }

  test("cors preflight") {
    Options("/json") ~> endpoint.routes ~> check {
      assertEquals(response.status, StatusCodes.OK)
    }
  }

  test("cors preflight with body") {
    val request = HttpRequest(
      method = HttpMethods.OPTIONS,
      entity = HttpEntity.Chunked(
        MediaTypes.`application/octet-stream`,
        Source.single(HttpEntity.LastChunk(""))
      )
    )
    request ~> endpoint.routes ~> check {
      assertEquals(response.status, StatusCodes.OK)
    }
  }

  test("valid ipc metrics are produced") {
    Get("/text") ~> endpoint.routes ~> check {
      IpcMetric.validate(Spectator.globalRegistry())
    }
  }

  def getEndpoint(response: HttpResponse): String = {
    response.headers.find(_.is("netflix-endpoint")).get.value()
  }

  test("endpoint header, nothing unmatched") {
    Get("/endpoint") ~> endpoint.routes ~> check {
      assertEquals(getEndpoint(response), "/endpoint")
    }
  }

  test("endpoint header, int part") {
    Get("/endpoint/404") ~> endpoint.routes ~> check {
      assertEquals(getEndpoint(response), "/endpoint")
    }
  }

  test("endpoint header, nested paths are appended") {
    Get("/endpoint/v1/foo/404/bar/i-1234567890") ~> endpoint.routes ~> check {
      // Ideally it wouldn't match the 404 value that was extracted, but since this sort
      // of nesting is not common for our use-cases, that is left for later refinement
      assertEquals(getEndpoint(response), "/endpoint/v1/foo/404/bar")
    }
  }

  test("diagnostic headers are added to response") {
    Get("/text") ~> endpoint.routes ~> check {
      val zone = response.headers.find(_.is("netflix-zone"))
      assertEquals(zone, Some(RawHeader("Netflix-Zone", "us-east-1e")))
    }
  }

  test("close: 0.0, none closed") {
    (0 until 100).foreach { _ =>
      Get("/close/0.0") ~> endpoint.routes ~> check {
        assert(!response.headers.exists(_.is("connection")))
      }
    }
  }

  test("close: 0.5, roughly half closed") {
    var closed = 0
    (0 until 100).foreach { _ =>
      Get("/close/0.5") ~> endpoint.routes ~> check {
        if (response.headers.exists(_.is("connection"))) {
          closed += 1
        }
      }
    }
    // there can be some random variation, this range should be wide enough to
    // avoid spurious test failures
    assert(closed >= 25 && closed <= 75)
  }

  test("close: 1.0, all closed") {
    (0 until 100).foreach { _ =>
      Get("/close/1.0") ~> endpoint.routes ~> check {
        assert(response.headers.exists(_.is("connection")))
      }
    }
  }
}

object CustomDirectivesSuite {

  case class Message(subject: String, body: String)
}

/*
 * Copyright 2014-2026 Netflix, Inc.
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
import com.netflix.atlas.json3.Json
import com.netflix.atlas.pekko.testkit.MUnitRouteSuite
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.Spectator
import com.netflix.spectator.api.Utils
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
        } ~
        corsPreflight(Nil)
      }
    }
  }

  val endpoint = new TestService(system)

  // Routes wrapped with the full standard request handling stack (access log plus the
  // real exception and rejection handlers). This is used to check what endpoint value
  // gets recorded when the inner route of an `endpointPath*` directive throws an
  // exception or is rejected. In those cases the response is produced by the exception
  // or rejection handler, which is wrapped *outside* of the `endpointPath*` directive.
  // The recorded-endpoint mechanism still applies the `Netflix-Endpoint` header to those
  // error responses so the endpoint is captured on the access log.
  private val standardRoutes: Route = {
    val inner =
      endpointPath("endpoint-ok") {
        get {
          complete(HttpResponse(status = StatusCodes.OK))
        }
      } ~
      endpointPath("endpoint-throw") {
        get {
          throw new IllegalArgumentException("boom")
        }
      } ~
      endpointPathPrefix("endpoint-reject") {
        path("data") {
          // Only supports GET so that a POST after the prefix has been matched will
          // result in a method rejection.
          get {
            complete(HttpResponse(status = StatusCodes.OK))
          }
        }
      } ~
      endpointPath("endpoint-large") {
        post {
          // Small size limit so a modest payload triggers an EntityStreamSizeException
          // when the entity is consumed (after the endpoint path has been matched).
          withSizeLimit(10) {
            parseEntity(CustomDirectives.json[String]) { _ =>
              complete(HttpResponse(status = StatusCodes.OK))
            }
          }
        }
      } ~
      // A shared prefix where an `endpointPath*` branch matches the prefix (recording its
      // endpoint) but then rejects, and a sibling non-endpoint route completes the
      // request. The sibling response must not inherit the rejected branch's endpoint.
      endpointPathPrefix("shared") {
        path("a") {
          get {
            complete(HttpResponse(status = StatusCodes.OK))
          }
        }
      } ~
      path("shared" / "b") {
        get {
          complete(HttpResponse(status = StatusCodes.OK))
        }
      } ~
      // Like the "shared" prefix above, but the sibling that completes the request is
      // itself an `endpointPath*` route. Attribution should follow the path actually
      // taken (the sibling), not the rejected prefix.
      endpointPathPrefix("picked") {
        path("a") {
          get {
            complete(HttpResponse(status = StatusCodes.OK))
          }
        }
      } ~
      endpointPath("picked" / "b") {
        get {
          complete(HttpResponse(status = StatusCodes.OK))
        }
      }
    RequestHandler.standardOptions(inner)
  }

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

  private def endpointTag: Option[String] = {
    import scala.jdk.CollectionConverters.*
    Spectator
      .globalRegistry()
      .iterator()
      .asScala
      .map(_.id())
      .filter(_.name() == "ipc.server.call")
      .flatMap(id => Option(Utils.getTagValue(id, "ipc.endpoint")))
      .nextOption()
  }

  test("endpoint header/tag set when inner route completes") {
    Get("/endpoint-ok") ~> standardRoutes ~> check {
      assertEquals(response.status, StatusCodes.OK)
      assertEquals(getEndpoint(response), "/endpoint-ok")
      // The access log endpoint tag matches the matched path
      assertEquals(endpointTag, Some("/endpoint-ok"))
    }
  }

  test("endpoint header/tag preserved when inner route throws") {
    Get("/endpoint-throw") ~> standardRoutes ~> check {
      assertEquals(response.status, StatusCodes.BadRequest)
      // Even though the response is produced by the exception handler wrapped outside
      // of the endpointPath directive, the endpoint matched during path matching is
      // still applied to the response and recorded on the access log.
      assertEquals(getEndpoint(response), "/endpoint-throw")
      assertEquals(endpointTag, Some("/endpoint-throw"))
    }
  }

  test("endpoint header/tag preserved when inner route is rejected") {
    Post("/endpoint-reject/data") ~> standardRoutes ~> check {
      assertEquals(response.status, StatusCodes.MethodNotAllowed)
      // Even though the response is produced by the rejection handler wrapped outside
      // of the endpointPathPrefix directive, the matched endpoint prefix is still
      // applied to the response and recorded on the access log.
      assertEquals(getEndpoint(response), "/endpoint-reject")
      assertEquals(endpointTag, Some("/endpoint-reject"))
    }
  }

  test("endpoint not leaked to a sibling route after prefix match and rejection") {
    // GET /shared/b matches the "shared" endpoint prefix (recording it) but its inner
    // route rejects on the sub-path, so routing backtracks to the sibling
    // "shared" / "b" route which completes successfully. That successful response must
    // not be attributed to the rejected prefix's endpoint.
    Get("/shared/b") ~> standardRoutes ~> check {
      assertEquals(response.status, StatusCodes.OK)
      assert(response.headers.find(_.is("netflix-endpoint")).isEmpty)
      assertNotEquals(endpointTag, Some("/shared"))
    }
  }

  test("endpoint attributed to the path taken when a sibling endpointPath completes") {
    // GET /picked/b matches the "picked" endpoint prefix (recording it) but its inner
    // route rejects; routing backtracks to the sibling endpointPath("picked" / "b")
    // which completes. The endpoint must reflect the path that actually handled the
    // request, not the rejected prefix.
    Get("/picked/b") ~> standardRoutes ~> check {
      assertEquals(response.status, StatusCodes.OK)
      assertEquals(getEndpoint(response), "/picked/b")
      assertEquals(endpointTag, Some("/picked/b"))
    }
  }

  test("endpoint header/tag preserved when payload is too large") {
    val body = "\"" + "a".repeat(100) + "\""
    Post("/endpoint-large", body) ~> standardRoutes ~> check {
      assertEquals(response.status, StatusCodes.ContentTooLarge)
      // The size limit is exceeded while consuming the entity, which happens after the
      // endpoint path has been matched, so the endpoint is still applied to the error
      // response and recorded on the access log.
      assertEquals(getEndpoint(response), "/endpoint-large")
      assertEquals(endpointTag, Some("/endpoint-large"))
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

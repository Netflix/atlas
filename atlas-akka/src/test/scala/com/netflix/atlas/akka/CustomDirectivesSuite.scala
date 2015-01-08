/*
 * Copyright 2015 Netflix, Inc.
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

import akka.actor.ActorRefFactory
import org.scalatest.FunSuite
import spray.http._
import spray.routing._
import spray.testkit.ScalatestRouteTest


class CustomDirectivesSuite extends FunSuite with ScalatestRouteTest {

  class TestService(val actorRefFactory: ActorRefFactory) extends HttpService {

    def routes: RequestContext => Unit = {
      CustomDirectives.corsFilter {
        CustomDirectives.jsonpFilter {
          path("text") {
            get { ctx =>
              val entity = HttpEntity(MediaTypes.`text/plain`, "text response")
              ctx.responder ! HttpResponse(status = StatusCodes.OK, entity = entity)
            }
          } ~
          path("json") {
            get { ctx =>
              val entity = HttpEntity(MediaTypes.`application/json`, "[1,2,3]")
              ctx.responder ! HttpResponse(status = StatusCodes.OK, entity = entity)
            }
          } ~
          path("binary") {
            get { ctx =>
              val entity = HttpEntity(MediaTypes.`application/octet-stream`, "text response")
              ctx.responder ! HttpResponse(status = StatusCodes.OK, entity = entity)
            }
          } ~
          path("error") {
            get { ctx =>
              val entity = HttpEntity(MediaTypes.`text/plain`, "error")
              ctx.responder ! HttpResponse(status = StatusCodes.BadRequest, entity = entity)
            }
          } ~
          path("empty") {
            get { ctx =>
              val headers = List(HttpHeaders.RawHeader("foo", "bar"))
              ctx.responder ! HttpResponse(status = StatusCodes.OK, headers = headers)
            }
          }
        }
      }
    }
  }

  val endpoint = new TestService(system)

  test("text") {
    Get("/text") ~> endpoint.routes ~> check {
      val expected = """text response"""
      assert(expected === responseAs[String])
    }
  }

  test("json") {
    Get("/json") ~> endpoint.routes ~> check {
      val expected = """[1,2,3]"""
      assert(expected === responseAs[String])
    }
  }

  test("binary") {
    Get("/binary") ~> endpoint.routes ~> check {
      val expected = """text response"""
      assert(expected === responseAs[String])
    }
  }

  test("jsonp with text") {
    Get("/text?callback=foo") ~> endpoint.routes ~> check {
      val expected = """foo({"status":200,"headers":{"content-type":["text/plain"]},"body":"text response"})"""
      assert(expected === responseAs[String])
    }
  }

  test("jsonp with json") {
    Get("/json?callback=foo") ~> endpoint.routes ~> check {
      val expected = """foo({"status":200,"headers":{"content-type":["application/json"]},"body":[1,2,3]})"""
      assert(expected === responseAs[String])
    }
  }

  test("jsonp with binary") {
    Get("/binary?callback=foo") ~> endpoint.routes ~> check {
      val expected = """foo({"status":200,"headers":{"content-type":["application/octet-stream"]},"body":"dGV4dCByZXNwb25zZQ=="})"""
      assert(expected === responseAs[String])
    }
  }

  test("jsonp with 400") {
    Get("/error?callback=foo") ~> endpoint.routes ~> check {
      val expected = """foo({"status":400,"headers":{"content-type":["text/plain"]},"body":"error"})"""
      assert(StatusCodes.OK === response.status)
      assert(expected === responseAs[String])
    }
  }

  test("jsonp with empty response body and custom header") {
    Get("/empty?callback=foo") ~> endpoint.routes ~> check {
      val expected = """foo({"status":200,"headers":{"foo":["bar"],"content-type":["text/plain"]},"body":""})"""
      assert(StatusCodes.OK === response.status)
      assert(expected === responseAs[String])
    }
  }

  test("cors") {
    val headers = List(HttpHeaders.Origin(List(HttpOrigin("http://localhost"))))
    val req = HttpRequest(HttpMethods.GET, Uri("/json"), headers)
    req ~> endpoint.routes ~> check {
      assert(headers.nonEmpty)
      response.headers.foreach {
        case HttpHeaders.`Access-Control-Allow-Origin`(v) =>
          assert("http://localhost" === v.toString)
        case HttpHeaders.`Access-Control-Allow-Methods`(v) =>
          assert("GET,PATCH,POST,PUT,DELETE" === v.mkString(","))
        case h =>
          fail(s"unexpected header: $h")
      }
      val expected = """[1,2,3]"""
      assert(expected === responseAs[String])
    }
  }

  test("cors with custom header") {
    val headers = List(HttpHeaders.Origin(List(HttpOrigin("http://localhost"))))
    val req = HttpRequest(HttpMethods.GET, Uri("/empty"), headers)
    req ~> endpoint.routes ~> check {
      assert(headers.nonEmpty)
      response.headers.foreach {
        case HttpHeaders.`Access-Control-Allow-Origin`(v) =>
          assert("http://localhost" === v.toString)
        case HttpHeaders.`Access-Control-Allow-Methods`(v) =>
          assert("GET,PATCH,POST,PUT,DELETE" === v.mkString(","))
        case HttpHeaders.`Access-Control-Expose-Headers`(v) =>
          assert("foo" === v.mkString(","))
        case HttpHeaders.RawHeader("foo", v) =>
          assert("bar" === v)
        case h =>
          fail(s"unexpected header: $h")
      }
      val expected = ""
      assert(expected === responseAs[String])
    }
  }

  test("cors with custom request headers") {
    val headers = List(
      HttpHeaders.Origin(List(HttpOrigin("http://localhost"))),
      HttpHeaders.`Access-Control-Request-Headers`("foo"))
    val req = HttpRequest(HttpMethods.GET, Uri("/json"), headers)
    req ~> endpoint.routes ~> check {
      assert(headers.nonEmpty)
      response.headers.foreach {
        case HttpHeaders.`Access-Control-Allow-Origin`(v) =>
          assert("http://localhost" === v.toString)
        case HttpHeaders.`Access-Control-Allow-Methods`(v) =>
          assert("GET,PATCH,POST,PUT,DELETE" === v.mkString(","))
        case HttpHeaders.`Access-Control-Allow-Headers`(v) =>
          assert("foo" === v.mkString(","))
        case h =>
          fail(s"unexpected header: $h")
      }
      val expected = """[1,2,3]"""
      assert(expected === responseAs[String])
    }
  }

  // Some browsers send this when a request is made from a file off the local filesystem
  test("cors null origin") {
    val headers = List(HttpHeaders.RawHeader("Origin", "null"))
    val req = HttpRequest(HttpMethods.GET, Uri("/json"), headers)
    req ~> endpoint.routes ~> check {
      assert(headers.nonEmpty)
      response.headers.foreach {
        case HttpHeaders.`Access-Control-Allow-Origin`(v) =>
          assert("*" === v.toString)
        case HttpHeaders.`Access-Control-Allow-Methods`(v) =>
          assert("GET,PATCH,POST,PUT,DELETE" === v.mkString(","))
        case h =>
          fail(s"unexpected header: $h")
      }
      val expected = """[1,2,3]"""
      assert(expected === responseAs[String])
    }
  }
}

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

import com.netflix.atlas.pekko.testkit.MUnitRouteSuite
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.Spectator
import com.netflix.spectator.api.Utils
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route

class RequestAuthenticatorSuite extends MUnitRouteSuite {

  import CustomDirectives.*
  import RequestAuthenticatorSuite.*

  override def beforeEach(context: BeforeEach): Unit = {
    val registry = Spectator.globalRegistry()
    registry.removeAll()
    registry.add(new DefaultRegistry())
  }

  private def clientAppTag: Option[String] = {
    import scala.jdk.CollectionConverters.*
    Spectator
      .globalRegistry()
      .iterator()
      .asScala
      .map(_.id())
      .filter(_.name() == "ipc.server.call")
      .flatMap(id => Option(Utils.getTagValue(id, "ipc.client.app")))
      .nextOption()
  }

  private def serverCallRecorded: Boolean = {
    import scala.jdk.CollectionConverters.*
    Spectator
      .globalRegistry()
      .iterator()
      .asScala
      .map(_.id())
      .exists(_.name() == "ipc.server.call")
  }

  private def configWith(authenticator: Option[String], accessLog: Boolean = false): Config = {
    val auth = authenticator.fold("")(c => s"""authenticator = "$c"""")
    ConfigFactory.parseString(s"""
        |atlas.pekko.api-endpoints = []
        |atlas.pekko.cors-host-patterns = []
        |atlas.pekko.diagnostic-headers = []
        |atlas.pekko.request-handler {
        |  cors = false
        |  compression = false
        |  access-log = $accessLog
        |  close-probability = 0.0
        |  $auth
        |}
      """.stripMargin)
  }

  test("passthrough when no authenticator configured") {
    val authenticator = RequestAuthenticator(configWith(None))
    assertEquals(authenticator, RequestAuthenticator.passthrough)
  }

  test("load authenticator with config constructor") {
    val cls = classOf[ConfigCtorAuthenticator].getName
    val authenticator = RequestAuthenticator(configWith(Some(cls)))
    assert(authenticator.isInstanceOf[ConfigCtorAuthenticator])
  }

  test("load authenticator with empty constructor") {
    val cls = classOf[EmptyCtorAuthenticator].getName
    val authenticator = RequestAuthenticator(configWith(Some(cls)))
    assert(authenticator.isInstanceOf[EmptyCtorAuthenticator])
  }

  test("extractCaller: anonymous when nothing set") {
    val route = extractCaller { caller =>
      complete(caller.direct.id)
    }
    Get("/") ~> route ~> check {
      assertEquals(responseAs[String], "unknown")
    }
  }

  test("extractCaller: reads context set on the request") {
    val route = mapRequest(_.addAttribute(CallerContext.attr, testCaller)) {
      extractCaller { caller =>
        complete(
          s"${caller.direct.id},${caller.original.id},${caller.forwardedToken.getOrElse("")}"
        )
      }
    }
    Get("/") ~> route ~> check {
      assertEquals(responseAs[String], "test_app,test_user,raw-token")
    }
  }

  test("standardOptions applies the configured authenticator") {
    val cls = classOf[FixedCallerAuthenticator].getName
    val settings = RequestHandler.Settings(configWith(Some(cls)))
    val inner = extractCaller { caller =>
      complete(caller.original.id)
    }
    val route = RequestHandler.standardOptions(inner, settings)
    Get("/") ~> route ~> check {
      assertEquals(response.status, StatusCodes.OK)
      assertEquals(responseAs[String], "test_user")
    }
  }

  test("access log: app caller recorded as client app") {
    val cls = classOf[FixedCallerAuthenticator].getName
    val settings = RequestHandler.Settings(configWith(Some(cls), accessLog = true))
    // Use a non-"/ok" path so the request goes through the authenticator (the health-check
    // `ok` route is intentionally left outside the authenticator).
    val route = RequestHandler.standardOptions(complete("ok"), settings)
    Get("/") ~> route ~> check {
      assertEquals(response.status, StatusCodes.OK)
      assertEquals(clientAppTag, Some("test_app"))
    }
  }

  test("access log: user caller not added to metrics") {
    val cls = classOf[UserCallerAuthenticator].getName
    val settings = RequestHandler.Settings(configWith(Some(cls), accessLog = true))
    val route = RequestHandler.standardOptions(complete("ok"), settings)
    Get("/") ~> route ~> check {
      assertEquals(response.status, StatusCodes.OK)
      assertEquals(clientAppTag, None)
    }
  }

  test("rejecting authenticator: rejection rendered by standard handler") {
    val cls = classOf[RejectingAuthenticator].getName
    val settings = RequestHandler.Settings(configWith(Some(cls)))
    val route = RequestHandler.standardOptions(complete("secret"), settings)
    Get("/") ~> route ~> check {
      assertEquals(response.status, StatusCodes.Forbidden)
      assert(responseAs[String].contains("denied"))
    }
  }

  test("rejecting authenticator: health-check ok route is exempt") {
    val cls = classOf[RejectingAuthenticator].getName
    val settings = RequestHandler.Settings(configWith(Some(cls)))
    val route = RequestHandler.standardOptions(complete("secret"), settings)
    Get("/ok") ~> route ~> check {
      assertEquals(response.status, StatusCodes.OK)
    }
  }

  test("withCaller: direct and initial callers added as log-only tags") {
    val entry = AccessLogger.ipcLogger.createServerEntry
    val caller = CallerContext(
      Principal(Principal.Kind.App, "app1"),
      Principal(Principal.Kind.User, "user1"),
      None
    )
    AccessLogger.newServerLogger(entry).withCaller(caller)
    val json = entry.toString
    // Both callers appear in the log-only additionalLogTags; the origin (a user) is not a
    // metric field, so its presence proves the log tag was written.
    assert(json.contains("caller.origin"), json)
    assert(json.contains("user1"), json)
    // App direct caller is also recorded as the bounded client-app metric dimension.
    assert(json.contains("clientApp"), json)
    assert(json.contains("app1"), json)
  }

  test("withCaller: no origin tag when initial caller matches direct") {
    val entry = AccessLogger.ipcLogger.createServerEntry
    val user = Principal(Principal.Kind.User, "user1")
    AccessLogger.newServerLogger(entry).withCaller(CallerContext(user, user, None))
    val json = entry.toString
    assert(json.contains("user1"), json)
    assert(!json.contains("caller.origin"), json)
  }

  test("rejecting authenticator: rejected request is still access-logged") {
    val cls = classOf[RejectingAuthenticator].getName
    val settings = RequestHandler.Settings(configWith(Some(cls), accessLog = true))
    val route = RequestHandler.standardOptions(complete("secret"), settings)
    Get("/") ~> route ~> check {
      assertEquals(response.status, StatusCodes.Forbidden)
      assert(serverCallRecorded)
    }
  }
}

object RequestAuthenticatorSuite {

  private val testCaller: CallerContext = CallerContext(
    Principal(Principal.Kind.App, "test_app"),
    Principal(Principal.Kind.User, "test_user"),
    Some("raw-token")
  )

  class ConfigCtorAuthenticator(config: Config) extends RequestAuthenticator {

    // Reference the config so the parameter is not flagged as unused
    require(config != null)
    override def apply(route: Route): Route = route
  }

  class EmptyCtorAuthenticator extends RequestAuthenticator {
    override def apply(route: Route): Route = route
  }

  class FixedCallerAuthenticator extends RequestAuthenticator {

    override def apply(route: Route): Route = {
      CustomDirectives.recordCaller(testCaller) { route }
    }
  }

  class UserCallerAuthenticator extends RequestAuthenticator {

    private val caller = CallerContext(
      Principal(Principal.Kind.User, "test_user"),
      Principal(Principal.Kind.User, "test_user"),
      None
    )

    override def apply(route: Route): Route = {
      CustomDirectives.recordCaller(caller) { route }
    }
  }

  class RejectingAuthenticator extends RequestAuthenticator {

    override def apply(route: Route): Route = {
      reject(CustomRejection(StatusCodes.Forbidden, "denied"))
    }
  }
}

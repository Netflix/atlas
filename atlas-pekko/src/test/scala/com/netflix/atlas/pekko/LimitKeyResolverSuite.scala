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

import munit.FunSuite
import org.apache.pekko.http.scaladsl.model.HttpRequest

class LimitKeyResolverSuite extends FunSuite {

  // "vip" is provisioned in both namespaces so the tests below distinguish a legacy caller being
  // kept out of an authenticated caller's budget from it merely naming something unconfigured.
  private val resolver =
    new DefaultLimitKeyResolver(_ => Set("vip", "legacy.vip", "legacy.old-app"))

  private def app(id: String): CallerContext = {
    CallerContext(Principal(Principal.Kind.App, id), Principal.Anonymous, None)
  }

  private def user(id: String): CallerContext = {
    CallerContext(Principal(Principal.Kind.User, id), Principal.Anonymous, None)
  }

  test("provisioned caller gets its own bucket") {
    val key = resolver.resolve(app("vip"), "graph", HttpRequest())
    assertEquals(key, LimitKey("vip", "vip", "graph"))
  }

  test("authenticated but unprovisioned caller shares the default bucket") {
    val key = resolver.resolve(app("some-app"), "graph", HttpRequest())
    assertEquals(key, LimitKey(LimitKey.DefaultBucket, "some-app", "graph"))
  }

  test("user identity is used as the sub-key") {
    val key = resolver.resolve(user("user@example.com"), "graph", HttpRequest())
    assertEquals(key, LimitKey(LimitKey.DefaultBucket, "user@example.com", "graph"))
  }

  test("legacy id can select a provisioned bucket of its own") {
    val request = HttpRequest(uri = "/api/v1/graph?id=old-app")
    val key = resolver.resolve(CallerContext.Anonymous, "graph", request)
    assertEquals(key, LimitKey("legacy.old-app", "legacy.old-app", "graph"))
  }

  test("legacy id cannot name an authenticated caller") {
    // "vip" has a dedicated budget under both names, but the parameter is asserted rather than
    // authenticated, so it is namespaced and lands in the legacy budget rather than in the one the
    // authenticated caller "vip" would get.
    val request = HttpRequest(uri = "/api/v1/graph?id=vip")
    val key = resolver.resolve(CallerContext.Anonymous, "graph", request)
    assertEquals(key, LimitKey("legacy.vip", "legacy.vip", "graph"))
    assertNotEquals(key, resolver.resolve(app("vip"), "graph", HttpRequest()))
  }

  test("an authenticated caller cannot reach a legacy budget by naming itself into it") {
    // Nothing constrains what an authenticator produces, so the namespace has to be closed from
    // both sides: an identity that already carries the prefix keeps its own sub-key but must not
    // match the legacy caller's dedicated budget.
    val key = resolver.resolve(app("legacy.old-app"), "graph", HttpRequest())
    assertEquals(key, LimitKey(LimitKey.DefaultBucket, "legacy.old-app", "graph"))
  }

  test("an empty legacy id is not an identity") {
    // `?id=` parses to a present but empty value, which must not be read as the bare prefix. The
    // bare prefix is provisioned here so the value is what decides the outcome rather than the
    // lookup happening to miss.
    val bare = new DefaultLimitKeyResolver(_ => Set(LimitKey.LegacyPrefix))
    val request = HttpRequest(uri = "/api/v1/graph?id=")
    val key = bare.resolve(CallerContext.Anonymous, "graph", request)
    assertEquals(key, LimitKey(LimitKey.DefaultBucket, LimitKey.Anonymous, "graph"))
  }

  test("a legacy caller and an authenticated caller of the same name stay separate") {
    val legacy = resolver.resolve(
      CallerContext.Anonymous,
      "graph",
      HttpRequest(uri = "/api/v1/graph?id=old-app")
    )
    val authenticated = resolver.resolve(app("old-app"), "graph", HttpRequest())
    assertNotEquals(legacy.subKey, authenticated.subKey)
    assertNotEquals(legacy.bucket, authenticated.bucket)
  }

  test("unprovisioned legacy id is not trusted as an identity") {
    // The parameter is supplied by an unauthenticated caller, so honoring an arbitrary value would
    // let one caller claim a share of a fair-share bucket for every value it invents.
    val request = HttpRequest(uri = "/api/v1/graph?id=legacy-app&q=name,sps,:eq")
    val key = resolver.resolve(CallerContext.Anonymous, "graph", request)
    assertEquals(key, LimitKey(LimitKey.DefaultBucket, LimitKey.Anonymous, "graph"))
  }

  test("rotating the legacy id yields the same sub-key every time") {
    val keys = (1 to 100).map { i =>
      val request = HttpRequest(uri = s"/api/v1/graph?id=random-$i")
      resolver.resolve(CallerContext.Anonymous, "graph", request)
    }
    assertEquals(
      keys.distinct.toList,
      List(LimitKey(LimitKey.DefaultBucket, LimitKey.Anonymous, "graph"))
    )
  }

  test("anonymous caller with no legacy id uses the anonymous marker") {
    val key = resolver.resolve(CallerContext.Anonymous, "graph", HttpRequest(uri = "/api/v1/graph"))
    assertEquals(key, LimitKey(LimitKey.DefaultBucket, LimitKey.Anonymous, "graph"))
  }

  test("provisioning is scoped per endpoint") {
    val perEndpoint =
      new DefaultLimitKeyResolver(ep => if (ep == "graph") Set("vip") else Set.empty)
    assertEquals(
      perEndpoint.resolve(app("vip"), "graph", HttpRequest()),
      LimitKey("vip", "vip", "graph")
    )
    assertEquals(
      perEndpoint.resolve(app("vip"), "tags", HttpRequest()),
      LimitKey(LimitKey.DefaultBucket, "vip", "tags")
    )
  }
}

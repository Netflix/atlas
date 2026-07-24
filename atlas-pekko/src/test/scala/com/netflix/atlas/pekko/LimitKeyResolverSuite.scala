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

  private val resolver = new DefaultLimitKeyResolver(_ => Set("vip"))

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

  test("anonymous caller falls back to the legacy id query parameter") {
    val request = HttpRequest(uri = "/api/v1/graph?id=legacy-app&q=name,sps,:eq")
    val key = resolver.resolve(CallerContext.Anonymous, "graph", request)
    assertEquals(key, LimitKey(LimitKey.DefaultBucket, "legacy-app", "graph"))
  }

  test("legacy id can select a provisioned bucket") {
    val request = HttpRequest(uri = "/api/v1/graph?id=vip")
    val key = resolver.resolve(CallerContext.Anonymous, "graph", request)
    assertEquals(key, LimitKey("vip", "vip", "graph"))
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

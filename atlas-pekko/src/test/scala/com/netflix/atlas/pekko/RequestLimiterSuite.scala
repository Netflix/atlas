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

import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import munit.FunSuite

class RequestLimiterSuite extends FunSuite {

  private def config(s: String): Config = ConfigFactory.parseString(s)

  private def graphKey(subKey: String, bucket: String = LimitKey.DefaultBucket): LimitKey = {
    LimitKey(bucket, subKey, "graph")
  }

  private def requests(
    registry: Registry,
    endpoint: String,
    result: String,
    reason: String
  ): Long = {
    registry
      .counter(
        "atlas.limiter.requests",
        "endpoint",
        endpoint,
        "result",
        result,
        "reason",
        reason
      )
      .count()
  }

  private val graphConfig =
    """
      |mode = enforce
      |endpoints {
      |  graph {
      |    total-budget = 100
      |    default-bucket-budget = 2
      |    caller-budgets {
      |      vip     = 4
      |      blocked = 0
      |    }
      |  }
      |}
      |""".stripMargin

  private def limiterMeterCount(registry: Registry): Int = {
    import scala.jdk.CollectionConverters.*
    registry.iterator().asScala.count(_.id().name().startsWith("atlas.limiter."))
  }

  test("off mode is a zero-cost bypass that permits everything and registers no meters") {
    val registry = new DefaultRegistry()
    val cfg = graphConfig.replace("mode = enforce", "mode = off")
    val limiter = new RequestLimiter(config(cfg), registry)
    val tokens = (1 to 100).flatMap(i => limiter.acquire(graphKey(s"u$i"), 10))
    assertEquals(tokens.size, 100)
    tokens.foreach(t => assertEquals(t, LimitToken.NoOp))
    // Disabled limiting must not build limiters, gauges, or counters.
    assertEquals(limiterMeterCount(registry), 0)
  }

  test("unlisted endpoint is not limited and records nothing even in enforce mode") {
    val registry = new DefaultRegistry()
    val limiter = new RequestLimiter(config(graphConfig), registry)
    val key = LimitKey(LimitKey.DefaultBucket, "u1", "tags")
    val tokens = (1 to 50).flatMap(_ => limiter.acquire(key, 1000))
    assertEquals(tokens.size, 50)
    assertEquals(requests(registry, "tags", "permitted", "none"), 0L)
  }

  test("shared default bucket is exhausted across distinct callers") {
    val registry = new DefaultRegistry()
    val limiter = new RequestLimiter(config(graphConfig), registry)
    assert(limiter.acquire(graphKey("u1"), 1).isDefined)
    assert(limiter.acquire(graphKey("u2"), 1).isDefined)
    // Budget of 2 for the default bucket is now full.
    assert(limiter.acquire(graphKey("u3"), 1).isEmpty)
    assertEquals(requests(registry, "graph", "denied", "bucket"), 1L)
  }

  test("dedicated bucket has its own budget separate from the default bucket") {
    val registry = new DefaultRegistry()
    val limiter = new RequestLimiter(config(graphConfig), registry)
    // Fill the default bucket.
    assert(limiter.acquire(graphKey("u1"), 2).isDefined)
    assert(limiter.acquire(graphKey("u2"), 1).isEmpty)
    // The vip caller has its own dedicated bucket and is unaffected.
    val vip = graphKey("vip", "vip")
    assert(limiter.acquire(vip, 4).isDefined)
    assert(limiter.acquire(vip, 1).isEmpty)
  }

  test("a bucket with a budget of zero blocks the caller") {
    val registry = new DefaultRegistry()
    val limiter = new RequestLimiter(config(graphConfig), registry)
    val blocked = graphKey("blocked", "blocked")
    assert(limiter.acquire(blocked, 1).isEmpty)
    assertEquals(requests(registry, "graph", "denied", "bucket"), 1L)
  }

  test("endpoint total budget caps combined cost across buckets") {
    val cfg =
      """
        |mode = enforce
        |endpoints {
        |  graph {
        |    total-budget = 3
        |    default-bucket-budget = 10
        |    caller-budgets { vip = 2 }
        |  }
        |}
        |""".stripMargin
    val registry = new DefaultRegistry()
    val limiter = new RequestLimiter(config(cfg), registry)

    val d = limiter.acquire(graphKey("u1"), 2)
    assert(d.isDefined)
    // vip bucket would allow this, but the endpoint total (3) cannot fit another 2.
    assert(limiter.acquire(graphKey("vip", "vip"), 2).isEmpty)
    assertEquals(requests(registry, "graph", "denied", "total"), 1L)

    // The vip bucket permits reserved before the total check must have been released: after
    // freeing the total budget, a full vip request succeeds.
    d.get.release()
    assert(limiter.acquire(graphKey("vip", "vip"), 2).isDefined)
  }

  test("shadow mode reports denials but still admits the request") {
    val cfg = graphConfig.replace("mode = enforce", "mode = shadow")
    val registry = new DefaultRegistry()
    val limiter = new RequestLimiter(config(cfg), registry)
    assert(limiter.acquire(graphKey("u1"), 1).isDefined)
    assert(limiter.acquire(graphKey("u2"), 1).isDefined)
    // Would be denied in enforce mode, but shadow admits with a no-op token.
    val shed = limiter.acquire(graphKey("u3"), 1)
    assertEquals(shed, Some(LimitToken.NoOp))
    assertEquals(requests(registry, "graph", "denied", "bucket"), 1L)
  }

  test("releasing a token frees the reserved capacity") {
    val registry = new DefaultRegistry()
    val limiter = new RequestLimiter(config(graphConfig), registry)
    val t = limiter.acquire(graphKey("u1"), 2).get
    assert(limiter.acquire(graphKey("u2"), 1).isEmpty)
    t.release()
    assert(limiter.acquire(graphKey("u2"), 2).isDefined)
  }

  test("token release is idempotent") {
    val registry = new DefaultRegistry()
    val limiter = new RequestLimiter(config(graphConfig), registry)
    val a = limiter.acquire(graphKey("u1"), 1).get
    val b = limiter.acquire(graphKey("u2"), 1).get
    a.release()
    a.release()
    // Only b should still be holding a permit; a double release must not free b's slot.
    assert(limiter.acquire(graphKey("u3"), 2).isEmpty)
    b.release()
    assert(limiter.acquire(graphKey("u3"), 2).isDefined)
  }

  test("dedicatedBuckets lists provisioned callers for an endpoint") {
    val registry = new DefaultRegistry()
    val limiter = new RequestLimiter(config(graphConfig), registry)
    assertEquals(limiter.dedicatedBuckets("graph"), Set("vip", "blocked"))
    assertEquals(limiter.dedicatedBuckets("tags"), Set.empty[String])
  }

  test("an unprovisioned bucket name collapses onto the shared default bucket") {
    val registry = new DefaultRegistry()
    val limiter = new RequestLimiter(config(graphConfig), registry)
    // A bucket that is not provisioned must not mint its own limiter; it shares the default
    // bucket (budget 2), so it competes for the same budget as other unprovisioned callers.
    assert(limiter.acquire(graphKey("u1"), 1).isDefined)
    assert(limiter.acquire(LimitKey("adhoc", "u2", "graph"), 1).isDefined)
    assert(limiter.acquire(graphKey("u3"), 1).isEmpty)
  }

  test("a caller budget named default does not override the shared default bucket") {
    val cfg =
      """
        |mode = enforce
        |endpoints {
        |  graph {
        |    default-bucket-budget = 2
        |    caller-budgets { default = 100 }
        |  }
        |}
        |""".stripMargin
    val registry = new DefaultRegistry()
    val limiter = new RequestLimiter(config(cfg), registry)
    // The reserved default bucket keeps default-bucket-budget (2), not the caller budget (100).
    assert(limiter.acquire(graphKey("u1"), 1).isDefined)
    assert(limiter.acquire(graphKey("u2"), 1).isDefined)
    assert(limiter.acquire(graphKey("u3"), 1).isEmpty)
  }

  test("caller budget keys containing a dot are read literally") {
    val cfg =
      """
        |mode = enforce
        |endpoints {
        |  graph {
        |    default-bucket-budget = 1
        |    caller-budgets { "app.with.dots" = 3 }
        |  }
        |}
        |""".stripMargin
    val registry = new DefaultRegistry()
    val limiter = new RequestLimiter(config(cfg), registry)
    assertEquals(limiter.dedicatedBuckets("graph"), Set("app.with.dots"))
    val key = LimitKey("app.with.dots", "app.with.dots", "graph")
    assert(limiter.acquire(key, 3).isDefined)
    assert(limiter.acquire(key, 1).isEmpty)
  }

  test("unknown mode is rejected") {
    intercept[IllegalArgumentException] {
      new RequestLimiter(config("mode = bogus\nendpoints {}"), new DefaultRegistry())
    }
  }

  test("fair-share default bucket contains a hog while a victim can still acquire") {
    val cfg =
      """
        |mode = enforce
        |endpoints {
        |  graph {
        |    fair-share = true
        |    default-bucket-budget = 4
        |  }
        |}
        |""".stripMargin
    val registry = new DefaultRegistry()
    val limiter = new RequestLimiter(config(cfg), registry)

    // Hog fills the shared bucket, then keeps hammering until it is penalized.
    val held = (1 to 4).flatMap(_ => limiter.acquire(graphKey("hog"), 1))
    assertEquals(held.size, 4)
    assert(limiter.acquire(graphKey("victim"), 1).isEmpty) // bucket full, marks victim as waiting
    (1 to 3).foreach(_ => assert(limiter.acquire(graphKey("hog"), 1).isEmpty))

    // A permit frees: the penalized hog cannot reclaim it, but the victim can.
    held.head.release()
    assert(limiter.acquire(graphKey("hog"), 1).isEmpty)
    assert(limiter.acquire(graphKey("victim"), 1).isDefined)
  }

  test("fair-share is off by default so the default bucket is a plain shared limiter") {
    val cfg =
      """
        |mode = enforce
        |endpoints {
        |  graph { default-bucket-budget = 4 }
        |}
        |""".stripMargin
    val registry = new DefaultRegistry()
    val limiter = new RequestLimiter(config(cfg), registry)
    // Without fair sharing a single caller can take the whole bucket and is not penalized.
    val held = (1 to 4).flatMap(_ => limiter.acquire(graphKey("hog"), 1))
    assertEquals(held.size, 4)
    held.head.release()
    // The freed permit is available first-come, including back to the same caller.
    assert(limiter.acquire(graphKey("hog"), 1).isDefined)
  }
}

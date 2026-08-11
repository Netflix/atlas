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
import com.netflix.spectator.api.ManualClock
import com.typesafe.config.ConfigFactory
import munit.FunSuite
import org.apache.pekko.http.scaladsl.model.HttpRequest

import java.time.Duration

/**
  * A caller that can choose its own `subKey` would otherwise be granted one share of a fair-share
  * bucket per identity it invents, and one entry of tracked state to scan on every later request.
  * These cover the two defences against that: the resolver refuses to take an identity from an
  * unauthenticated caller, and the limiter caps how many sub-keys it will track no matter what a
  * resolver hands it.
  */
class FairShareLimiterSybilSuite extends FunSuite {

  private val budget = 100

  private val limitsConfig = ConfigFactory.parseString(
    s"""
       |mode = "enforce"
       |fair-share {
       |  window = 5s
       |  penalized-threshold = 3.0
       |  demerit-per-denial = 1.0
       |  decay-per-second = 0.3
       |  max-penalty-duration = 60s
       |  max-tracked-callers = 1000
       |}
       |endpoints {
       |  graph {
       |    default-bucket-budget = $budget
       |    fair-share = true
       |  }
       |}
       |""".stripMargin
  )

  // The limiter takes its clock from the registry, so a manual one is the only way to keep the
  // contention window from expiring mid-test. Under the wall clock a slow run would age the
  // anonymous caller out of the window, leaving the honest caller alone with the whole budget --
  // and the assertions below would then pass without the fairness policy doing anything.
  private def newLimiter(): (RequestLimiter, LimitKeyResolver) = {
    val limiter = new RequestLimiter(limitsConfig, new DefaultRegistry(new ManualClock()))
    limiter -> new DefaultLimitKeyResolver(limiter.dedicatedBuckets)
  }

  private def newFairShareLimiter(maxTracked: Int): FairShareLimiter = {
    new FairShareLimiter(
      budget,
      new ManualClock(),
      Duration.ofSeconds(5),
      penalizedThreshold = 3.0,
      demeritPerDenial = 1.0,
      decayPerSecond = 0.3,
      maxPenaltyDuration = Duration.ofSeconds(60),
      maxTrackedCallers = maxTracked
    )
  }

  private def request(id: String): HttpRequest = {
    HttpRequest(uri = s"/api/v1/graph?q=name,sps,:eq&id=$id")
  }

  test("a flood of rotating ids cannot starve an honest caller") {
    val (limiter, resolver) = newLimiter()

    // The attacker sends a fresh id on every request, the way `?id=$RANDOM$RANDOM` would, and fills
    // the bucket. A lone caller is allowed the whole budget, so this much is expected.
    val held = (1 to budget).flatMap { i =>
      val key = resolver.resolve(CallerContext.Anonymous, "graph", request(s"attacker-$i"))
      limiter.acquire(key, 1)
    }
    assertEquals(held.size, budget)

    // The honest caller is authenticated, so it gets a sub-key of its own. It is denied while the
    // bucket is full, which marks it as wanting capacity.
    val honest = CallerContext(Principal(Principal.Kind.App, "honest"), Principal.Anonymous, None)
    val honestKey = resolver.resolve(honest, "graph", HttpRequest(uri = "/api/v1/graph"))
    assert(limiter.acquire(honestKey, 1).isEmpty)

    // The attacker keeps hammering under fresh ids. Every one of them lands on the same anonymous
    // sub-key, so the denials accumulate against one caller and it is penalized as a hog, rather
    // than each request arriving as an unpenalized identity with a share of its own.
    (1 to 500).foreach { i =>
      val key = resolver.resolve(CallerContext.Anonymous, "graph", request(s"attacker-more-$i"))
      assert(limiter.acquire(key, 1).isEmpty)
    }

    // As permits free up they go to the honest caller, not back to the flood.
    held.take(50).foreach(_.release())
    val honestHeld = (1 to 50).flatMap(_ => limiter.acquire(honestKey, 1))
    assertEquals(honestHeld.size, 50)

    held.drop(50).foreach(_.release())
    honestHeld.foreach(_.release())
    assertEquals(limiter.acquire(honestKey, budget).map(_.release()).isDefined, true)
  }

  test("tracked sub-keys stay bounded however many distinct ids arrive") {
    val limiter = newFairShareLimiter(maxTracked = 1000)

    // Straight at the limiter, bypassing the resolver, as a resolver that trusts an unbounded value
    // would. Each request arrives under a fresh sub-key and completes immediately; releasing leaves
    // the state in place and it stays active for the whole window, so nothing is pruned.
    (1 to 50000).foreach { i =>
      val key = s"attacker-$i"
      assert(limiter.tryAcquire(key, 1))
      limiter.release(key, 1)
    }

    assertEquals(limiter.trackedCallers, 1000)
  }

  test("the tracked set holds at the cap rather than drifting past it") {
    val maxTracked = 100
    val limiter = newFairShareLimiter(maxTracked)

    // `tryAcquire` does make a pass over the tracked callers, so the tracked set is what bounds the
    // work a flood can cause per request as well as the memory it can retain. Check the bound holds
    // throughout a long run of churn, not just at the end of it.
    (1 to 5000).foreach { i =>
      val key = s"attacker-$i"
      assert(limiter.tryAcquire(key, 1))
      limiter.release(key, 1)
      if (i % 500 == 0) {
        assert(
          limiter.trackedCallers <= maxTracked,
          s"tracked ${limiter.trackedCallers} callers after $i requests"
        )
      }
    }
    assertEquals(limiter.trackedCallers, maxTracked)
  }

  test("a denied request cannot grow the tracked set past the cap") {
    val limiter = newFairShareLimiter(maxTracked = 8)

    // State is still created before the budget check, so that a denial can be recorded against the
    // caller. That is only safe because the cap applies to it too: an attacker needs no successful
    // admission, but it also gains nothing from being refused.
    assert(limiter.tryAcquire("a", budget))
    (1 to 1000).foreach(i => assert(!limiter.tryAcquire(s"denied-$i", 1)))
    assertEquals(limiter.trackedCallers, 8)
  }
}

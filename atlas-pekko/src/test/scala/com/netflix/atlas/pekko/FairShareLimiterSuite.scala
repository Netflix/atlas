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

import com.netflix.spectator.api.ManualClock
import munit.FunSuite

import java.time.Duration

class FairShareLimiterSuite extends FunSuite {

  private val window = Duration.ofSeconds(5)

  private def newLimiter(budget: Int, clock: ManualClock): FairShareLimiter = {
    new FairShareLimiter(
      budget,
      clock,
      window,
      penalizedThreshold = 3.0,
      demeritPerDenial = 1.0,
      decayPerSecond = 0.3
    )
  }

  // Advance the clock by the given number of seconds.
  private def advance(clock: ManualClock, seconds: Long): Unit = {
    clock.setMonotonicTime(clock.monotonicTime() + seconds * 1_000_000_000L)
  }

  test("acquire and release within budget") {
    val limiter = newLimiter(4, new ManualClock())
    assert(limiter.tryAcquire("a", 2))
    assertEquals(limiter.usedPermits, 2)
    limiter.release("a", 2)
    assertEquals(limiter.usedPermits, 0)
  }

  test("a lone caller may use the whole budget") {
    val limiter = newLimiter(4, new ManualClock())
    assert(limiter.tryAcquire("a", 4))
    assertEquals(limiter.usedPermits, 4)
    assert(!limiter.tryAcquire("a", 1))
  }

  test("a well-behaved caller may borrow above its share when nobody else is waiting") {
    val limiter = newLimiter(4, new ManualClock())
    // b has attempted, so it counts toward the active set (share becomes 2), but it has not been
    // denied, so a may still borrow the spare capacity.
    assert(limiter.tryAcquire("b", 1))
    assert(limiter.tryAcquire("a", 3))
    assertEquals(limiter.usedPermits, 4)
  }

  test("a hog is contained to a floor and a victim can still acquire") {
    val clock = new ManualClock()
    val limiter = newLimiter(4, clock)

    // The hog grabs the whole budget while alone.
    (1 to 4).foreach(_ => assert(limiter.tryAcquire("hog", 1)))
    assertEquals(limiter.usedPermits, 4)

    // The victim is denied because the bucket is full, which marks it as wanting more.
    assert(!limiter.tryAcquire("victim", 1))

    // The hog keeps hammering and, being denied without backing off, accrues demerit until it is
    // penalized (threshold 3.0 with +1 per denial).
    (1 to 3).foreach(_ => assert(!limiter.tryAcquire("hog", 1)))

    // A permit frees up. The penalized hog cannot reclaim it (held to its reduced share)...
    limiter.release("hog", 1)
    assert(!limiter.tryAcquire("hog", 1))
    // ...but the victim, which is under its share, can.
    assert(limiter.tryAcquire("victim", 1))
  }

  test("a hog recovers once its demerit decays") {
    val clock = new ManualClock()
    val limiter = newLimiter(4, clock)

    // Drive the hog over the penalty threshold while the victim contends for capacity.
    (1 to 4).foreach(_ => assert(limiter.tryAcquire("hog", 1)))
    assert(!limiter.tryAcquire("victim", 1))
    (1 to 3).foreach(_ => assert(!limiter.tryAcquire("hog", 1)))
    limiter.release("hog", 1)
    assert(!limiter.tryAcquire("hog", 1))

    // Free the bucket and let enough time pass for the demerit to decay below the threshold. The
    // former hog, no longer penalized and with nobody waiting, can use the capacity again.
    limiter.release("hog", 3)
    advance(clock, 30)
    assert(limiter.tryAcquire("hog", 4))
    assertEquals(limiter.usedPermits, 4)
  }

  test("cost is clamped to the budget") {
    val limiter = newLimiter(4, new ManualClock())
    assert(limiter.tryAcquire("a", 100))
    assertEquals(limiter.usedPermits, 4)
    limiter.release("a", 100)
    assertEquals(limiter.usedPermits, 0)
  }

  test("bookkeeping is pruned once callers age out of the window") {
    val clock = new ManualClock()
    val limiter = newLimiter(4, clock)
    assert(limiter.tryAcquire("a", 1))
    limiter.release("a", 1)
    // After the window passes with no activity from "a", a new caller sees the full budget as its
    // share (active count of one), confirming "a" is no longer counted.
    advance(clock, 10)
    assert(limiter.tryAcquire("b", 4))
    assertEquals(limiter.usedPermits, 4)
  }

  test("borrowing is blocked while another well-behaved caller is waiting") {
    val clock = new ManualClock()
    val limiter = newLimiter(4, clock)

    // a holds its equal share (2 of 4 for two active callers).
    assert(limiter.tryAcquire("a", 2))
    // b attempts and is denied, marking it as wanting more capacity.
    assert(!limiter.tryAcquire("b", 3))
    // a may not borrow above its share while b is waiting.
    assert(!limiter.tryAcquire("a", 1))

    // Once b ages out of the window it no longer counts as waiting, and a (now the only active
    // caller) may use the rest of the budget.
    advance(clock, 10)
    assert(limiter.tryAcquire("a", 2))
    assertEquals(limiter.usedPermits, 4)
  }

  test("concurrent acquire and release keep budget accounting consistent") {
    val budget = 8
    val limiter = newLimiter(budget, new ManualClock())
    val threads = 8
    val iterations = 5000
    val violations = new java.util.concurrent.atomic.AtomicInteger(0)

    val workers = (0 until threads).map { t =>
      val thread = new Thread(() => {
        val key = s"c$t"
        var i = 0
        while (i < iterations) {
          val cost = 1 + (i % 2)
          if (limiter.tryAcquire(key, cost)) {
            val u = limiter.usedPermits
            if (u < 0 || u > budget) violations.incrementAndGet()
            limiter.release(key, cost)
          }
          i += 1
        }
      })
      thread.start()
      thread
    }
    workers.foreach(_.join())

    // The budget invariant must hold at every observation, and with every acquire balanced by a
    // release the accounting must return to exactly zero.
    assertEquals(violations.get(), 0)
    assertEquals(limiter.usedPermits, 0)
  }

  test("budget must be positive") {
    intercept[IllegalArgumentException] {
      newLimiter(0, new ManualClock())
    }
  }
}

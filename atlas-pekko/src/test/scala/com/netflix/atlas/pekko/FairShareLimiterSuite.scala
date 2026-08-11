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

  private val penaltyDuration = Duration.ofSeconds(60)

  private def newLimiter(
    budget: Int,
    clock: ManualClock,
    maxTrackedCallers: Int = 1000
  ): FairShareLimiter = {
    new FairShareLimiter(
      budget,
      clock,
      window,
      penalizedThreshold = 3.0,
      demeritPerDenial = 1.0,
      decayPerSecond = 0.3,
      maxPenaltyDuration = penaltyDuration,
      maxTrackedCallers = maxTrackedCallers
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

  test("a hog is still held to its floor while penalized, however large the budget") {
    val clock = new ManualClock()
    val limiter = newLimiter(1000, clock)

    // Demerit is clamped only at `penalizedThreshold + budget`, which is still enough for
    // `share - demerit` to reach the floor however large the share is. With two callers the share
    // is 500, so a ceiling below that would leave the hog almost unconstrained.
    assert(limiter.tryAcquire("hog", 1000))
    assert(!limiter.tryAcquire("victim", 1))
    (1 to 600).foreach(_ => assert(!limiter.tryAcquire("hog", 1)))
    limiter.release("hog", 1000)

    var admitted = 0
    while (admitted < 1000 && limiter.tryAcquire("hog", 1)) admitted += 1
    assertEquals(admitted, 1)
  }

  // Build a limiter with one knob overridden, for the validation and edge-case tests below.
  private def tunedLimiter(
    clock: ManualClock = new ManualClock(),
    budget: Int = 100,
    penalizedThreshold: Double = 3.0,
    demeritPerDenial: Double = 1.0,
    decayPerSecond: Double = 0.3,
    maxPenaltyDuration: Duration = penaltyDuration
  ): FairShareLimiter = {
    new FairShareLimiter(
      budget,
      clock,
      window,
      penalizedThreshold,
      demeritPerDenial,
      decayPerSecond,
      maxPenaltyDuration,
      maxTrackedCallers = 1000
    )
  }

  test("a huge demerit still floors the hog rather than wrapping into a licence") {
    val clock = new ManualClock()
    // One denial would otherwise push the demerit past what an `Int` can hold, and narrowing that
    // without clamping would wrap it negative and turn `share - demerit` into a cap far above the
    // budget. Both the ceiling on the accumulator and the clamp on the narrowing prevent it.
    val limiter = tunedLimiter(clock, demeritPerDenial = 3.0e9)

    assert(limiter.tryAcquire("hog", 100))
    assert(!limiter.tryAcquire("victim", 1))
    assert(!limiter.tryAcquire("hog", 1))
    limiter.release("hog", 100)

    var admitted = 0
    while (admitted < 100 && limiter.tryAcquire("hog", 1)) admitted += 1
    assertEquals(admitted, 1)
  }

  test("max tracked callers must be positive") {
    intercept[IllegalArgumentException] {
      newLimiter(4, new ManualClock(), maxTrackedCallers = 0)
    }
  }

  test("decay must be positive") {
    // Zero decay makes every demerit permanent, which also makes the state carrying it unprunable,
    // so the tracked set would wedge at the cap and never let another caller in.
    intercept[IllegalArgumentException] {
      tunedLimiter(new ManualClock(), demeritPerDenial = 1.0, decayPerSecond = 0.0)
    }
  }

  test("window must be positive") {
    intercept[IllegalArgumentException] {
      new FairShareLimiter(
        100,
        new ManualClock(),
        Duration.ZERO,
        3.0,
        1.0,
        0.3,
        penaltyDuration,
        1000
      )
    }
  }

  test("demerit is bounded so a burst cannot buy an unbounded penalty") {
    val clock = new ManualClock()
    val limiter = newLimiter(100, clock)

    // A hog that hammers for a long time accrues demerit at one per denial, but the ceiling is
    // `penalizedThreshold + budget`, so recovery stays proportional to the budget instead of to how
    // long the caller kept at it. Uncapped, these 10k denials would hold it down for over nine
    // hours; the ceiling of 103 clears in under six minutes.
    assert(limiter.tryAcquire("victim", 100))
    (1 to 10000).foreach(_ => assert(!limiter.tryAcquire("hog", 1)))
    limiter.release("victim", 100)

    advance(clock, 6 * 60)
    assert(limiter.tryAcquire("hog", 100))
    assertEquals(limiter.usedPermits, 100)
  }

  test("a caller arriving after the tracked set goes stale is tracked, not folded in") {
    val clock = new ManualClock()
    val limiter = newLimiter(100, clock, maxTrackedCallers = 1)

    // Poison the shared entry, then let the tracked set go stale. The pruning pass has to run
    // before the tracked-or-shared decision, or the first caller to arrive is handed the crowd's
    // demerit and floored at one permit while the slot it needed sits free.
    assert(limiter.tryAcquire("filler", 100))
    (1 to 500).foreach(i => assert(!limiter.tryAcquire(s"flood-$i", 1)))
    limiter.release("filler", 100)
    advance(clock, 30)

    assert(limiter.tryAcquire("honest", 100))
    assertEquals(limiter.trackedCallers, 1)
  }

  test("permits are fully returned when many callers churn past the cap") {
    // Every acquire is paired with exactly one release of the same cost, so once the run drains,
    // `usedPermits` must be zero. Far more callers than slots, so callers cross between the tracked
    // set and the shared entry constantly - which is where an accounting slip strands permits and
    // shrinks the effective budget for the life of the process.
    val rng = new scala.util.Random(20260810)
    val clock = new ManualClock()
    val limiter = newLimiter(1000, clock, maxTrackedCallers = 6)
    val held = Array.fill(40)(List.empty[Int])

    (1 to 20000).foreach { _ =>
      clock.setMonotonicTime(clock.monotonicTime() + rng.nextInt(400000000).toLong)
      val i = rng.nextInt(held.length)
      val key = s"caller-$i"
      if (held(i).nonEmpty && rng.nextInt(100) < 50) {
        limiter.release(key, held(i).head)
        held(i) = held(i).tail
      } else {
        val cost = 1 + rng.nextInt(20)
        if (limiter.tryAcquire(key, cost)) held(i) = cost :: held(i)
      }
    }

    held.indices.foreach(i => held(i).foreach(c => limiter.release(s"caller-$i", c)))
    assertEquals(limiter.usedPermits, 0)
  }

  test("a penalty does not outlast the maximum penalty duration") {
    val clock = new ManualClock()
    val limiter = newLimiter(100, clock)

    // Drive the demerit as far above the threshold as it will go. Decaying that at 0.3 per second
    // would keep this caller penalized for minutes.
    assert(limiter.tryAcquire("hog", 100))
    (1 to 500).foreach(_ => assert(!limiter.tryAcquire("hog", 1)))
    limiter.release("hog", 100)

    // Still penalized part way through the horizon, so it is held to its floor. Note this attempt
    // is itself denied, which restarts the horizon: it runs from the last denial, so a caller that
    // keeps hammering stays penalized for as long as it keeps it up.
    advance(clock, 30)
    assert(limiter.tryAcquire("victim", 1))
    assert(!limiter.tryAcquire("hog", 50))

    // Once it stops being denied for the horizon the penalty is gone, whatever the demerit reached.
    advance(clock, 61)
    assert(limiter.tryAcquire("hog", 50))
  }

  test("the horizon runs from the last denial, not the first") {
    val clock = new ManualClock()
    val limiter = newLimiter(100, clock)

    // Enough denials that the horizon, rather than the decay, is what would end the penalty.
    assert(limiter.tryAcquire("hog", 100))
    (1 to 500).foreach(_ => assert(!limiter.tryAcquire("hog", 1)))
    limiter.release("hog", 100)

    // Denied again most of the way through the horizon, which restarts it, so the caller is still
    // penalized at a point well past the horizon measured from where it began.
    advance(clock, 59)
    assert(limiter.tryAcquire("victim", 1))
    assert(!limiter.tryAcquire("hog", 50))
    advance(clock, 30)
    assert(!limiter.tryAcquire("hog", 50))
  }

  test("max penalty duration must be positive") {
    intercept[IllegalArgumentException] {
      tunedLimiter(maxPenaltyDuration = Duration.ofSeconds(0))
    }
    intercept[IllegalArgumentException] {
      tunedLimiter(maxPenaltyDuration = Duration.ofSeconds(-1))
    }
  }

  test("reaching the cap evicts the stalest idle caller, not the busiest") {
    val clock = new ManualClock()
    val limiter = newLimiter(100, clock, maxTrackedCallers = 3)

    // Three callers, seen at increasing times, none holding anything.
    assert(limiter.tryAcquire("oldest", 1))
    limiter.release("oldest", 1)
    advance(clock, 1)
    assert(limiter.tryAcquire("middle", 1))
    limiter.release("middle", 1)
    advance(clock, 1)
    assert(limiter.tryAcquire("newest", 1))
    limiter.release("newest", 1)
    assertEquals(limiter.trackedCallers, 3)

    // A fourth arrival displaces the least recently seen, and the set stays at the cap.
    advance(clock, 1)
    assert(limiter.tryAcquire("arrival", 1))
    assertEquals(limiter.trackedCallers, 3)
    assert(!limiter.isTracked("oldest"))
    assert(limiter.isTracked("middle"))
    assert(limiter.isTracked("newest"))
    assert(limiter.isTracked("arrival"))
  }

  test("a caller holding permits is never evicted") {
    val clock = new ManualClock()
    val limiter = newLimiter(100, clock, maxTrackedCallers = 2)

    // "holder" is the least recently seen by a wide margin, but its permits are still out.
    assert(limiter.tryAcquire("holder", 10))
    advance(clock, 60)
    assert(limiter.tryAcquire("other", 1))
    limiter.release("other", 1)

    // Room is made from the idle caller instead, and the set is allowed past the cap only while
    // every member is holding something.
    advance(clock, 1)
    assert(limiter.tryAcquire("arrival", 1))
    assert(limiter.isTracked("holder"))

    // The holder's permits come back, which is only possible because its state survived.
    limiter.release("holder", 10)
    assertEquals(limiter.usedPermits, 1)
  }

  test("permits are returned in full when many callers churn past the cap") {
    val clock = new ManualClock()
    val budget = 1000
    val limiter = newLimiter(budget, clock, maxTrackedCallers = 6)

    // Far more callers than slots, each acquiring and releasing in strict pairs. Every permit has
    // to come back: an evicted caller must never be one that still holds something.
    (1 to 20000).foreach { i =>
      val key = s"caller-${i % 40}"
      if (limiter.tryAcquire(key, 1 + (i % 3))) limiter.release(key, 1 + (i % 3))
      if (i % 500 == 0) advance(clock, 1)
    }
    assertEquals(limiter.usedPermits, 0)
  }

  test("an evicted caller does not hand its demerit to the next arrival") {
    val clock = new ManualClock()
    val limiter = newLimiter(100, clock, maxTrackedCallers = 2)

    // Drive one caller well over the penalty threshold, then let it go quiet.
    assert(limiter.tryAcquire("hog", 100))
    (1 to 20).foreach(_ => assert(!limiter.tryAcquire("hog", 1)))
    limiter.release("hog", 100)
    advance(clock, 6)

    // An unfamiliar caller arriving into a bucket with nothing held gets the whole budget, rather
    // than inheriting a penalty from the caller whose slot it took.
    assert(limiter.tryAcquire("filler", 1))
    limiter.release("filler", 1)
    advance(clock, 1)
    assert(limiter.tryAcquire("innocent", 100))
    assertEquals(limiter.usedPermits, 100)
  }

  test("a hog cannot shed its penalty by being evicted") {
    val clock = new ManualClock()
    val limiter = newLimiter(100, clock, maxTrackedCallers = 2)

    // The hog keeps arriving, so it is never the least recently seen and never the eviction
    // candidate however many other callers churn through the remaining slot.
    assert(limiter.tryAcquire("hog", 100))
    (1 to 20).foreach(_ => assert(!limiter.tryAcquire("hog", 1)))
    (1 to 50).foreach { i =>
      assert(!limiter.tryAcquire("hog", 1))
      val key = s"churn-$i"
      if (limiter.tryAcquire(key, 1)) limiter.release(key, 1)
    }
    assert(limiter.isTracked("hog"))

    // Still penalized: a permit frees up and the hog cannot take it back.
    limiter.release("hog", 1)
    assert(!limiter.tryAcquire("hog", 1))
  }

  test("a denied release returns the permits and marks the caller as wanting capacity") {
    val limiter = newLimiter(4, new ManualClock())

    // "a" was admitted by this limiter but shed by another, so its permits come back as a denial.
    assert(limiter.tryAcquire("a", 1))
    limiter.releaseDenied("a", 1)
    assertEquals(limiter.usedPermits, 0)

    // "a" is now waiting, so "b" is held to its share of two rather than borrowing the whole
    // bucket. A plain release would have left "a" invisible and "b" free to take all four.
    assert(!limiter.tryAcquire("b", 4))
    assert(limiter.tryAcquire("b", 2))
  }

  test("a plain release leaves the caller free to borrow") {
    val limiter = newLimiter(4, new ManualClock())
    assert(limiter.tryAcquire("a", 1))
    limiter.release("a", 1)
    assert(limiter.tryAcquire("b", 4))
  }

  test("a denied release accrues demerit, so a caller shed repeatedly becomes a hog") {
    val clock = new ManualClock()
    val limiter = newLimiter(100, clock)

    // Shed over and over without backing off. Demerit has to accumulate, or a caller refused by
    // another limit could hammer indefinitely without ever being contained.
    (1 to 10).foreach { _ =>
      assert(limiter.tryAcquire("hammer", 1))
      limiter.releaseDenied("hammer", 1)
    }
    assert(limiter.tryAcquire("victim", 1))

    // Penalized now, so it is held to its floor even though the bucket is nearly empty.
    assert(!limiter.tryAcquire("hammer", 50))
  }

  test("a denied release for a caller holding nothing is a no-op") {
    val limiter = newLimiter(4, new ManualClock())
    assert(limiter.tryAcquire("a", 2))

    // Nothing was shed, so nothing is recorded and no permits move.
    limiter.releaseDenied("never-acquired", 4)
    assertEquals(limiter.usedPermits, 2)

    // "never-acquired" was not marked as wanting capacity, so "a" may still borrow.
    assert(limiter.tryAcquire("a", 2))
    assertEquals(limiter.usedPermits, 4)
  }
}

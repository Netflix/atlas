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

import com.netflix.spectator.api.Clock

import java.time.Duration
import scala.collection.mutable

/**
  * Weighted concurrency limiter for a bucket shared by many callers that keeps one caller from
  * starving the others. It is a drop-in [[ConcurrencyLimiter]] that uses the `subKey` to identify
  * the individual caller within the bucket.
  *
  * Each caller is assigned an equal share of the budget based on the number of callers currently
  * contending (holding permits or having attempted within a recent window). A well-behaved caller
  * may borrow spare capacity above its share while no other well-behaved caller is waiting. A
  * caller that keeps getting denied without backing off accrues a decaying demerit; once it is
  * over a threshold it is treated as a hog: it is held below its fair share (floored at one permit,
  * never starved to zero) so headroom stays free for the others, and its own denials no longer
  * count as contention that would block others from borrowing. The demerit decays over time, so a
  * caller that stops hammering recovers its full share and the ability to borrow. Recent denial,
  * not current usage, is what marks a caller as wanting more capacity.
  *
  * Fairness acts on admission decisions as permits churn rather than by preempting held permits, so
  * it converges to a fair allocation as requests complete.
  *
  * Performance: all per-caller state lives in a single map keyed by `subKey`, and the running total
  * is maintained incrementally. `tryAcquire` makes one pass over the recent callers, reusing a
  * pre-allocated scan function, so it allocates nothing on the request path except a state object
  * the first time a new caller is seen. The pass, the total, and all state are guarded by a single
  * monitor; the section is O(number of recently active callers), which is bounded by concurrency.
  *
  * @param budget
  *     Total number of permits shared by the bucket. Must be positive.
  * @param clock
  *     Source of monotonic time used for the contention window and demerit decay.
  * @param window
  *     How long after its last attempt a caller still counts as contending, and how long a denial
  *     still marks a caller as wanting more capacity.
  * @param penalizedThreshold
  *     Demerit at or above which a caller is treated as a hog.
  * @param demeritPerDenial
  *     Amount added to a caller's demerit each time it is denied.
  * @param decayPerSecond
  *     Rate at which a caller's demerit decays while it is not being denied.
  */
final class FairShareLimiter(
  budget: Int,
  clock: Clock,
  window: Duration,
  penalizedThreshold: Double,
  demeritPerDenial: Double,
  decayPerSecond: Double
) extends ConcurrencyLimiter {

  import FairShareLimiter.*

  require(budget > 0, s"budget must be positive: $budget")

  private val windowNanos = window.toNanos
  private val decayPerNano = decayPerSecond / 1e9

  private val callers = new mutable.HashMap[String, CallerState]()
  private var total = 0

  // Scratch state for the single scan pass. Only touched while the monitor is held, so a single
  // reusable scan function can read it without capturing per-call variables (which would allocate).
  private var scanNow = 0L
  private var scanSubKey: String = ""
  private var scanActive = 0
  private var scanContended = false

  // Prune callers that are no longer active and no longer carry demerit, count the active callers,
  // and detect whether another well-behaved caller was recently denied (and so wants capacity). A
  // caller keeps its demerit until it fully decays, even after it ages out of the window, so a hog
  // cannot shed a penalty just by pausing for one window.
  private val scan: (String, CallerState) => Boolean = { (k, s) =>
    val d = s.demerit(scanNow, decayPerNano)
    val active = s.used > 0 || scanNow - s.lastSeen <= windowNanos
    val keep = active || d > 0.0
    if (keep) {
      if (active) scanActive += 1
      if (
        !scanContended && k != scanSubKey && d < penalizedThreshold &&
        s.lastDenied != NeverDenied && scanNow - s.lastDenied <= windowNanos
      ) {
        scanContended = true
      }
    }
    keep
  }

  private def clamp(cost: Int): Int = math.min(budget, math.max(1, cost))

  override def tryAcquire(subKey: String, cost: Int): Boolean = synchronized {
    val now = clock.monotonicTime()
    val c = clamp(cost)
    val state = callers.getOrElseUpdate(subKey, new CallerState())
    state.lastSeen = now

    scanNow = now
    scanSubKey = subKey
    scanActive = 0
    scanContended = false
    callers.filterInPlace(scan)

    val active = math.max(1, scanActive)
    val share = math.ceil(budget.toDouble / active).toInt
    val d = state.demerit(now, decayPerNano)
    val cap =
      if (d >= penalizedThreshold)
        // A hog is contained below its fair share so headroom stays free for well-behaved bursts,
        // floored at one permit rather than starved to zero. It is only penalized while its demerit
        // is elevated; once it stops being denied the demerit decays and it recovers.
        math.max(1, share - math.round(d).toInt)
      else if (scanContended)
        share
      else
        budget

    if (total + c > budget || state.used + c > cap) {
      state.demeritValue = d + demeritPerDenial
      state.demeritTime = now
      state.lastDenied = now
      false
    } else {
      state.used += c
      total += c
      true
    }
  }

  override def release(subKey: String, cost: Int): Unit = synchronized {
    val c = clamp(cost)
    // Decrement `total` only by what this caller actually held, so a mis-paired or duplicate
    // release cannot drive `total` below the real usage (which would let the bucket over-admit).
    // The state object is left in place so demerit and recency survive until it is pruned.
    callers.get(subKey).foreach { state =>
      if (state.used > 0) {
        val released = math.min(state.used, c)
        state.used -= released
        total = math.max(0, total - released)
      }
    }
  }

  override def usedPermits: Int = synchronized(total)
  override def maxPermits: Int = budget
}

object FairShareLimiter {

  /** Sentinel meaning a caller has not been denied. */
  private final val NeverDenied: Long = Long.MinValue

  /** Mutable per-caller bookkeeping held in the limiter's map; primitive fields avoid boxing. */
  private final class CallerState {

    var used: Int = 0
    var lastSeen: Long = 0L
    var lastDenied: Long = NeverDenied
    var demeritValue: Double = 0.0
    var demeritTime: Long = 0L

    /** Demerit decayed to `now`. */
    def demerit(now: Long, decayPerNano: Double): Double = {
      if (demeritValue <= 0.0) 0.0
      else math.max(0.0, demeritValue - (now - demeritTime) * decayPerNano)
    }
  }
}

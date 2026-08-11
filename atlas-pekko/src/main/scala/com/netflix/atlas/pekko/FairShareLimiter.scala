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
  * caller that stops hammering recovers its full share and the ability to borrow, and in any case
  * recovers once its demerit reaches `maxPenaltyDuration` past its last denial. Recent denial, not
  * current usage, is what marks a caller as wanting more capacity.
  *
  * Fairness acts on admission decisions as permits churn rather than by preempting held permits, so
  * it converges to a fair allocation as requests complete.
  *
  * Fairness is only as trustworthy as the `subKey`: a caller that can present many identities is
  * granted one share per identity, so the sub-key must be derived from an authenticated identity or
  * from an otherwise bounded set (see [[DefaultLimitKeyResolver]]). The number of tracked sub-keys
  * is capped as a safeguard against a resolver that lets an untrusted value through: when the cap
  * is reached the least recently seen caller that holds no permits is forgotten to make room, so
  * every caller keeps a history of its own and an unfamiliar one never inherits another's demerit.
  * A caller holding permits is never evicted, so the cap is a target rather than a hard ceiling and
  * the set is bounded by the greater of the cap and the budget. Note that it bounds the state
  * retained, not the dilution: a caller presenting as many identities as the cap allows still gets
  * that many shares, so the resolver is the defence and this is the backstop.
  *
  * Performance: all per-caller state lives in a single map keyed by `subKey`, plus one shared state
  * for the callers past the cap, and the running total is maintained incrementally. `tryAcquire`
  * makes one pass over the tracked callers, reusing a pre-allocated scan function, so it allocates
  * nothing on the request path except a state object the first time a new caller is seen and a map
  * entry while a caller holds permits against the shared state. The pass, the total, and all state
  * are guarded by a single monitor; the section is O(number of tracked callers), which is bounded
  * by `maxTrackedCallers` and, in the intended configuration, by concurrency well below it. Set the
  * cap with that in mind: it is the worst case for the critical section, not just for memory.
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
  * @param maxPenaltyDuration
  *     Longest a caller stays penalized after its last denial. Demerit is clamped only at the point
  *     where more of it can no longer contain a hog any further, so this bounds how long a penalty
  *     lasts rather than how severe it gets.
  * @param maxTrackedCallers
  *     Cap on the number of sub-keys tracked individually, which bounds the retained state. Must be
  *     positive.
  */
final class FairShareLimiter(
  budget: Int,
  clock: Clock,
  window: Duration,
  penalizedThreshold: Double,
  demeritPerDenial: Double,
  decayPerSecond: Double,
  maxPenaltyDuration: Duration,
  maxTrackedCallers: Int
) extends ConcurrencyLimiter {

  import FairShareLimiter.*

  require(budget > 0, s"budget must be positive: $budget")
  require(maxTrackedCallers > 0, s"maxTrackedCallers must be positive: $maxTrackedCallers")

  require(penalizedThreshold > 0.0, s"penalizedThreshold must be positive: $penalizedThreshold")
  // Zero decay would leave every demerit permanent, which also makes the state carrying it
  // unprunable, so the tracked set would wedge at `maxTrackedCallers` and every later caller would
  // be folded onto the shared state for good. Zero window would leave nothing inside the recency
  // tests, silently disabling contention detection and with it the fairness policy.
  require(decayPerSecond > 0.0, s"decayPerSecond must be positive: $decayPerSecond")
  require(demeritPerDenial > 0.0, s"demeritPerDenial must be positive: $demeritPerDenial")

  require(
    !maxPenaltyDuration.isNegative && !maxPenaltyDuration.isZero,
    s"maxPenaltyDuration must be positive: $maxPenaltyDuration"
  )
  require(!window.isNegative && !window.isZero, s"window must be positive: $window")

  private val windowNanos = window.toNanos
  private val decayPerNano = decayPerSecond / 1e9
  private val penaltyNanos = maxPenaltyDuration.toNanos

  // Demerit past this point has no further effect on the cap: `share` never exceeds `budget`, so
  // the penalty is already at its floor. All the excess does is extend how long the caller stays
  // penalized and how long its state is retained, both of which the caller sets by how hard it
  // hammers. Clamping keeps recovery proportional to the budget rather than to the size of a burst.
  private val maxDemerit = penalizedThreshold + budget

  private val callers = new mutable.HashMap[String, CallerState]()
  private var total = 0

  // Scratch state for the single scan pass. Only touched while the monitor is held, so a single
  // reusable scan function can read it without capturing per-call variables (which would allocate).
  private var scanNow = 0L
  private var scanSubKey: String = ""
  private var scanActive = 0
  private var scanContended = false
  private var scanCurrentActive = false

  // Least recently seen caller that holds no permits, and so the one to evict if the cap is reached
  // and a new caller has to be tracked. Chosen during the same pass rather than by a second walk.
  private var scanStalestKey: String = null
  private var scanStalestSeen = 0L

  // Prune callers that are no longer active and no longer carry demerit, count the active callers,
  // and detect whether another well-behaved caller was recently denied (and so wants capacity). A
  // caller keeps its demerit until it fully decays, even after it ages out of the window, so a hog
  // cannot shed a penalty just by pausing for one window.
  private val scan: (String, CallerState) => Boolean = { (k, s) =>
    val d = s.demerit(scanNow, decayPerNano, penaltyNanos)
    val active = s.used > 0 || scanNow - s.lastSeen <= windowNanos
    val keep = active || d > 0.0
    if (keep) {
      if (active) {
        scanActive += 1
        // Recorded so the caller in hand is not counted twice: the scan runs before its state is
        // refreshed, so `tryAcquire` has to count it separately when the scan did not.
        if (k == scanSubKey) scanCurrentActive = true
      }
      if (
        !scanContended && k != scanSubKey && d < penalizedThreshold &&
        s.lastDenied != NeverDenied && scanNow - s.lastDenied <= windowNanos
      ) {
        scanContended = true
      }
      // A caller holding permits is never a candidate: forgetting it would lose the permits it has
      // yet to return. Everything else is fair game, oldest first.
      if (s.used == 0 && (scanStalestKey == null || s.lastSeen < scanStalestSeen)) {
        scanStalestKey = k
        scanStalestSeen = s.lastSeen
      }
    }
    keep
  }

  private def clamp(cost: Int): Int = math.min(budget, math.max(1, cost))

  // Every caller gets a state of its own. When the cap is reached the least recently seen caller
  // that holds no permits is forgotten to make room, so an unfamiliar caller is never made to share
  // another's history. A caller that is hammering is by definition recently seen, so it is never
  // the one evicted and cannot shed a penalty by rotating identities; a caller quiet enough to be
  // evicted has, by the same token, a demerit that has already decayed or is about to.
  //
  // `getOrElse` with a null default avoids the `Option` that `get` would allocate on the request
  // path.
  private def stateFor(subKey: String): CallerState = {
    val existing = callers.getOrElse(subKey, null)
    if (existing != null) existing
    else {
      // The cap is a target rather than a hard ceiling. If every tracked caller is holding permits
      // there is nothing safe to evict, and the set is allowed past the cap; it stays bounded
      // because a holder holds at least one permit, so there can be no more of them than `budget`.
      if (callers.size >= maxTrackedCallers && scanStalestKey != null) {
        callers.remove(scanStalestKey)
        scanStalestKey = null
      }
      val state = new CallerState()
      callers.put(subKey, state)
      state
    }
  }

  override def tryAcquire(subKey: String, cost: Int): Boolean = synchronized {
    val now = clock.monotonicTime()
    val c = clamp(cost)

    // Prune before choosing the caller's state so the cap is tested against the callers actually
    // being tracked, not against entries this same pass is about to sweep, and so the eviction
    // candidate is picked from what survives. Choosing first would evict a caller to make room the
    // pass was about to free anyway.
    scanNow = now
    scanSubKey = subKey
    scanActive = 0
    scanContended = false
    scanCurrentActive = false
    scanStalestKey = null
    callers.filterInPlace(scan)

    val state = stateFor(subKey)
    state.lastSeen = now
    // The caller in hand is active by definition, but the scan ran before its state was refreshed,
    // so count it here unless the scan already did.
    if (!scanCurrentActive) scanActive += 1

    val active = math.max(1, scanActive)
    val share = math.ceil(budget.toDouble / active).toInt
    val d = state.demerit(now, decayPerNano, penaltyNanos)
    val cap =
      if (d >= penalizedThreshold)
        // A hog is contained below its fair share so headroom stays free for well-behaved bursts,
        // floored at one permit rather than starved to zero. It is only penalized while its demerit
        // is elevated; once it stops being denied the demerit decays and it recovers.
        math.max(1, share - math.min(share.toLong, math.round(d)).toInt)
      else if (scanContended)
        share
      else
        budget

    if (total + c > budget || state.used + c > cap) {
      state.demeritValue = math.min(maxDemerit, d + demeritPerDenial)
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
    // Charge the release only up to what the caller actually holds, so a mis-paired or duplicate
    // release cannot drive `total` below the real usage, which would let the bucket over-admit. A
    // caller holding permits is never evicted, so its state is always here to be found; a sub-key
    // that holds nothing is a no-op. The state is left in place so demerit and recency survive
    // until it is pruned.
    val state = callers.getOrElse(subKey, null)
    if (state != null && state.used > 0) {
      val released = math.min(state.used, c)
      state.used -= released
      total = math.max(0, total - released)
    }
  }

  override def usedPermits: Int = synchronized(total)
  override def maxPermits: Int = budget

  /** Number of callers being tracked individually. */
  private[pekko] def trackedCallers: Int = synchronized(callers.size)

  /** Whether a state is currently held for the given sub-key. Exposed for tests. */
  private[pekko] def isTracked(subKey: String): Boolean = synchronized(callers.contains(subKey))
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

    /**
      * Demerit decayed to `now`. It decays linearly and is also dropped outright once it is older
      * than `horizonNanos`, which bounds how long a penalty can last. Decay alone bounds it only by
      * how much demerit was accrued, so without the horizon a burst of denials would hold a caller
      * over the threshold for far longer than the burst itself.
      */
    def demerit(now: Long, decayPerNano: Double, horizonNanos: Long): Double = {
      if (demeritValue <= 0.0) 0.0
      else {
        val age = now - demeritTime
        if (age > horizonNanos) 0.0
        else math.max(0.0, demeritValue - age * decayPerNano)
      }
    }
  }
}

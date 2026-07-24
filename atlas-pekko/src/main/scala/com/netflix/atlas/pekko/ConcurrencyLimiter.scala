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

import java.util.concurrent.Semaphore

/**
  * Limits the number of concurrent operations that can be in flight at once. The limit is on
  * concurrency rather than a request rate: a permit is held for the full lifetime of an
  * operation and released when it completes. The limit is weighted, so an operation acquires a
  * number of permits equal to its estimated cost; a more expensive operation therefore consumes
  * a larger share of the budget for as long as it runs.
  *
  * Acquisition is non-blocking. This limiter is intended to be used from the request handling
  * path, where blocking a dispatcher thread waiting for capacity would be harmful. A caller that
  * cannot acquire immediately should shed the request rather than wait.
  *
  * Implementations must be safe for use by multiple threads.
  */
trait ConcurrencyLimiter {

  /**
    * Attempt to acquire `cost` permits without blocking. Returns true only if all of the permits
    * were available at the time of the call, in which case the caller must eventually call
    * [[release]] with the same cost. A cost that exceeds the total budget is clamped to the
    * budget so that a single large operation can still be admitted when the limiter is otherwise
    * idle.
    *
    * @param subKey
    *     Identifies the individual caller within this limiter. Ignored by limiters that do not
    *     provide per-caller fairness.
    * @param cost
    *     Number of permits to acquire. Values less than one are treated as one.
    */
  def tryAcquire(subKey: String, cost: Int): Boolean

  /**
    * Release `cost` permits previously acquired by [[tryAcquire]]. The cost must match the value
    * passed to the corresponding successful acquire.
    */
  def release(subKey: String, cost: Int): Unit

  /** Number of permits currently held. Intended for reporting via gauges. */
  def usedPermits: Int

  /** Total number of permits, or `Int.MaxValue` when there is no limit. */
  def maxPermits: Int
}

object ConcurrencyLimiter {

  /**
    * Create a limiter for the given budget. A budget of zero or less produces a limiter that
    * rejects every request, which can be used to fully block a caller. Otherwise a weighted
    * semaphore backed limiter is returned.
    */
  def apply(budget: Int): ConcurrencyLimiter = {
    if (budget <= 0) NoneLimiter else new SemaphoreLimiter(budget)
  }

  /** Limiter that permits every request without bound. */
  val Unlimited: ConcurrencyLimiter = AllowLimiter
}

/**
  * Weighted concurrency limiter backed by a [[java.util.concurrent.Semaphore]]. An operation
  * acquires a number of permits equal to its cost and releases the same number on completion.
  */
final class SemaphoreLimiter(budget: Int) extends ConcurrencyLimiter {

  require(budget > 0, s"budget must be positive: $budget")

  private val semaphore = new Semaphore(budget)

  private def clamp(cost: Int): Int = math.min(budget, math.max(1, cost))

  override def tryAcquire(subKey: String, cost: Int): Boolean = {
    semaphore.tryAcquire(clamp(cost))
  }

  override def release(subKey: String, cost: Int): Unit = {
    semaphore.release(clamp(cost))
  }

  override def usedPermits: Int = budget - semaphore.availablePermits()

  override def maxPermits: Int = budget
}

/** Limiter that always permits, used when an endpoint has no configured limit. */
object AllowLimiter extends ConcurrencyLimiter {

  override def tryAcquire(subKey: String, cost: Int): Boolean = true
  override def release(subKey: String, cost: Int): Unit = ()
  override def usedPermits: Int = 0
  override def maxPermits: Int = Int.MaxValue
}

/** Limiter that always denies, used to fully block a bucket by configuring a budget of zero. */
object NoneLimiter extends ConcurrencyLimiter {

  override def tryAcquire(subKey: String, cost: Int): Boolean = false
  override def release(subKey: String, cost: Int): Unit = ()
  override def usedPermits: Int = 0
  override def maxPermits: Int = 0
}

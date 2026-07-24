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

import com.netflix.spectator.api.Counter
import com.netflix.spectator.api.DistributionSummary
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.patterns.PolledMeter
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigUtil

import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import scala.jdk.CollectionConverters.*

/**
  * Enforces caller aware, weighted concurrency limits on requests. Each request maps to a
  * [[LimitKey]] and carries an estimated cost. Limiting is opt-in per endpoint: a request against
  * a configured endpoint must acquire capacity from both the per-bucket limiter selected by
  * [[LimitKey.bucket]] and the endpoint's total limiter, and both are released together when the
  * returned [[LimitToken]] is released. Acquisition is non-blocking, so a request that cannot get
  * capacity immediately is rejected rather than queued.
  *
  * Budgets are absolute for a single instance. When operating a fleet the configured budgets
  * should already account for the per-instance share of the overall capacity.
  *
  * All state is derived from configuration at construction time. Because both the set of endpoints
  * and, after collapsing unprovisioned callers onto the shared default bucket, the set of buckets
  * are fixed by configuration, the limiters and meters are pre-built and the request path performs
  * only lookups. This keeps `acquire` allocation free except for the token returned to a permitted
  * request.
  *
  * @param config
  *     Configuration subtree rooted at `atlas.pekko.request-limits`.
  * @param registry
  *     Registry used to report limiter activity.
  */
class RequestLimiter(config: Config, registry: Registry) {

  import RequestLimiter.*

  private val mode: Mode = Mode.fromString(config.getString("mode"))

  private val clock = registry.clock()

  // Tuning shared by every bucket that enables fair sharing.
  private val fairShare: FairShareConfig = FairShareConfig.from(config)

  // When disabled there is nothing to enforce, so no limiters or gauges are built.
  private val endpoints: Map[String, EndpointLimiter] = {
    if (mode == Mode.Off) Map.empty
    else readEndpoints(config).map { case (name, limits) => name -> buildEndpoint(name, limits) }
  }

  // Reused for every request that is not subject to limiting, so those paths allocate nothing.
  private val permit: Option[LimitToken] = Some(LimitToken.NoOp)

  /**
    * Attempt to reserve capacity for a request. Returns a [[LimitToken]] that must be released
    * when the request completes, or `None` if the request should be shed. In `shadow` mode a
    * token is always returned even when the policy would deny the request, so that limits can be
    * observed against real traffic before being enforced.
    */
  def acquire(key: LimitKey, cost: Int): Option[LimitToken] = {
    // Limiting is opt-in per endpoint. When disabled globally, or when the endpoint has no
    // configured limits, the request is permitted without reserving capacity or recording metrics.
    if (mode == Mode.Off) permit
    else
      endpoints.get(key.endpoint) match {
        case Some(endpoint) => endpoint.acquire(key.bucket, key.subKey, cost)
        case None           => permit
      }
  }

  /**
    * Bucket names that have a dedicated budget on the given endpoint. A [[LimitKeyResolver]] uses
    * this to decide whether a caller gets its own bucket or shares the default bucket. Callers
    * not listed here share [[LimitKey.DefaultBucket]].
    */
  def dedicatedBuckets(endpoint: String): Set[String] = {
    endpoints.get(endpoint).map(_.dedicatedBuckets).getOrElse(Set.empty)
  }

  private def buildEndpoint(name: String, limits: EndpointLimits): EndpointLimiter = {
    val total =
      if (limits.totalBudget <= 0) ConcurrencyLimiter.Unlimited
      else ConcurrencyLimiter(limits.totalBudget)
    monitorInflight(total, name, "__total__")

    // The default bucket is shared by many callers, so it can enable fair sharing to keep one
    // caller from starving the others. Dedicated buckets are per-caller and so gain nothing.
    val default =
      if (limits.fairShare && limits.defaultBucketBudget > 0)
        new FairShareLimiter(
          limits.defaultBucketBudget,
          clock,
          fairShare.window,
          fairShare.penalizedThreshold,
          fairShare.demeritPerDenial,
          fairShare.decayPerSecond
        )
      else ConcurrencyLimiter(limits.defaultBucketBudget)
    monitorInflight(default, name, LimitKey.DefaultBucket)

    // A caller budget named "default" is reserved for the shared bucket and does not create a
    // dedicated one; it is dropped here so the shared budget comes only from default-bucket-budget.
    val dedicated = (limits.callerBudgets - LimitKey.DefaultBucket).map {
      case (bucket, budget) =>
        val limiter = ConcurrencyLimiter(budget)
        monitorInflight(limiter, name, bucket)
        bucket -> limiter
    }

    new EndpointLimiter(
      total = total,
      default = default,
      dedicated = dedicated,
      enforce = mode == Mode.Enforce,
      cost = registry.distributionSummary("atlas.limiter.cost", "endpoint", name),
      permitted = requestCounter(name, "permitted", "none"),
      deniedBucket = requestCounter(name, "denied", "bucket"),
      deniedTotal = requestCounter(name, "denied", "total")
    )
  }

  private def requestCounter(endpoint: String, result: String, reason: String): Counter = {
    registry.counter(
      "atlas.limiter.requests",
      "endpoint",
      endpoint,
      "result",
      result,
      "reason",
      reason
    )
  }

  // Only bounded limiters are worth a gauge; an unlimited or fully blocked limiter would report a
  // constant value.
  private def monitorInflight(
    limiter: ConcurrencyLimiter,
    endpoint: String,
    bucket: String
  ): Unit = {
    if (limiter.maxPermits > 0 && limiter.maxPermits < Int.MaxValue) {
      PolledMeter
        .using(registry)
        .withName("atlas.limiter.inflight")
        .withTag("endpoint", endpoint)
        .withTag("bucket", bucket)
        .monitorValue(limiter, (l: ConcurrencyLimiter) => l.usedPermits.toDouble)
    }
  }
}

object RequestLimiter {

  /** How limit decisions are applied. */
  sealed trait Mode

  object Mode {

    /** Limiting is disabled; every request is permitted and no capacity is reserved. */
    case object Off extends Mode

    /**
      * Limit decisions are computed and reported, and capacity is reserved for permitted
      * requests, but requests the policy would deny are still admitted. Used to observe the
      * effect of a configuration against live traffic before enforcing it.
      */
    case object Shadow extends Mode

    /** Requests the policy denies are shed. */
    case object Enforce extends Mode

    def fromString(s: String): Mode = s.toLowerCase match {
      case "off"     => Off
      case "shadow"  => Shadow
      case "enforce" => Enforce
      case other     => throw new IllegalArgumentException(s"unknown request-limits mode: $other")
    }
  }

  /**
    * Pre-built limiters and meters for a single endpoint. The request path is confined to lookups
    * and counter increments so that only the permitted case allocates.
    */
  private class EndpointLimiter(
    total: ConcurrencyLimiter,
    default: ConcurrencyLimiter,
    dedicated: Map[String, ConcurrencyLimiter],
    enforce: Boolean,
    cost: DistributionSummary,
    permitted: Counter,
    deniedBucket: Counter,
    deniedTotal: Counter
  ) {

    // Result returned when the policy denies a request: nothing in enforce mode, a no-op token in
    // shadow mode so the request is still admitted. Reused so the denied path allocates nothing.
    private val denied: Option[LimitToken] = if (enforce) None else Some(LimitToken.NoOp)

    def dedicatedBuckets: Set[String] = dedicated.keySet

    def acquire(bucket: String, subKey: String, requestCost: Int): Option[LimitToken] = {
      cost.record(math.max(1, requestCost).toLong)

      // Any bucket that is not a provisioned dedicated caller shares the default bucket.
      val bucketLimiter = dedicated.getOrElse(bucket, default)
      if (!bucketLimiter.tryAcquire(subKey, requestCost)) {
        deniedBucket.increment()
        return denied
      }
      if (!total.tryAcquire(subKey, requestCost)) {
        bucketLimiter.release(subKey, requestCost)
        deniedTotal.increment()
        return denied
      }

      permitted.increment()
      Some(LimitToken { () =>
        total.release(subKey, requestCost)
        bucketLimiter.release(subKey, requestCost)
      })
    }
  }

  /**
    * Concurrency budgets for a single endpoint.
    *
    * @param totalBudget
    *     Cap on the combined cost across all buckets of the endpoint. A value of zero or less
    *     means there is no endpoint wide cap and only the per-bucket limits apply.
    * @param defaultBucketBudget
    *     Budget for the shared default bucket, applied to any bucket without a dedicated budget.
    * @param callerBudgets
    *     Dedicated budgets keyed by bucket name. Presence of an entry provisions a dedicated
    *     bucket for that caller. A value of zero blocks the bucket entirely.
    * @param fairShare
    *     Whether the shared default bucket enforces per-caller fairness.
    */
  private case class EndpointLimits(
    totalBudget: Int,
    defaultBucketBudget: Int,
    callerBudgets: Map[String, Int],
    fairShare: Boolean
  )

  /** Tuning for buckets that enable fair sharing. */
  private case class FairShareConfig(
    window: Duration,
    penalizedThreshold: Double,
    demeritPerDenial: Double,
    decayPerSecond: Double
  )

  private object FairShareConfig {

    // Fallbacks used when a caller supplies config without merging reference.conf (for example a
    // programmatic Config), and for any key omitted from a partial `fair-share` block. Keep these
    // in sync with the reference.conf defaults.
    private val defaults: Config = ConfigFactory.parseString(
      """
        |window = 5s
        |penalized-threshold = 3.0
        |demerit-per-denial = 1.0
        |decay-per-second = 0.3
        |""".stripMargin
    )

    def from(config: Config): FairShareConfig = {
      val base = if (config.hasPath("fair-share")) config.getConfig("fair-share") else defaults
      val c = base.withFallback(defaults)
      FairShareConfig(
        c.getDuration("window"),
        c.getDouble("penalized-threshold"),
        c.getDouble("demerit-per-denial"),
        c.getDouble("decay-per-second")
      )
    }
  }

  private def readEndpoints(config: Config): Map[String, EndpointLimits] = {
    if (!config.hasPath("endpoints")) Map.empty
    else {
      val obj = config.getConfig("endpoints")
      obj
        .root()
        .keySet()
        .asScala
        .map { name =>
          // Quote the key so an endpoint name that happens to contain a dot is looked up
          // literally rather than being interpreted as a nested config path.
          val ec = obj.getConfig(ConfigUtil.joinPath(name))
          val totalBudget = if (ec.hasPath("total-budget")) ec.getInt("total-budget") else 0
          val fair = ec.hasPath("fair-share") && ec.getBoolean("fair-share")
          name -> EndpointLimits(
            totalBudget,
            ec.getInt("default-bucket-budget"),
            readBudgets(ec),
            fair
          )
        }
        .toMap
    }
  }

  private def readBudgets(config: Config): Map[String, Int] = {
    if (!config.hasPath("caller-budgets")) Map.empty
    else {
      // Read each budget with getInt for consistent type coercion and path-qualified errors,
      // mirroring readEndpoints. Keys are quoted so caller ids containing dots (for example an
      // application name or user email) are treated as a single literal key.
      val cb = config.getConfig("caller-budgets")
      cb.root()
        .keySet()
        .asScala
        .map(k => k -> cb.getInt(ConfigUtil.joinPath(k)))
        .toMap
    }
  }
}

/**
  * Handle for capacity reserved by [[RequestLimiter.acquire]]. The reserved permits are returned
  * when [[release]] is called. Release is idempotent, so it is safe to attach it to more than one
  * completion signal for a request.
  */
sealed trait LimitToken {
  def release(): Unit
}

object LimitToken {

  /** Token that holds no capacity, returned when limiting is not applied to a request. */
  case object NoOp extends LimitToken {
    override def release(): Unit = ()
  }

  def apply(fn: () => Unit): LimitToken = new Impl(fn)

  private final class Impl(fn: () => Unit) extends LimitToken {

    private val released = new AtomicBoolean(false)
    override def release(): Unit = if (released.compareAndSet(false, true)) fn()
  }
}

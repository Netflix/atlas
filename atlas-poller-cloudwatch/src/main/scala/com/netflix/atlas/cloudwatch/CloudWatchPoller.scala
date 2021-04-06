/*
 * Copyright 2014-2021 Netflix, Inc.
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
package com.netflix.atlas.cloudwatch

import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import java.util.function.LongFunction
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.Status
import akka.routing.FromConfig
import akka.stream.CompletionStrategy
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.github.benmanes.caffeine.cache.Caffeine
import com.netflix.atlas.poller.Messages
import com.netflix.iep.leader.api.LeaderStatus
import com.netflix.spectator.api.Functions
import com.netflix.spectator.api.Id
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.histogram.BucketCounter
import com.netflix.spectator.api.patterns.PolledMeter
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient
import software.amazon.awssdk.services.cloudwatch.model.Datapoint
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit

/**
  * Poller for fetching data from CloudWatch and reporting the data into Atlas.
  *
  * @param config
  *     Config for setting up the poller. See the reference.conf for more details
  *     about the settings.
  * @param registry
  *     Registry for reporting metrics. The primary metrics are:
  *
  *     - `atlas.cloudwatch.listAge`: gauge showing the age in seconds of the list
  *       metadata.
  *     - `atlas.cloudwatch.listSize`: gauge showing the number of metrics found
  *       by calling list metrics on CloudWatch.
  *     - `atlas.cloudwatch.pendingGets`: gauge showing the number of metric get
  *       requests that are currently in-flight. This should be less than the
  *       list size or the system is starting to back up.
  *
  *     More detailed metrics on the specific AWS calls can be used by configuring
  *     the `spectator-ext-aws` metric collector with the SDK.
  * @param client
  *     AWS CloudWatch client.
  */
class CloudWatchPoller(
  config: Config,
  registry: Registry,
  client: CloudWatchClient,
  leaderStatus: LeaderStatus
) extends Actor
    with StrictLogging {

  import CloudWatchPoller._

  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._

  // Load the categories and tagger based on the config settings
  private val categories = getCategories(config)
  private val tagger = getTagger(config)

  // Metadata for the metrics in CloudWatch that we need to fetch and how to
  // map them into Atlas metrics.
  private val metricsMetadata = new AtomicReference[List[MetricMetadata]](Nil)

  // Child actor for getting the data for a metric. This will do the call using the
  // AWS SDK which is blocking and should be run in an isolated dispatcher.
  private val metricsGetRef =
    context.actorOf(
      FromConfig.props(
        Props(
          classOf[GetMetricActor],
          client,
          registry,
          buildBucketCounterCache(registry, categories)
        )
      ),
      "metrics-get"
    )

  // Throttler to control the rate of get metrics calls in order to stay within AWS SDK limits.
  private implicit val system = context.system

  private val throttledMetricsGetRef = Source
    .actorRef[List[MetricMetadata]](
      {
        case Status.Success(s: CompletionStrategy) => s
        case Status.Success(_)                     => CompletionStrategy.draining
      }: PartialFunction[Any, CompletionStrategy], {
        case Status.Failure(t) => t
      }: PartialFunction[Any, Throwable],
      config.getInt("atlas.cloudwatch.metrics-get-buffer-size"),
      OverflowStrategy.dropHead
    )
    .flatMapConcat(ms => Source(ms))
    .throttle(config.getInt("atlas.cloudwatch.metrics-get-max-rate-per-second"), 1.second)
    .toMat(Sink.foreach(message => metricsGetRef.tell(message, self)))(Keep.left)
    .run()

  // Child actor for listing metrics. This will do the call using the
  // AWS SDK which is blocking and should be run in an isolated dispatcher.
  private val metricsListRef =
    context.actorOf(FromConfig.props(Props(new ListMetricsActor(client, tagger))), "metrics-list")

  // Batch size to use for flushing data back to the poller manager.
  private val batchSize = config.getInt("atlas.cloudwatch.batch-size")

  // Actor that sent the Tick message and that should receive the response.
  private var responder: ActorRef = _

  // Indicates if a list operation is currently in-flight. Only one list operation
  // should be running at a time.
  private var pendingList: Boolean = false

  // Last time the metadata list was successfully updated.
  private val listUpdateTime: AtomicLong = PolledMeter
    .using(registry)
    .withName("atlas.cloudwatch.listAge")
    .monitorValue(new AtomicLong(registry.clock().wallTime()), Functions.age(registry.clock()))

  // Size of the metadata list. Compare with pending gets to get an idea of
  // how well we are keeping up with polling all of the data.
  private val listSize: AtomicLong = PolledMeter
    .using(registry)
    .withName("atlas.cloudwatch.listSize")
    .monitorValue(new AtomicLong(0L))

  // Number of get requests that are in-flight.
  private val pendingGets: AtomicLong = PolledMeter
    .using(registry)
    .withName("atlas.cloudwatch.pendingGets")
    .monitorValue(new AtomicLong(0L))

  // Cache of the last values received for a given metric
  private val cacheTTL = config.getDuration("atlas.cloudwatch.cache-ttl")

  private val metricCache = Caffeine
    .newBuilder()
    .expireAfterWrite(cacheTTL.toMillis, TimeUnit.MILLISECONDS)
    .build[MetricMetadata, MetricData]()

  // List keeping track of current batch of metric data.
  private val metricBatch: MList = new MList

  // Regularly flush any pending data that is still buffered
  context.system.scheduler.scheduleAtFixedRate(5.seconds, 5.seconds, self, Flush)

  def receive: Receive = {
    case Flush          => flush()
    case Messages.Tick  => refresh() // From PollerManager
    case m: MetricData  => processMetricData(m) // Response from GetMetricActor
    case MetricList(ms) => processMetricList(ms) // Response from ListMetricsActor
  }

  private def refresh(): Unit = {
    responder = sender()
    if (leaderStatus.hasLeadership) {
      logger.debug("Refreshing metrics")
      refreshMetricsList()
      fetchMetricsData()
      sendMetricData()
    } else {
      logger.debug("Skipping metrics refresh, do not have leadership.")
    }
  }

  /** Refresh the metadata list if one is not already in progress. */
  private def refreshMetricsList(): Unit = {
    if (pendingList) {
      logger.debug(s"list already in progress, skipping")
    } else {
      logger.info(s"refreshing list of cloudwatch metrics for ${categories.size} categories")
      pendingList = true
      metricsListRef ! ListMetrics(categories)
    }
  }

  /** Schedule all metrics in the metadata list for a refresh. */
  private def fetchMetricsData(): Unit = {
    val ms = metricsMetadata.get()
    val pending = pendingGets.get()
    val num = ms.size
    if (pending > num) {
      logger.warn(s"skipping fetch, still have ${pendingGets.get()} metrics pending")
    } else {
      if (pending > 0) {
        logger.warn(s"not keeping up, still have ${pendingGets.get()} metrics pending")
      }
      pendingGets.addAndGet(num)
      logger.info(s"requesting data for $num metrics")
      throttledMetricsGetRef ! ms
    }
  }

  /**
    * Process the returned list of metrics. An empty list will get ignored as it is likely
    * in error. The `atlas.cloudwatch.listAge` metric can be used to monitor how long it
    * has been since the metadata was successfully updated.
    */
  private def processMetricList(ms: List[MetricMetadata]): Unit = {
    pendingList = false
    if (ms.nonEmpty) {
      listUpdateTime.set(registry.clock().wallTime())
      val size = ms.size
      logger.info(s"found $size cloudwatch metrics")
      listSize.set(size)
      metricsMetadata.set(ms)
    } else {
      logger.warn("no cloudwatch metrics found")
    }
  }

  /** Add a datapoint to the cache. */
  private def processMetricData(data: MetricData): Unit = {
    pendingGets.decrementAndGet()
    val maybeMetricData = Option(metricCache.getIfPresent(data.meta))
    val prev = maybeMetricData.flatMap(_.current)
    val timestamp = data.lastReportedTimestamp.orElse {
      maybeMetricData.flatMap(_.lastReportedTimestamp)
    }
    metricCache.put(data.meta, data.copy(previous = prev, lastReportedTimestamp = timestamp))
  }

  /** Send all metrics that are currently in the cache. */
  private def sendMetricData(): Unit = {
    metricCache.asMap().forEach { (meta, data) =>
      val now = registry.clock().wallTime()
      val d = data.datapoint(Instant.ofEpochMilli(now))
      if (!d.sum.isNaN) {
        val ts = tagger(meta.dimensions) ++ meta.definition.tags + ("name" -> meta.definition.alias)
        val newValue = meta.convert(d)
        metricBatch += new AtlasDatapoint(ts, now, newValue)
        flush()
      }
    }
  }

  /** Flush data if the batch size is big enough or we are done with the current iteration. */
  private def flush(): Unit = {
    val now = registry.clock().wallTime()
    if (metricBatch.nonEmpty) {
      val age = now - metricBatch.head.timestamp
      if (age > 5000) {
        val batch = metricBatch.toList
        metricBatch.clear()
        logger.info(s"writing ${batch.size} metrics to client, age = $age ms")
        responder ! Messages.MetricsPayload(Map.empty, batch)
      } else if (metricBatch.lengthCompare(batchSize) >= 0) {
        val batch = metricBatch.toList
        metricBatch.clear()
        logger.info(s"writing ${batch.size} metrics to client, max batch size reached")
        responder ! Messages.MetricsPayload(Map.empty, batch)
      } else {
        logger.debug(s"not writing metrics, age = $age ms, size = ${metricBatch.size}")
      }
    }
  }
}

object CloudWatchPoller {

  case object Flush

  private val Zero = Datapoint
    .builder()
    .minimum(0.0)
    .maximum(0.0)
    .sum(0.0)
    .sampleCount(0.0)
    .timestamp(Instant.now())
    .unit(StandardUnit.NONE)
    .build()

  private val DatapointNaN = Datapoint
    .builder()
    .minimum(Double.NaN)
    .maximum(Double.NaN)
    .sum(Double.NaN)
    .sampleCount(Double.NaN)
    .timestamp(Instant.now())
    .unit(StandardUnit.NONE)
    .build()

  private[cloudwatch] def getCategories(config: Config): List[MetricCategory] = {
    import scala.jdk.CollectionConverters._
    val categories = config.getStringList("atlas.cloudwatch.categories").asScala.map { name =>
      val cfg = config.getConfig(s"atlas.cloudwatch.$name")
      MetricCategory.fromConfig(cfg)
    }
    categories.toList
  }

  private[cloudwatch] def getTagger(config: Config): Tagger = {
    val cfg = config.getConfig("atlas.cloudwatch.tagger")
    val cls = Class.forName(cfg.getString("class"))
    cls.getConstructor(classOf[Config]).newInstance(cfg).asInstanceOf[Tagger]
  }

  val PeriodLagIdName: String = "atlas.cloudwatch.periodLag"

  private def buildBucketCounterCache(
    registry: Registry,
    metricCategories: List[MetricCategory]
  ): Map[Id, BucketCounter] = {

    metricCategories.flatMap { category =>
      val noDataThreshold = category.periodCount + category.endPeriodOffset

      val bucketFunction: LongFunction[String] =
        (periodCount: Long) =>
          if (periodCount > noDataThreshold) // threshold is intended to be small (< 10)
            "no_data"
          else
            periodCount.toString

      val id = registry
        .createId(PeriodLagIdName)
        .withTag("cwNamespace", category.namespace)
        .withTag("periodSeconds", category.period.toString)

      category.metrics.map { metric =>
        val periodLagId = id.withTag("cwMetricName", metric.name)
        periodLagId -> BucketCounter
          .get(
            registry,
            periodLagId,
            bucketFunction
          )

      }
    }.toMap
  }

  case class GetMetricData(metric: MetricMetadata)

  case class MetricData(
    meta: MetricMetadata,
    previous: Option[Datapoint],
    current: Option[Datapoint],
    lastReportedTimestamp: Option[Instant]
  ) {

    def datapoint(now: Instant = Instant.now): Datapoint = {
      if (meta.definition.monotonicValue) {
        previous.fold(DatapointNaN) { p =>
          // For a monotonic counter, use the max statistic. These will typically have a
          // single reporting source that maintains the state over time. If the sample count
          // is larger than one, it will be a spike due to the reporter sending the value
          // multiple times within that interval. The max will allow us to ignore those
          // spikes and get the last written value.
          val c = current.getOrElse(DatapointNaN)
          val delta = math.max(c.maximum - p.maximum, 0.0)
          Datapoint
            .builder()
            .minimum(delta)
            .maximum(delta)
            .sum(delta)
            .sampleCount(c.sampleCount)
            .timestamp(c.timestamp)
            .unit(c.unit)
            .build()
        }
      } else {
        current.getOrElse {
          // We send 0 values for gaps in CloudWatch data because previously, users were
          // confused or concerned when they saw spans of NaN values in the data reported.
          // Those spans occur especially for low-volume resources and resources where the
          // only available period is greater than than the period configured for the
          // `MetricCategory` (although, that may indicate a misconfiguration).
          //
          // This implementation reports `0` if there's no configured timeout or if we've
          // received at least one datapoint until the timeout is exceeded. It reports `NaN`
          // until the first datapoint is received or for no data within and beyond the
          // timeout threshold.
          //
          // Requiring at least one datapoint prevents interpolating `0` from startup until
          // the timeout for obsolete resources.  It may result in NaN gaps for low volume
          // resources when deploying. But that is likely preferable to suddenly and briefly
          // reporting `0` for obsolete resources and possibly triggering alerts for those
          // with expressions that use wildcards for the resource selector.
          val reportNaN = meta.category.timeout.exists { timeout =>
            lastReportedTimestamp.fold(true) { timestamp =>
              Duration.between(timestamp, now).compareTo(timeout) > 0
            }
          }

          if (reportNaN) DatapointNaN else Zero
        }
      }
    }
  }

  case class ListMetrics(categories: List[MetricCategory])

  case class MetricList(data: List[MetricMetadata])
}

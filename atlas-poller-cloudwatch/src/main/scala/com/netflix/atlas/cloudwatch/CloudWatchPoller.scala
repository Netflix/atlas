/*
 * Copyright 2014-2017 Netflix, Inc.
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

import java.util.Date
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.routing.FromConfig
import com.amazonaws.services.cloudwatch.AmazonCloudWatch
import com.amazonaws.services.cloudwatch.model.Datapoint
import com.amazonaws.services.cloudwatch.model.StandardUnit
import com.netflix.atlas.poller.Messages
import com.netflix.spectator.api.Functions
import com.netflix.spectator.api.Registry
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

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
class CloudWatchPoller(config: Config, registry: Registry, client: AmazonCloudWatch)
  extends Actor with StrictLogging {

  import CloudWatchPoller._

  // Load the categories and tagger based on the config settings
  private val categories = getCategories(config)
  private val tagger = getTagger(config)

  // Metadata for the metrics in CloudWatch that we need to fetch and how to
  // map them into Atlas metrics.
  private val metricsMetadata = new AtomicReference[List[MetricMetadata]](Nil)

  // Child actor for getting the data for a metric. This will do the call using the
  // AWS SDK which is blocking and should be run in an isolated dispatcher.
  private val metricsGetRef = context.actorOf(
    FromConfig.props(Props(new GetMetricActor(client))), "metrics-get")

  // Child actor for listing metrics. This will do the call using the
  // AWS SDK which is blocking and should be run in an isolated dispatcher.
  private val metricsListRef = context.actorOf(
    FromConfig.props(Props(new ListMetricsActor(client, tagger))), "metrics-list")

  // Batch size to use for flushing data back to the poller manager.
  private val batchSize = config.getInt("atlas.cloudwatch.batch-size")

  // Actor that sent the Tick message and that should receive the response.
  private var responder: ActorRef = _

  // Indicates if a list operation is currently in-flight. Only one list operation
  // should be running at a time.
  private var pendingList: Boolean = false

  // Last time the metadata list was successfully updated.
  private val listUpdateTime: AtomicLong = registry.gauge(
    "atlas.cloudwatch.listAge",
    new AtomicLong(registry.clock().wallTime()),
    Functions.age(registry.clock()))

  // Size of the metadata list. Compare with pending gets to get an idea of
  // how well we are keeping up with polling all of the data.
  private val listSize: AtomicLong = registry.gauge(
    "atlas.cloudwatch.listSize",
    new AtomicLong(0L))

  // Number of get requests that are in-flight.
  private val pendingGets: AtomicLong = registry.gauge(
    "atlas.cloudwatch.pendingGets",
    new AtomicLong(0L))

  // List keeping track of current batch of metric data.
  private var metricBatch: MList = new MList

  def receive: Receive = {
    case Messages.Tick   => refresh()               // From PollerManager
    case m: MetricData   => processMetricData(m)    // Response from GetMetricActor
    case MetricList(ms)  => processMetricList(ms)   // Response from ListMetricsActor
  }

  private def refresh(): Unit = {
    responder = sender()
    refreshMetricsList()
    fetchMetricsData()
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
    if (pendingGets.get() > 0) {
      logger.warn(s"not keeping up, still have ${pendingGets.get()} metrics pending")
    }
    val ms = metricsMetadata.get()
    pendingGets.addAndGet(ms.size)
    logger.info(s"requesting data for ${ms.size} metrics")
    ms.foreach { m => metricsGetRef ! m }
  }

  /**
    * Process the returned list of metrics. An empty list will get ignored as it is likely
    * in error. The `atlas.cloudwatch.listAge` metric can be used to monitor how long it
    * has been since the metadata was succesfully updated.
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

  /** Add a datapoint to the current batch. */
  private def processMetricData(data: MetricData): Unit = {
    pendingGets.decrementAndGet()
    val d = data.datapoint
    val meta = data.meta
    val ts = tagger(meta.dimensions) ++ meta.definition.tags + ("name" -> meta.definition.alias)
    val now = System.currentTimeMillis()
    val newValue = meta.convert(d)
    metricBatch += new AtlasDatapoint(ts, now, newValue)
    flush()
  }

  /** Flush data if the batch size is big enough or we are done with the current iteration. */
  private def flush(): Unit = {
    if (metricBatch.nonEmpty && (pendingGets.get() <= 0 || metricBatch.size > batchSize)) {
      val batch = metricBatch.toList
      metricBatch.clear()
      logger.info(s"writing ${batch.size} metrics to client")
      responder ! Messages.MetricsPayload(Map.empty, batch)
    }
  }
}

object CloudWatchPoller {

  private val Zero = new Datapoint()
    .withMinimum(0.0)
    .withMaximum(0.0)
    .withSum(0.0)
    .withSampleCount(0.0)
    .withTimestamp(new Date())
    .withUnit(StandardUnit.None)

  private def getCategories(config: Config): List[MetricCategory] = {
    import scala.collection.JavaConverters._
    val categories = config.getStringList("atlas.cloudwatch.categories").asScala.map { name =>
      val cfg = config.getConfig(s"atlas.cloudwatch.$name")
      MetricCategory.fromConfig(cfg)
    }
    categories.toList
  }

  private def getTagger(config: Config): Tagger = {
    val cfg = config.getConfig("atlas.cloudwatch.tagger")
    val cls = Class.forName(cfg.getString("class"))
    cls.getConstructor(classOf[Config]).newInstance(cfg).asInstanceOf[Tagger]
  }

  case class GetMetricData(metric: MetricMetadata)

  case class MetricData(meta: MetricMetadata, data: Option[Datapoint]) {
    def datapoint: Datapoint = data.getOrElse(Zero)
  }

  case class ListMetrics(categories: List[MetricCategory])

  case class MetricList(data: List[MetricMetadata])
}

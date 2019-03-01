/*
 * Copyright 2014-2019 Netflix, Inc.
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
import java.util.Date

import akka.actor.Actor
import com.amazonaws.services.cloudwatch.AmazonCloudWatch
import com.amazonaws.services.cloudwatch.model.Datapoint
import com.netflix.spectator.api.Id
import com.netflix.spectator.api.Registry
import com.netflix.spectator.api.histogram.BucketCounter
import com.typesafe.scalalogging.StrictLogging

/**
  * Queries CloudWatch to get a datapoint for given metric. This actor makes blocking
  * calls to the Amazon SDK, it should be run in a dedicated dispatcher.
  */
class GetMetricActor(
  client: AmazonCloudWatch,
  registry: Registry,
  bucketCounterCache: Map[Id, BucketCounter]
) extends Actor
    with StrictLogging {
  import CloudWatchPoller._

  private val basePeriodLagId = registry.createId(PeriodLagIdName)

  def receive: Receive = {
    case m: MetricMetadata =>
      val metric = getMetric(m)
      val timestamp = metric.map(m => m.getTimestamp.toInstant)
      sender() ! MetricData(m, None, metric, timestamp)
  }

  /**
    * Queries CloudWatch for the metric with a time range ending at the current time and
    * starting at the configured number of periods.
    *
    * @return the most recent value prior to the configured end offset or None (if no
    *         values were reported in that time range).
    */
  private def getMetric(m: MetricMetadata): Option[Datapoint] = {
    try {
      import scala.collection.JavaConverters._
      val now = Instant.now()
      val start = now.minusSeconds(m.category.periodCount * m.category.period)

      val request = m.toGetRequest(start, now)
      val result = client.getMetricStatistics(request)

      // Datapoints might not be ordered by time, sort before using
      val datapoints = result.getDatapoints.asScala.toList
      val sorted = datapoints
        .filter(!_.getSum.isNaN)
        .sortWith(_.getTimestamp.getTime > _.getTimestamp.getTime)
      recordLag(now, sorted.headOption.map(_.getTimestamp), m)

      val endOffset = now.minusSeconds(m.category.endPeriodOffset * m.category.period)
      sorted.find(_.getTimestamp.toInstant.isBefore(endOffset))
    } catch {
      case e: Exception =>
        logger.warn(s"failed to get data for ${m.category.namespace}/${m.definition.name}", e)
        None
    }
  }

  /**
    * Record how many periods back from now the latest returned datapoint is. Though not perfect,
    * this will give a reasonable approximation of data latency across the collected metrics.
    */
  private def recordLag(now: Instant, maybeTimestamp: Option[Date], m: MetricMetadata): Unit = {
    val mostRecentDatapointTimestamp = maybeTimestamp.map(_.toInstant).getOrElse(Instant.EPOCH)
    val lagDuration = Duration.between(mostRecentDatapointTimestamp, now)

    val lagSeconds = lagDuration.toMillis / 1000L
    val periodLag = lagSeconds / m.category.period
    val id = basePeriodLagId
      .withTag("cwMetricName", m.definition.name)
      .withTag("cwNamespace", m.category.namespace)
      .withTag("periodSeconds", m.category.period.toString)

    bucketCounterCache.get(id).foreach(_.record(periodLag))
  }
}

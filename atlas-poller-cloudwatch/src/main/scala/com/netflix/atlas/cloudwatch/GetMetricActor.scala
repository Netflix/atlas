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

import java.time.Instant

import akka.actor.Actor
import com.amazonaws.services.cloudwatch.AmazonCloudWatch
import com.amazonaws.services.cloudwatch.model.Datapoint
import com.typesafe.scalalogging.StrictLogging

/**
  * Queries CloudWatch to get a datapoint for given metric. This actor makes blocking
  * calls to the Amazon SDK, it should be run in a dedicated dispatcher.
  */
class GetMetricActor(client: AmazonCloudWatch) extends Actor with StrictLogging {
  import CloudWatchPoller._

  def receive: Receive = {
    case m: MetricMetadata => sender() ! MetricData(m, getMetric(m))
  }

  /**
    * Queries cloudwatch for two periods of the metric and returns the most recent
    * value or None if no values were reported in that window.
    */
  private def getMetric(m: MetricMetadata): Option[Datapoint] = {
    try {
      import scala.collection.JavaConverters._
      val e = Instant.now().minusSeconds(m.category.period)
      val s = e.minusSeconds(2 * m.category.period)
      val request = m.toGetRequest(s, e)
      val result = client.getMetricStatistics(request)

      // Datapoints might not be ordered by time, sort before using
      val datapoints = result.getDatapoints.asScala.toList
      val sorted = datapoints
        .filter(!_.getSum.isNaN)
        .sortWith(_.getTimestamp.getTime > _.getTimestamp.getTime)
      sorted.headOption
    } catch {
      case e: Exception =>
        logger.warn(s"failed to get data for ${m.category.namespace}/${m.definition.name}", e)
        None
    }
  }
}

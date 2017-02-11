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

import akka.actor.Actor
import com.amazonaws.services.cloudwatch.AmazonCloudWatch
import com.amazonaws.services.cloudwatch.model.ListMetricsRequest
import com.amazonaws.services.cloudwatch.model.Metric
import com.typesafe.scalalogging.StrictLogging

/**
  * Queries CloudWatch to get a list of for available metrics. This actor makes blocking
  * calls to the Amazon SDK, it should be run in a dedicated dispatcher.
  */
class ListMetricsActor(client: AmazonCloudWatch, tagger: Tagger) extends Actor with StrictLogging {
  import CloudWatchPoller._

  def receive: Receive = {
    case ListMetrics(categories) => sender() ! MetricList(listMetrics(categories))
  }

  private def listMetrics(categories: List[MetricCategory]): List[MetricMetadata] = {
    try {
      val builder = categories.foldLeft(List.newBuilder[MetricMetadata]) { (acc, c) =>
        acc ++= listMetrics(c)
      }
      builder.result()
    } catch {
      case e: Exception =>
        logger.warn("failed to list metrics", e)
        Nil
    }
  }

  private def listMetrics(category: MetricCategory): List[MetricMetadata] = {
    import scala.collection.JavaConverters._
    category.toListRequests.flatMap { case (definition, request) =>
      val mname = s"${category.namespace}/${definition.name}"
      logger.debug(s"refreshing list for $mname")
      val candidates = listMetrics(request, Nil).map { m =>
        MetricMetadata(category, definition, m.getDimensions.asScala.toList)
      }
      logger.debug(s"before filtering, found ${candidates.size} metrics for $mname")
      val after = candidates.filter { m => category.filter.matches(tagger(m.dimensions)) }
      logger.debug(s"after filtering, found ${after.size} metrics for $mname")
      after
    }
  }

  @scala.annotation.tailrec
  private def listMetrics(request: ListMetricsRequest, metrics: List[Metric]): List[Metric] = {
    import scala.collection.JavaConverters._
    val result = client.listMetrics(request)
    val ms = metrics ++ result.getMetrics.asScala
    if (result.getNextToken == null) ms else {
      listMetrics(request.withNextToken(result.getNextToken), ms)
    }
  }
}

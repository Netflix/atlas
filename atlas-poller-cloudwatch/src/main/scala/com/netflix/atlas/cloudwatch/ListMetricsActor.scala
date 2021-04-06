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

import akka.actor.Actor
import com.typesafe.scalalogging.StrictLogging
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient
import software.amazon.awssdk.services.cloudwatch.model.ListMetricsRequest
import software.amazon.awssdk.services.cloudwatch.model.Metric

/**
  * Queries CloudWatch to get a list of for available metrics. This actor makes blocking
  * calls to the Amazon SDK, it should be run in a dedicated dispatcher.
  */
class ListMetricsActor(client: CloudWatchClient, tagger: Tagger) extends Actor with StrictLogging {
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
    import scala.jdk.CollectionConverters._
    category.toListRequests.flatMap {
      case (definition, request) =>
        val mname = s"${category.namespace}/${definition.name}"
        logger.debug(s"refreshing list for $mname")
        val candidates = listMetrics(request).map { m =>
          MetricMetadata(category, definition, m.dimensions.asScala.toList)
        }
        logger.debug(s"before filtering, found ${candidates.size} metrics for $mname")
        val after = candidates.filter { m =>
          category.filter.matches(tagger(m.dimensions))
        }
        logger.debug(s"after filtering, found ${after.size} metrics for $mname")
        after
    }
  }

  private def listMetrics(request: ListMetricsRequest): List[Metric] = {
    import scala.jdk.StreamConverters._
    client
      .listMetricsPaginator(request)
      .metrics()
      .stream()
      .toScala(List)
  }
}

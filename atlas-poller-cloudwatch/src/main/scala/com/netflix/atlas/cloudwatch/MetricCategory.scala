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
import java.util.concurrent.TimeUnit
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.QueryVocabulary
import com.netflix.atlas.core.stacklang.Interpreter
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import software.amazon.awssdk.services.cloudwatch.model.DimensionFilter
import software.amazon.awssdk.services.cloudwatch.model.ListMetricsRequest

/**
  * Category of metrics to fetch from CloudWatch. This will typically correspond with
  * a CloudWatch namespace, though there may be multiple categories per namespace if
  * there is some variation in the behavior. For example, some namespaces will use a
  * different period for some metrics.
  *
  * @param namespace
  *     CloudWatch namespace for the data.
  * @param period
  *     How frequently data in this category is updated. Atlas is meant for data that
  *     is continuously reported and requires a consistent step. To minimize confusion
  *     for CloudWatch data we use the last reported value in CloudWatch as long as it
  *     is within one period from the polling time. The period is also needed for
  *     performing rate conversions on some metrics.
  * @param endPeriodOffset
  *     How many periods back from `now` to set the end of the time range.
  * @param periodCount
  *     How many periods total to retrieve.
  * @param timeout
  *     How long the system should interpolate a base value for unreported CloudWatch
  *     metrics before ceasing to send them. CloudWatch will return 0 metrics for at
  *     least two cases:
  *     - No metrics were recorded.
  *     - The resource has been removed, metrics still show up when listing metrics due
  *       to the retention window, but the specified time interval for the metric
  *       statistics request is after the removal.
  * @param dimensions
  *     The dimensions to query for when getting data from CloudWatch. For the
  *     GetMetricData calls we have to explicitly specify all of the dimensions. In some
  *     cases CloudWatch has duplicate data for pre-computed aggregates. For example,
  *     ELB data is reported overall for the load balancer and by zone. For Atlas it
  *     is better to map in the most granular form and allow the aggregate to be done
  *     dynamically at query time.
  * @param metrics
  *     The set of metrics to fetch and metadata for how to convert them.
  * @param filter
  *     Query expression used to select the set of metrics which should get published.
  *     This can sometimes be useful for debugging or if there are many "spammy" metrics
  *     for a given category.
  */
case class MetricCategory(
  namespace: String,
  period: Int,
  endPeriodOffset: Int,
  periodCount: Int,
  timeout: Option[Duration],
  dimensions: List[String],
  metrics: List[MetricDefinition],
  filter: Query
) {

  /**
    * Returns a set of list requests to fetch the metadata for the metrics matching
    * this category. As there may be a lot of data in CloudWatch that we are not
    * interested in, the list is used to restrict to the subset we actually care
    * about rather than a single request fetching everything for the namespace.
    */
  def toListRequests: List[(MetricDefinition, ListMetricsRequest)] = {
    import scala.jdk.CollectionConverters._
    metrics.map { m =>
      m -> ListMetricsRequest
        .builder()
        .namespace(namespace)
        .metricName(m.name)
        .dimensions(dimensions.map(d => DimensionFilter.builder().name(d).build()).asJava)
        .build()
    }
  }
}

object MetricCategory extends StrictLogging {

  private val interpreter = Interpreter(QueryVocabulary.allWords)

  private def parseQuery(query: String): Query = {
    interpreter.execute(query).stack match {
      case (q: Query) :: Nil => q
      case _ =>
        logger.warn(s"invalid query '$query', using default of ':true'")
        Query.True
    }
  }

  def fromConfig(config: Config): MetricCategory = {
    import scala.jdk.CollectionConverters._
    val metrics = config.getConfigList("metrics").asScala.toList
    val filter =
      if (!config.hasPath("filter")) Query.True
      else {
        parseQuery(config.getString("filter"))
      }
    val timeout = if (config.hasPath("timeout")) Some(config.getDuration("timeout")) else None
    val endPeriodOffset =
      if (config.hasPath("end-period-offset")) config.getInt("end-period-offset") else 1
    val periodCount = if (config.hasPath("period-count")) config.getInt("period-count") else 6

    apply(
      namespace = config.getString("namespace"),
      period = config.getDuration("period", TimeUnit.SECONDS).toInt,
      endPeriodOffset = endPeriodOffset,
      periodCount = periodCount,
      timeout = timeout,
      dimensions = config.getStringList("dimensions").asScala.toList,
      metrics = metrics.flatMap(MetricDefinition.fromConfig),
      filter = filter
    )
  }
}

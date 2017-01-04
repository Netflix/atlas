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

import java.util.concurrent.TimeUnit

import com.amazonaws.services.cloudwatch.model.DimensionFilter
import com.amazonaws.services.cloudwatch.model.ListMetricsRequest
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.QueryVocabulary
import com.netflix.atlas.core.stacklang.Interpreter
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

/**
  * Category of metrics to fetch from cloudwatch. This will typically correspond with
  * a CloudWatch namespace, though there may be multiple categories per namespace if
  * there is some variation in the behavior. For example, some namespaces will use a
  * different period for some metrics.
  *
  * @param namespace
  *     CloudWatch namespace for the data.
  * @param period
  *     How frequently data in this category is updated. Atlas is meant for data that
  *     is continuously reported and requires a consistent step. To minimize confusion
  *     for cloudwatch data we use the last reported value in cloudwatch as long as it
  *     is within one period from the polling time. The period is also needed for
  *     performing rate conversions on some metrics.
  * @param dimensions
  *     The dimensions to query for when getting data from cloudwatch. For the
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
  dimensions: List[String],
  metrics: List[MetricDefinition],
  filter: Query) {

  /**
    * Returns a set of list requests to fetch the metadata for the metrics matching
    * this category. As there may be a lot of data in cloudwatch that we are not
    * interested in, the list is used to restrict to the subset we actually care
    * about rather than a single request fetching everything for the namespace.
    */
  def toListRequests: List[(MetricDefinition, ListMetricsRequest)] = {
    import scala.collection.JavaConverters._
    metrics.map { m =>
      m -> new ListMetricsRequest()
        .withNamespace(namespace)
        .withMetricName(m.name)
        .withDimensions(dimensions.map(d => new DimensionFilter().withName(d)).asJava)
    }
  }
}

object MetricCategory extends StrictLogging {

  private val interpreter = Interpreter(QueryVocabulary.allWords)

  private def parseQuery(query: String): Query = {
    interpreter.execute(query).stack match {
      case (q: Query) :: Nil => q
      case _  =>
        logger.warn(s"invalid query '$query', using default of ':true'")
        Query.True
    }
  }

  def fromConfig(config: Config): MetricCategory = {
    import scala.collection.JavaConverters._
    val metrics = config.getConfigList("metrics").asScala.toList
    val filter = if (!config.hasPath("filter")) Query.True else {
      parseQuery(config.getString("filter"))
    }
    apply(
      namespace = config.getString("namespace"),
      period = config.getDuration("period", TimeUnit.SECONDS).toInt,
      dimensions = config.getStringList("dimensions").asScala.toList,
      metrics = metrics.flatMap(MetricDefinition.fromConfig),
      filter = filter
    )
  }
}

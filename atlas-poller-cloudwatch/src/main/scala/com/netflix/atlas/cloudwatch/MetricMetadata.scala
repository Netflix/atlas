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
import java.util.Date

import com.amazonaws.services.cloudwatch.model.Datapoint
import com.amazonaws.services.cloudwatch.model.Dimension
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest
import com.amazonaws.services.cloudwatch.model.Statistic

/**
  * Metadata for a particular metric to retrieve from CloudWatch.
  */
case class MetricMetadata(
  category: MetricCategory,
  definition: MetricDefinition,
  dimensions: List[Dimension]) {

  def convert(d: Datapoint): Double = definition.conversion(this, d)

  def toGetRequest(s: Instant, e: Instant): GetMetricStatisticsRequest = {
    import scala.collection.JavaConverters._
    new GetMetricStatisticsRequest()
      .withMetricName(definition.name)
      .withNamespace(category.namespace)
      .withDimensions(dimensions.asJava)
      .withStatistics(Statistic.Maximum, Statistic.Minimum, Statistic.Sum, Statistic.SampleCount)
      .withPeriod(category.period)
      .withStartTime(new Date(s.toEpochMilli))
      .withEndTime(new Date(e.toEpochMilli))
  }
}

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

import software.amazon.awssdk.services.cloudwatch.model.Datapoint
import software.amazon.awssdk.services.cloudwatch.model.Dimension
import software.amazon.awssdk.services.cloudwatch.model.GetMetricStatisticsRequest
import software.amazon.awssdk.services.cloudwatch.model.Statistic

import java.time.Instant

/**
  * Metadata for a particular metric to retrieve from CloudWatch.
  */
case class MetricMetadata(
  category: MetricCategory,
  definition: MetricDefinition,
  dimensions: List[Dimension]
) {

  def convert(d: Datapoint): Double = definition.conversion(this, d)

  def toGetRequest(s: Instant, e: Instant): GetMetricStatisticsRequest = {
    import scala.jdk.CollectionConverters._
    GetMetricStatisticsRequest
      .builder()
      .metricName(definition.name)
      .namespace(category.namespace)
      .dimensions(dimensions.asJava)
      .statistics(Statistic.MAXIMUM, Statistic.MINIMUM, Statistic.SUM, Statistic.SAMPLE_COUNT)
      .period(category.period)
      .startTime(s)
      .endTime(e)
      .build()
  }
}

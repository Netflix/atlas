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

import com.amazonaws.services.cloudwatch.model.Datapoint
import com.amazonaws.services.cloudwatch.model.StandardUnit
import com.typesafe.config.Config

/**
  * Definition for a particular cloudwatch metric.
  *
  * @param name
  *     Name of the metric as it is stored in CloudWatch.
  * @param alias
  *     Alias to make the metric follow Atlas naming conventions.
  * @param conversion
  *     Conversion to apply to the datapoint when extracting the value. See [[Conversions]]
  *     for more information.
  * @param tags
  *     Tags that should be applied to the metric.
  */
case class MetricDefinition(
  name: String,
  alias: String,
  conversion: (MetricMetadata, Datapoint) => Double,
  tags: Map[String, String])

object MetricDefinition {

  /**
    * Create a set of metric definitions from a config. In most cases the list will have
    * a single item if using a standard conversion from [[Conversions]]. Three composite
    * types are supported: `timer`, `timer-millis`, and `dist-summary`. These will get
    * mapped in as close as possible to way spectator would map in those types. The primary
    * gap is that with CloudWatch we cannot get a totalOfSquares so the standard deviation
    * cannot be computed.
    *
    * Note, `timer` should generally be preferred over `timer-millis`. If the unit is
    * specified on the datapoint, then it will automatically convert to seconds. When using
    * `timer-millis` the unit is ignored and the data will be assumed to be in milliseconds.
    * It is mostly intended for use with some latency values that do not explicitly mark the
    * unit.
    */
  def fromConfig(config: Config): List[MetricDefinition] = {
    import scala.collection.JavaConverters._
    val tags = if (!config.hasPath("tags")) Map.empty[String, String] else {
      config.getConfigList("tags").asScala
        .map(c => c.getString("key") -> c.getString("value"))
        .toMap
    }

    config.getString("conversion") match {
      case "timer"          => newDist(config, "totalTime", tags)
      case "timer-millis"   => milliTimer(config, tags)
      case "dist-summary"   => newDist(config, "totalAmount", tags)
      case cnv              => List(newMetricDef(config, cnv, tags))
    }
  }

  /**
    * Note, count must come first so it is easy to skip for other unit conversions. See
    * [[milliTimer]] for example.
    */
  private def newDist(config: Config, total: String, tags: Tags): List[MetricDefinition] = {
    List(
      newMetricDef(config, "count,rate", tags + ("statistic" -> "count")),
      newMetricDef(config, "sum,rate",   tags + ("statistic" -> total)),
      newMetricDef(config, "max",        tags + ("statistic" -> "max"))
    )
  }

  /**
    * Timer where the input unit is milliseconds and we need to perform an unit conversion
    * on the totalTime and max results.
    */
  private def milliTimer(config: Config, tags: Tags): List[MetricDefinition] = {
    val ms = newDist(config, "totalTime", tags)
    ms.head :: ms.tail.map { m =>
      m.copy(conversion = Conversions.toUnit(m.conversion, StandardUnit.Milliseconds))
    }
  }

  private def newMetricDef(config: Config, cnv: String, tags: Tags): MetricDefinition = {
    MetricDefinition(
      name = config.getString("name"),
      alias = config.getString("alias"),
      conversion = Conversions.fromName(cnv),
      tags = tags
    )
  }
}

/*
 * Copyright 2014-2025 Netflix, Inc.
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
package com.netflix.atlas.chart

import java.io.OutputStream
import java.io.OutputStreamWriter
import com.netflix.atlas.chart.model.*
import com.netflix.atlas.core.model.SummaryStats
import tools.jackson.core.ObjectWriteContext

/**
  * Returns a handful of summary stats instead of all the raw data for a given graph.
  */
class StatsJsonGraphEngine extends GraphEngine {

  import com.netflix.atlas.chart.GraphEngine.*

  def name: String = "stats.json"

  def contentType: String = "application/json"

  def write(config: GraphDef, output: OutputStream): Unit = {
    val writer = new OutputStreamWriter(output, "UTF-8")
    val seriesList = config.plots.flatMap(_.lines)
    val gen = jsonFactory.createGenerator(ObjectWriteContext.empty, writer)

    gen.writeStartObject()
    gen.writeNumberProperty("start", config.startTime.toEpochMilli)
    gen.writeNumberProperty("end", config.endTime.toEpochMilli)
    gen.writeNumberProperty("step", config.step)

    gen.writeArrayPropertyStart("legend")
    seriesList.foreach { series =>
      val label = series.data.label
      gen.writeString(label)
    }
    gen.writeEndArray()

    gen.writeArrayPropertyStart("metrics")
    seriesList.foreach { series =>
      gen.writeStartObject()
      series.data.tags.toList.sortWith(_._1 < _._1).foreach { t =>
        gen.writeStringProperty(t._1, t._2)
      }
      gen.writeEndObject()
    }
    gen.writeEndArray()

    gen.writeArrayPropertyStart("stats")
    seriesList.foreach { series =>
      val stats =
        SummaryStats(series.data, config.startTime.toEpochMilli, config.endTime.toEpochMilli)

      gen.writeStartObject()
      gen.writeNumberProperty("count", stats.count)
      if (seriesList.nonEmpty) {
        gen.writeNumberProperty("avg", stats.avg)
        gen.writeNumberProperty("total", stats.total)
        gen.writeNumberProperty("max", stats.max)
        gen.writeNumberProperty("min", stats.min)
        gen.writeNumberProperty("last", stats.last)
      }
      gen.writeEndObject()
    }
    gen.writeEndArray()

    gen.writeArrayPropertyStart("notices")
    config.warnings.foreach(gen.writeString)
    gen.writeEndArray()

    gen.writeEndObject()
    gen.flush()
  }
}

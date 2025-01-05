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
    val gen = jsonFactory.createGenerator(writer)

    gen.writeStartObject()
    gen.writeNumberField("start", config.startTime.toEpochMilli)
    gen.writeNumberField("end", config.endTime.toEpochMilli)
    gen.writeNumberField("step", config.step)

    gen.writeArrayFieldStart("legend")
    seriesList.foreach { series =>
      val label = series.data.label
      gen.writeString(label)
    }
    gen.writeEndArray()

    gen.writeArrayFieldStart("metrics")
    seriesList.foreach { series =>
      gen.writeStartObject()
      series.data.tags.toList.sortWith(_._1 < _._1).foreach { t =>
        gen.writeStringField(t._1, t._2)
      }
      gen.writeEndObject()
    }
    gen.writeEndArray()

    gen.writeArrayFieldStart("stats")
    seriesList.foreach { series =>
      val stats =
        SummaryStats(series.data, config.startTime.toEpochMilli, config.endTime.toEpochMilli)

      gen.writeStartObject()
      gen.writeNumberField("count", stats.count)
      if (seriesList.nonEmpty) {
        gen.writeNumberField("avg", stats.avg)
        gen.writeNumberField("total", stats.total)
        gen.writeNumberField("max", stats.max)
        gen.writeNumberField("min", stats.min)
        gen.writeNumberField("last", stats.last)
      }
      gen.writeEndObject()
    }
    gen.writeEndArray()

    gen.writeArrayFieldStart("notices")
    config.warnings.foreach(gen.writeString)
    gen.writeEndArray()

    gen.writeEndObject()
    gen.flush()
  }
}

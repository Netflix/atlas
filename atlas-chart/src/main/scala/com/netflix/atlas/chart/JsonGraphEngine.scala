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

class JsonGraphEngine extends GraphEngine {

  import com.netflix.atlas.chart.GraphEngine.*

  def name: String = "json"

  def contentType: String = "application/json"

  def write(config: GraphDef, output: OutputStream): Unit = {
    val writer = new OutputStreamWriter(output, "UTF-8")
    val seriesList = config.plots.flatMap(_.lines)
    val gen = jsonFactory.createGenerator(writer)

    gen.writeStartObject()
    gen.writeNumberField("start", config.startTime.toEpochMilli)
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

    gen.writeArrayFieldStart("values")
    val step = config.step.asInstanceOf[Int]
    val endTime = config.endTime.toEpochMilli
    var timestamp = config.startTime.toEpochMilli
    while (timestamp < endTime) {
      gen.writeStartArray()
      seriesList.foreach { series =>
        val v = series.data.data(timestamp)
        gen.writeNumber(v)
      }
      gen.writeEndArray()
      timestamp += step
    }
    gen.writeEndArray()

    gen.writeArrayFieldStart("notices")
    config.warnings.foreach(gen.writeString)
    gen.writeEndArray()

    if (config.stats.inputLines > 0) {
      val start = config.startTime.toEpochMilli / 1000
      val end = config.endTime.toEpochMilli / 1000
      val graphLines = config.plots.map(_.data.size).sum
      val graphDatapoints = graphLines * ((end - start) / (config.step / 1000) + 1)

      gen.writeObjectFieldStart("explain")
      gen.writeNumberField("dataFetchTime", config.loadTime)

      gen.writeNumberField("inputLines", config.stats.inputLines)
      gen.writeNumberField("intermediateLines", config.stats.outputLines)
      gen.writeNumberField("graphLines", graphLines)

      gen.writeNumberField("inputDatapoints", config.stats.inputDatapoints)
      gen.writeNumberField("intermediateDatapoints", config.stats.outputDatapoints)
      gen.writeNumberField("graphDatapoints", graphDatapoints)
      gen.writeEndObject()
    }

    gen.writeEndObject()
    gen.flush()
  }
}

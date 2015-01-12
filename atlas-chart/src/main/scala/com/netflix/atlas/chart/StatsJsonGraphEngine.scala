/*
 * Copyright 2015 Netflix, Inc.
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

import com.fasterxml.jackson.core.JsonGenerator
import com.netflix.atlas.core.model.SummaryStats


/**
 * Returns a handful of summary stats instead of all the raw data for a given graph.
 */
class StatsJsonGraphEngine extends GraphEngine {

  import java.lang.{Double => JDouble}

import com.netflix.atlas.chart.GraphEngine._

  def name: String = "stats.json"
  def contentType: String = "application/json"

  private def writeRawField(gen: JsonGenerator, name: String, value: String) {
    gen.writeFieldName(name)
    gen.writeRawValue(value)
  }

  private def numberStr(numberFmt: String, v: Double): String = {
    numberFmt.format(v)
  }

  def write(config: GraphDef, output: OutputStream) {
    val writer = new OutputStreamWriter(output, "UTF-8")
    val seriesList = config.plots.flatMap(_.series)
    val count = seriesList.size
    val numberFmt = config.numberFormat
    val gen = jsonFactory.createGenerator(writer)

    gen.writeStartObject()
    gen.writeNumberField("start", config.startTime.toEpochMilli)
    gen.writeNumberField("end", config.endTime.toEpochMilli)
    gen.writeNumberField("step", config.step)

    gen.writeArrayFieldStart("legend")
    (0 until count).zip(seriesList).foreach {
      case (i, series) =>
        val label = series.label
        gen.writeString(label)
    }
    gen.writeEndArray()

    gen.writeArrayFieldStart("metrics")
    seriesList.foreach { series =>
      gen.writeStartObject()
      series.tags.toList.sortWith(_._1 < _._1).foreach { t =>
        gen.writeStringField(t._1, t._2)
      }
      gen.writeEndObject()
    }
    gen.writeEndArray()

    gen.writeArrayFieldStart("stats")
    seriesList.foreach { series =>
      val stats = SummaryStats(series.data, config.startTime.toEpochMilli, config.endTime.toEpochMilli)

      gen.writeStartObject()
      gen.writeNumberField("count", stats.count)
      if (count > 0) {
        writeRawField(gen, "avg", numberStr(numberFmt, stats.avg))
        writeRawField(gen, "total", numberStr(numberFmt, stats.total))
        writeRawField(gen, "max", numberStr(numberFmt, stats.max))
        writeRawField(gen, "min", numberStr(numberFmt, stats.min))
        writeRawField(gen, "last", numberStr(numberFmt, stats.last))
      }
      gen.writeEndObject()
    }
    gen.writeEndArray()

    gen.writeArrayFieldStart("notices")
    config.notices.foreach {
      case Info(msg)    => gen.writeString("INFO: " + msg)
      case Warning(msg) => gen.writeString("WARNING: " + msg)
      case Error(msg)   => gen.writeString("ERROR: " + msg)
    }
    gen.writeEndArray()

    gen.writeEndObject()
    gen.flush()
  }
}

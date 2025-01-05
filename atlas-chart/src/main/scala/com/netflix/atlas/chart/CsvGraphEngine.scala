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
import java.time.Instant
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import com.netflix.atlas.chart.model.GraphDef

class CsvGraphEngine(val name: String, val contentType: String, sep: String) extends GraphEngine {

  def write(config: GraphDef, output: OutputStream): Unit = {
    val writer = new OutputStreamWriter(output, "UTF-8")
    val seriesList = config.plots.flatMap(_.lines)
    val count = seriesList.size
    val numberFmt = config.numberFormat
    writer.append("\"timestamp\"")
    (0 until count).zip(seriesList).map {
      case (_, series) =>
        val label = "\"%s\"".format(series.data.label)
        writer.append(sep).append(label)
    }
    writer.append("\n")
    val step = config.step
    val endTime = config.endTime.toEpochMilli
    var timestamp = config.startTime.toEpochMilli
    while (timestamp < endTime) {
      val t = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), config.timezone)
      writer.append(t.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))
      seriesList.foreach { series =>
        val v = series.data.data(timestamp)
        val vstr = numberFmt.format(v)
        writer.append(sep).append(vstr)
      }
      writer.append("\n")
      timestamp += step
    }
    writer.flush()
  }
}

/*
 * Copyright 2014-2026 Netflix, Inc.
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
import java.util.Locale
import java.util.regex.Pattern

import com.netflix.atlas.chart.model.GraphDef

class CsvGraphEngine(val name: String, val contentType: String, sep: String) extends GraphEngine {

  // Vertical whitespace: CR, LF, VT, FF, NEL, LS, and PS. Common readers treat each of
  // these as a line break.
  private val lineBreaks = Pattern.compile("\\v")

  /**
    * Escape a label for use within a quoted header field. Line breaks have already been
    * replaced so each row stays on a single line. By default, embedded quotes are doubled
    * (RFC 4180).
    */
  protected def escapeLabel(label: String): String = {
    label.replace("\"", "\"\"")
  }

  private def encodeLabel(label: String): String = {
    "\"" + escapeLabel(lineBreaks.matcher(label).replaceAll(" ")) + "\""
  }

  def write(config: GraphDef, output: OutputStream): Unit = {
    val writer = new OutputStreamWriter(output, "UTF-8")
    val seriesList = config.plots.flatMap(_.lines)
    val numberFmt = config.numberFormat
    writer.append("\"timestamp\"")
    seriesList.foreach { series =>
      writer.append(sep).append(encodeLabel(series.data.label))
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
        val vstr = numberFmt.formatLocal(Locale.ROOT, v)
        writer.append(sep).append(vstr)
      }
      writer.append("\n")
      timestamp += step
    }
    writer.flush()
  }
}

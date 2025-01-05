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
package com.netflix.atlas.eval.model

import com.fasterxml.jackson.core.JsonGenerator
import com.netflix.atlas.chart.model.LineStyle
import com.netflix.atlas.chart.model.Palette
import com.netflix.atlas.core.model.*
import com.netflix.atlas.core.util.Strings
import com.netflix.atlas.json.Json
import com.netflix.atlas.json.JsonSupport

import java.awt.Color
import java.time.Duration

/**
  * Message type use for emitting time series data in LWC and fetch responses.
  *
  * @param id
  *     Identifier for the time series. This can be used to stitch together messages for
  *     the same time series over time. For example in a streaming use-case you get one
  *     message per interval for each time series. To get all of the message for a given
  *     time series group by this id.
  * @param query
  *     Expression for the time series. Note, the same expression can result in many time
  *     series when using group by. For matching the data for a particular time series the
  *     id field should be used.
  * @param groupByKeys
  *     The final keys used for grouping the result. The value will be an empty list if
  *     the expression is not grouped. For multi-level group by this will be the final
  *     grouping used for the result.
  * @param start
  *     Start time for the data.
  * @param end
  *     End time for the data.
  * @param step
  *     Time interval between data points.
  * @param label
  *     Label associated with the time series. This is either the auto-generated string
  *     based on the expression or the value specified by the legend.
  * @param tags
  *     Tags associated with the final expression result. This is the set of exact matches
  *     from the query plus any keys used in the group by clause.
  * @param data
  *     Data for the time series.
  * @param styleMetadata
  *     Metadata for presentation details related to how to render the line.
  * @param samples
  *     Optional set of event samples associated with the message. Typically used when
  *     mapping events into a count with a few sample messages.
  */
case class TimeSeriesMessage(
  id: String,
  query: String,
  groupByKeys: List[String],
  start: Long,
  end: Long,
  step: Long,
  label: String,
  tags: Map[String, String],
  data: ChunkData,
  styleMetadata: Option[LineStyleMetadata],
  samples: List[List[Any]]
) extends JsonSupport {

  override def hasCustomEncoding: Boolean = true

  override def encode(gen: JsonGenerator): Unit = {
    gen.writeStartObject()
    gen.writeStringField("type", "timeseries")
    gen.writeStringField("id", id)
    gen.writeStringField("query", query)
    if (groupByKeys.nonEmpty) {
      gen.writeArrayFieldStart("groupByKeys")
      groupByKeys.foreach(gen.writeString)
      gen.writeEndArray()
    }
    gen.writeStringField("label", label)
    encodeTags(gen, tags)
    styleMetadata.foreach { metadata =>
      gen.writeNumberField("plot", metadata.plot)
      gen.writeStringField("color", Strings.zeroPad(metadata.color.getRGB, 8))
      gen.writeStringField("lineStyle", metadata.lineStyle.name())
      gen.writeNumberField("lineWidth", metadata.lineWidth)
    }
    gen.writeNumberField("start", start)
    gen.writeNumberField("end", end)
    gen.writeNumberField("step", step)
    gen.writeFieldName("data")
    data.encode(gen)
    if (samples.nonEmpty) {
      gen.writeFieldName("samples")
      Json.encode(gen, samples)
    }
    gen.writeEndObject()
  }

  private def encodeTags(gen: JsonGenerator, tags: Map[String, String]): Unit = {
    gen.writeObjectFieldStart("tags")
    tags.foreachEntry { (k, v) =>
      gen.writeStringField(k, v)
    }
    gen.writeEndObject()
  }
}

object TimeSeriesMessage {

  /**
    * Create a new time series message.
    *
    * @param expr
    *     Expression for the time series. Note, the same expression can result in many time
    *     series when using group by. For matching the data for a particular time series the
    *     id field should be used.
    * @param context
    *     Evaluation context that is used for getting the start, end, and step size used
    *     for the message.
    * @param ts
    *     Time series to use for the message.
    * @param palette
    *     If defined then include presentation metadata.
    * @param exprStr
    *     String view of the expression. Should match `expr`, but may be precomputed for
    *     group by expressions with many messages.
    */
  def apply(
    expr: StyleExpr,
    context: EvalContext,
    ts: TimeSeries,
    palette: Option[String] = None,
    exprStr: Option[String] = None
  ): TimeSeriesMessage = {
    val query = exprStr.getOrElse(expr.toString)
    val offset = Strings.toString(Duration.ofMillis(expr.offset))
    val outputTags = ts.tags + (TagKey.offset -> offset)
    val id = TaggedItem.computeId(outputTags + ("atlas.query" -> query)).toString
    val data = ts.data.bounded(context.start, context.end)
    TimeSeriesMessage(
      id,
      query,
      expr.expr.finalGrouping,
      context.start,
      context.end,
      context.step,
      ts.label,
      outputTags,
      ArrayData(data.data),
      palette.map(p => createStyleMetadata(expr, ts.label, p)),
      Nil
    )
  }

  private def createStyleMetadata(
    expr: StyleExpr,
    label: String,
    dfltPalette: String
  ): LineStyleMetadata = {
    val color = expr.color.fold(colorFromPalette(expr, label, dfltPalette))(Strings.parseColor)
    LineStyleMetadata(
      plot = expr.axis.getOrElse(0),
      color = color,
      lineStyle = expr.lineStyle.fold(LineStyle.LINE)(LineStyle.parse),
      lineWidth = expr.lineWidth
    )
  }

  private def colorFromPalette(expr: StyleExpr, label: String, dfltPalette: String): Color = {
    val palette = expr.palette.getOrElse(dfltPalette)
    Palette.create(palette).colors(label.hashCode)
  }
}

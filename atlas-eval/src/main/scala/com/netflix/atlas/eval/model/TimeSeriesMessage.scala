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
package com.netflix.atlas.eval.model

import java.time.Duration
import com.netflix.atlas.chart.model.LineStyle
import com.netflix.atlas.chart.model.Palette
import com.netflix.atlas.core.model.*
import com.netflix.atlas.core.util.Strings
import com.netflix.atlas.json3.Json
import com.netflix.atlas.json3.JsonSupport
import tools.jackson.core.JsonGenerator

/**
  * Message type use for emitting time series data in LWC and fetch responses.
  *
  * @param id
  *     Identifier for the time series. This can be used to stitch together messages for
  *     the same time series over time. For example in a streaming use-case you get one
  *     message per interval for each time series. To get all of the messages for a given
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
    gen.writeStringProperty("type", "timeseries")
    gen.writeStringProperty("id", id)
    gen.writeStringProperty("query", query)
    if (groupByKeys.nonEmpty) {
      gen.writeArrayPropertyStart("groupByKeys")
      groupByKeys.foreach(gen.writeString)
      gen.writeEndArray()
    }
    gen.writeStringProperty("label", label)
    encodeTags(gen, tags)
    styleMetadata.foreach { metadata =>
      gen.writeNumberProperty("plot", metadata.plot)
      gen.writeStringProperty("color", Strings.zeroPad(metadata.color.getRGB, 8))
      gen.writeStringProperty("lineStyle", metadata.lineStyle.name())
      gen.writeNumberProperty("lineWidth", metadata.lineWidth)
    }
    gen.writeNumberProperty("start", start)
    gen.writeNumberProperty("end", end)
    gen.writeNumberProperty("step", step)
    gen.writeName("data")
    data.encode(gen)
    if (samples.nonEmpty) {
      gen.writeName("samples")
      Json.encode(gen, samples)
    }
    gen.writeEndObject()
  }

  private def encodeTags(gen: JsonGenerator, tags: Map[String, String]): Unit = {
    gen.writeObjectPropertyStart("tags")
    tags.foreachEntry { (k, v) =>
      gen.writeStringProperty(k, v)
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
    *     If defined then include presentation metadata. This only supports the settings on
    *     the expression, use `GraphConfig.presentationStyle` with a `Factory` for style
    *     metadata that is consistent with the graph rendering.
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
    new Factory(expr, palette.map(p => createStyleFunction(expr, p)), exprStr).create(context, ts)
  }

  /**
    * Creates time series messages for an expression. The values that only depend on the
    * expression, such as the query string and the style metadata, are computed once. Use
    * this when creating many messages for the same expression, e.g. for each time series of
    * a group by or for each step of a stream.
    *
    * @param expr
    *     Expression for the time series.
    * @param style
    *     If defined then include presentation metadata. Maps the label of a line to the
    *     style, see `GraphConfig.presentationStyle`.
    * @param exprStr
    *     String view of the expression. Should match `expr`, if not specified it will be
    *     computed.
    */
  final class Factory(
    val expr: StyleExpr,
    style: Option[String => LineStyleMetadata],
    exprStr: Option[String] = None
  ) {

    private val query = exprStr.getOrElse(expr.toString)
    private val offset = Strings.toString(Duration.ofMillis(expr.offset))
    private val groupByKeys = expr.expr.finalGrouping

    /** Create a message for a time series from the expression. */
    def create(context: EvalContext, ts: TimeSeries): TimeSeriesMessage = {
      val outputTags = ts.tags + (TagKey.offset -> offset)
      val id = TaggedItem.computeId(outputTags + ("atlas.query" -> query)).toString
      val data = ts.data.bounded(context.start, context.end)
      TimeSeriesMessage(
        id,
        query,
        groupByKeys,
        context.start,
        context.end,
        context.step,
        ts.label,
        outputTags,
        ArrayData(data.data),
        style.map(_.apply(ts.label)),
        Nil
      )
    }
  }

  /** Style based only on the settings of the expression, used for `apply`. */
  private def createStyleFunction(
    expr: StyleExpr,
    dfltPalette: String
  ): String => LineStyleMetadata = {
    val plot = expr.axis.getOrElse(0)
    val lineStyle = expr.lineStyle.fold(LineStyle.LINE)(LineStyle.parse)
    val lineWidth = expr.lineWidth
    expr.color match {
      case Some(c) =>
        val metadata = LineStyleMetadata(plot, Strings.parseColor(c), lineStyle, lineWidth)
        _ => metadata
      case None =>
        val palette = Palette.create(expr.palette.getOrElse(dfltPalette).stripPrefix("hash:"))
        val template = LineStyleMetadata(plot, palette.colors(0), lineStyle, lineWidth)
        label => template.copy(color = palette.colors(label.hashCode))
    }
  }
}

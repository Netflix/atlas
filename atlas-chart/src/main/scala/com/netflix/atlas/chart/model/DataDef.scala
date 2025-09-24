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
package com.netflix.atlas.chart.model

import java.awt.Color
import java.time.Instant

import com.netflix.atlas.core.model.SummaryStats
import com.netflix.atlas.core.model.TimeSeries

/**
  * Defines data to show in the graph.
  */
sealed trait DataDef {

  def label: String

  def color: Color

  def withColor(c: Color): DataDef
}

/**
  * Defintion for a time series line.
  *
  * @param data
  *     Time series with the underlying data to render.
  * @param query
  *     Expression for the time series. Note, the same expression can result in many time
  *     series when using group by. For matching the data for a particular time series the
  *     id field should be used.
  * @param groupByKeys
  *     Set of keys used in the final grouping for the time series.
  * @param color
  *     Color to use when rendering the line.
  * @param lineStyle
  *     Style to use when rendering. Values are LINE, AREA, STACK, and VSPAN.
  * @param lineWidth
  *     Width of the stroke when rendering the line. Has no effect for styles other than LINE.
  * @param legendStats
  *     Summary stats for the data in the line.
  */
case class LineDef(
  data: TimeSeries,
  query: Option[String] = None,
  groupByKeys: List[String] = Nil,
  color: Color = Color.RED,
  lineStyle: LineStyle = LineStyle.LINE,
  lineWidth: Float = 1.0f,
  legendStats: SummaryStats = SummaryStats.empty
) extends DataDef {

  def label: String = data.label

  def withColor(c: Color): LineDef = copy(color = c)
}

/**
  * Defintion for a horizontal span.
  *
  * @param v1
  *     Starting value for the span.
  * @param v2
  *     Ending value for the span.
  * @param color
  *     Color to use when rendering the span.
  * @param labelOpt
  *     Label associated with the span to use in the legend.
  */
case class HSpanDef(v1: Double, v2: Double, color: Color, labelOpt: Option[String])
    extends DataDef {

  def label: String = labelOpt.getOrElse(s"span from $v1 to $v2")

  def withColor(c: Color): HSpanDef = copy(color = c)
}

/**
  * Defintion for a vertical span.
  *
  * @param t1
  *     Starting time for the span.
  * @param t2
  *     Ending time for the span.
  * @param color
  *     Color to use when rendering the span.
  * @param labelOpt
  *     Label associated with the span to use in the legend.
  */
case class VSpanDef(t1: Instant, t2: Instant, color: Color, labelOpt: Option[String])
    extends DataDef {

  def label: String = labelOpt.getOrElse(s"span from $t1 to $t2")

  def withColor(c: Color): VSpanDef = copy(color = c)
}

/**
  * Definition for a message that is included in the legend, but not displayed.
  *
  * @param label
  *     Label associated with the span to use in the legend.
  * @param color
  *     Color to use when rendering the text in the legend.
  */
case class MessageDef(label: String, color: Color = Color.BLACK) extends DataDef {

  def withColor(c: Color): MessageDef = copy(color = c)
}

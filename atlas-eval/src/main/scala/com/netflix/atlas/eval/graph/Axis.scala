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
package com.netflix.atlas.eval.graph

import com.netflix.atlas.chart.model.DataDef
import com.netflix.atlas.chart.model.HeatmapDef
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.Palette
import com.netflix.atlas.chart.model.PlotBound
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.chart.model.Scale
import com.netflix.atlas.chart.model.TickLabelMode
import com.netflix.atlas.chart.model.PlotBound.AutoStyle
import com.netflix.atlas.core.util.Strings

case class Axis(
  upper: Option[String] = None,
  lower: Option[String] = None,
  scale: Option[String] = None,
  stack: Boolean = false,
  ylabel: Option[String] = None,
  tickLabels: Option[String] = None,
  palette: Option[String] = None,
  sort: Option[String] = None,
  order: Option[String] = None,
  heatmapScale: Option[String] = None,
  heatmapUpper: Option[String] = None,
  heatmapLower: Option[String] = None,
  heatmapPalette: Option[String] = None,
  heatmapLabel: Option[String] = None
) {

  val tickLabelMode: TickLabelMode = {
    tickLabels.fold(TickLabelMode.DECIMAL)(TickLabelMode.apply)
  }

  private def getAxisTags(data: List[DataDef]): Map[String, String] = {
    val lines = data.collect { case line: LineDef => line.data.tags }
    if (lines.isEmpty)
      Map.empty
    else
      lines.reduce { (a, b) =>
        a.toSet.intersect(b.toSet).toMap
      }
  }

  def newPlotDef(data: List[DataDef] = Nil, multiY: Boolean = false): PlotDef = {
    val label = ylabel.map(s => Strings.substitute(s, getAxisTags(data)))
    val plot = PlotDef(
      data = data,
      lower = lower.fold[PlotBound](AutoStyle)(v => PlotBound(v)),
      upper = upper.fold[PlotBound](AutoStyle)(v => PlotBound(v)),
      ylabel = label,
      scale = Scale.fromName(scale.getOrElse("linear")),
      axisColor = if (multiY) data.headOption.map(_.color) else None,
      tickLabelMode = tickLabelMode
    )
    if (plot.heatmapLines.nonEmpty) {
      plot.copy(
        heatmap = Some(
          HeatmapDef(
            colorScale = Scale.fromName(heatmapScale.getOrElse("linear")),
            lower = heatmapLower.fold[PlotBound](AutoStyle)(v => PlotBound(v)),
            upper = heatmapUpper.fold[PlotBound](AutoStyle)(v => PlotBound(v)),
            palette = heatmapPalette.map(Palette.create),
            label = heatmapLabel
          )
        )
      )
    } else {
      plot
    }
  }
}

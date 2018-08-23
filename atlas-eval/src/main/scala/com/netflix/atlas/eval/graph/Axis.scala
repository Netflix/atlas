/*
 * Copyright 2014-2018 Netflix, Inc.
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

import java.awt.Color

import com.netflix.atlas.chart.model.DataDef
import com.netflix.atlas.chart.model.PlotBound
import com.netflix.atlas.chart.model.PlotBound.AutoStyle
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.chart.model.Scale
import com.netflix.atlas.chart.model.TickLabelMode

case class Axis(
  upper: Option[String] = None,
  lower: Option[String] = None,
  scale: Option[String] = None,
  stack: Boolean = false,
  ylabel: Option[String] = None,
  tickLabels: Option[String] = None,
  palette: Option[String] = None,
  sort: Option[String] = None,
  order: Option[String] = None
) {

  val tickLabelMode: TickLabelMode = {
    tickLabels.fold(TickLabelMode.DECIMAL)(TickLabelMode.apply)
  }

  def newPlotDef(data: List[DataDef] = Nil, multiY: Boolean = false): PlotDef = {
    PlotDef(
      data = data,
      lower = lower.fold[PlotBound](AutoStyle)(v => PlotBound(v)),
      upper = upper.fold[PlotBound](AutoStyle)(v => PlotBound(v)),
      ylabel = ylabel,
      scale = Scale.fromName(scale.getOrElse("linear")),
      axisColor = if (multiY) None else Some(Color.BLACK),
      tickLabelMode = tickLabelMode
    )
  }
}

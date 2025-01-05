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

import com.netflix.atlas.chart.model.Layout
import com.netflix.atlas.chart.model.VisionType

case class ImageFlags(
  title: Option[String],
  width: Int,
  height: Int,
  zoom: Double,
  axes: Map[Int, Axis],
  axisPerLine: Boolean,
  showLegend: Boolean,
  showLegendStats: Boolean,
  showOnlyGraph: Boolean,
  vision: VisionType,
  palette: String,
  theme: String,
  layout: Layout,
  hints: Set[String]
) {

  def presentationMetadataEnabled: Boolean = {
    hints.contains("presentation-metadata")
  }

  def axisPalette(settings: DefaultSettings, index: Int): String = {
    axes.get(index) match {
      case Some(axis) => axis.palette.getOrElse(settings.primaryPalette(theme))
      case None       => settings.primaryPalette(theme)
    }
  }

  def getAxis(index: Int): Axis = {
    axes.get(index) match {
      case Some(axis) => axis
      case None       => throw new IllegalArgumentException(s"invalid axis: $index")
    }
  }
}

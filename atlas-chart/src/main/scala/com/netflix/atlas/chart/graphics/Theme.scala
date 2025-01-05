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
package com.netflix.atlas.chart.graphics

import com.typesafe.config.Config

/**
  * Theme specifying the styles to use for the chart.
  *
  * @param image
  *     Styles for the overall image.
  * @param canvas
  *     Styles for the canvas area used to render the data of the chart.
  * @param minorGrid
  *     Styles used for rendering the minor grid lines.
  * @param majorGrid
  *     Styles used for rendering the major grid lines.
  * @param axis
  *     Styles used for rendering the axes.
  * @param legend
  *     Styles used for rendering the legend entries.
  * @param warnings
  *     Styles used for rendering warning messages.
  */
case class Theme(
  image: Styles,
  canvas: Styles,
  minorGrid: Styles,
  majorGrid: Styles,
  axis: Styles,
  legend: Styles,
  warnings: Styles
)

object Theme {

  def apply(config: Config): Theme = {
    Theme(
      image = Styles(config.getConfig("image")),
      canvas = Styles(config.getConfig("canvas")),
      minorGrid = Styles(config.getConfig("minor-grid")),
      majorGrid = Styles(config.getConfig("major-grid")),
      axis = Styles(config.getConfig("axis")),
      legend = Styles(config.getConfig("legend")),
      warnings = Styles(config.getConfig("warnings"))
    )
  }
}

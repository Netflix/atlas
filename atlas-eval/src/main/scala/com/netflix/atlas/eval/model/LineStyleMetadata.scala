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

import com.netflix.atlas.chart.model.LineStyle

import java.awt.Color

/**
  * Metadata for presentation details related to how to render the line.
  *
  * @param plot
  *     Identifies which axis the line should be associated with.
  * @param color
  *     Color to use for rendering the line.
  * @param lineStyle
  *     How to render the line (line, stack, area, etc).
  * @param lineWidth
  *     Width of the stroke when rendering the line.
  */
case class LineStyleMetadata(plot: Int, color: Color, lineStyle: LineStyle, lineWidth: Float) {

  require(color != null, "color cannot be null")
  require(lineStyle != null, "lineStyle cannot be null")
  require(lineWidth >= 0.0f, "lineWidth cannot be negative")
}

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

import com.netflix.iep.config.ConfigManager

object GraphConstants {

  private final val config = ConfigManager.dynamicConfig().getConfig("atlas.chart.limits")
  final val MaxYAxis = config.getInt("max-yaxes")
  final val MaxLinesInLegend = config.getInt("max-lines-in-legend")
  final val MinCanvasWidth = config.getInt("min-canvas-width")
  final val MinCanvasHeight = config.getInt("min-canvas-height")
  final val MaxWidth = config.getInt("max-width")
  final val MaxHeight = config.getInt("max-height")
  final val MaxZoom = config.getDouble("max-zoom")
}

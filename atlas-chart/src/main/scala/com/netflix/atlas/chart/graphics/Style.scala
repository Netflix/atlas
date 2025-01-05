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

import java.awt.BasicStroke
import java.awt.Color
import java.awt.Graphics2D
import java.awt.Stroke

import com.netflix.atlas.chart.Colors

/**
  * Style attributes associated with elements.
  *
  * @param color
  *     Color to set on the graphics object.
  * @param stroke
  *     Stroke to set on the graphics object.
  */
case class Style(color: Color = Color.BLACK, stroke: Stroke = new BasicStroke(1.0f)) {

  def configure(g: Graphics2D): Unit = {
    g.setColor(color)
    g.setStroke(stroke)
  }

  def withAlpha(alpha: Int): Style = {
    copy(color = Colors.withAlpha(color, alpha))
  }
}

object Style {
  val default: Style = Style()
}

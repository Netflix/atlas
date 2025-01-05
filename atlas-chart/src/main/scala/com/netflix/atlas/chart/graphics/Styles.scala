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
import java.awt.Stroke

import com.netflix.atlas.core.util.Strings
import com.typesafe.config.Config

/**
  * Set of styles for rendering an element on a chart.
  *
  * @param background
  *     Style used for the background fill.
  * @param line
  *     Style used to render lines.
  * @param text
  *     Style used to render text.
  */
case class Styles(background: Style, line: Style, text: Style)

object Styles {

  private val solidStroke: Stroke = {
    new BasicStroke(1.0f)
  }

  private val dashedStroke: Stroke = {
    new BasicStroke(
      1.0f,
      BasicStroke.CAP_BUTT,
      BasicStroke.JOIN_MITER,
      1.0f,
      Array(1.0f, 1.0f),
      0.0f
    )
  }

  def apply(config: Config): Styles = {
    Styles(
      loadStyle(config, "background"),
      loadStyle(config, "line"),
      loadStyle(config, "text")
    )
  }

  def loadStyle(config: Config, name: String): Style = {
    val color = Strings.parseColor(config.getString(s"$name-color"))
    val stroke = parseStroke(config.getString(s"$name-stroke"))
    Style(color, stroke)
  }

  def parseStroke(s: String): Stroke = {
    s match {
      case "dashed" => dashedStroke
      case "solid"  => solidStroke
      case _        => throw new IllegalArgumentException(s"unknown stroke: $s")
    }
  }
}

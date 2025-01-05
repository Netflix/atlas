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
import java.awt.Font
import java.awt.Stroke
import java.awt.image.BufferedImage
import java.util.concurrent.ConcurrentHashMap
import com.netflix.atlas.chart.util.Fonts
import com.netflix.iep.config.ConfigManager

import java.awt.Graphics2D

object ChartSettings {

  private val config = ConfigManager.dynamicConfig().getConfig("atlas.chart")

  val defaultTheme: String = config.getString(s"theme.default")

  private val themes = new ConcurrentHashMap[String, Theme]()

  def theme(name: String): Theme = {
    if (!config.hasPath(s"theme.$name")) {
      throw new IllegalArgumentException(s"invalid theme name: '$name'")
    } else {
      themes.computeIfAbsent(
        name,
        n => {
          val c = config.getConfig(s"theme.$n")
          Theme(c)
        }
      )
    }
  }

  /**
    * For some of the font operations a graphics context is needed. This is a simple dummy instance
    * that can be used for cases where we need to determine the size before the actual image object
    * is created.
    */
  val refImage = new BufferedImage(1, 1, BufferedImage.TYPE_INT_ARGB)
  val refGraphics: Graphics2D = refImage.createGraphics()

  /** Dashed stroke typically used for grid lines. */
  val dashedStroke: Stroke = {
    new BasicStroke(
      1.0f,
      BasicStroke.CAP_BUTT,
      BasicStroke.JOIN_MITER,
      1.0f,
      Array(1.0f, 1.0f),
      0.0f
    )
  }

  /**
    * Base monospaced font used for graphics. Monospace is used to make the layout easier.
    */
  val monospaceFont: Font = Fonts.loadFont(config.getString("fonts.monospace"))

  /** Small sized monospaced font. */
  val smallFont: Font = monospaceFont.deriveFont(10.0f)

  /** Normal sized monospaced font. */
  val normalFont: Font = monospaceFont

  /** Large sized monospaced font. */
  val largeFont: Font = monospaceFont.deriveFont(14.0f)

  /** Dimensions for a character using the small font. */
  val smallFontDims: Dimensions = dimensions(smallFont)

  /** Dimensions for a character using the normal font. */
  val normalFontDims: Dimensions = dimensions(normalFont)

  /** Dimensions for a character using the large font. */
  val largeFontDims: Dimensions = dimensions(largeFont)

  /**
    * Minimum width required for text elements. Value was chosen to allow typical messages to
    * display with a reasonable level of wrapping.
    */
  val minWidthForText: Int = smallFontDims.width * "Warnings: abcdef".length

  /**
    * Minimum width required for text elements. Value was chosen to allow the typical legend with
    * stats to show cleanly. It also keeps the cutoff below the level of sizes that are frequently
    * used in practice.
    */
  val minWidthForStats: Int = smallFontDims.width * 45

  /**
    * Determine the dimensions for a single character using `font`. It is assumed that the font
    * is monospaced.
    */
  def dimensions(font: Font): Dimensions = {
    refGraphics.setFont(font)
    val m = refGraphics.getFontMetrics
    Dimensions(m.stringWidth("X"), m.getHeight)
  }

  case class Dimensions(width: Int, height: Int)
}

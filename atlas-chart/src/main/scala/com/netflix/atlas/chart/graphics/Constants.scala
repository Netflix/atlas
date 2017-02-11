/*
 * Copyright 2014-2017 Netflix, Inc.
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
import java.awt.Font
import java.awt.Stroke
import java.awt.image.BufferedImage

import com.netflix.atlas.config.ConfigManager
import com.netflix.atlas.core.util.Strings
import com.typesafe.config.ConfigFactory

object Constants {

  private val config = ConfigManager.current.getConfig("atlas.chart")

  private def color(name: String): Color = Strings.parseColor(config.getString(s"colors.$name"))

  val backgroundColor = color("background")

  /**
   * For some of the font operations a graphics context is needed. This is a simple dummy instance
   * that can be used for cases where we need to determine the size before the actual image object
   * is created.
   */
  val refImage = new BufferedImage(1, 1, BufferedImage.TYPE_INT_ARGB)
  val refGraphics = refImage.createGraphics()

  /** Dashed stroke typically used for grid lines. */
  val dashedStroke: Stroke = {
    new BasicStroke(
      1.0f,
      BasicStroke.CAP_BUTT,
      BasicStroke.JOIN_MITER,
      1.0f,
      Array(1.0f, 1.0f),
      0.0f)
  }

  val minorGridColor = color("grid-minor")
  val minorGridStyle = Style(color = minorGridColor, stroke = dashedStroke)

  val majorGridColor = color("grid-major")
  val majorGridStyle = Style(color = majorGridColor, stroke = dashedStroke)

  // Try to avoid problems with different default fonts on various platforms. Java will use the
  // "Dialog" font by default which can get mapped differently on various systems. It looks like
  // passing a bad font name into the font constructor will just silently fall back to the
  // default so it should still function if this font isn't present. However, the lucida font
  // was chosen as it is expected to be widely available:
  // https://docs.oracle.com/javase/tutorial/2d/text/fonts.html
  val regularFont = new Font(config.getString("fonts.regular"), Font.PLAIN, 12)

  /**
   * Base monospaced font used for graphics. Monospace is used to make the layout easier.
   */
  val monospaceFont = new Font(config.getString("fonts.monospace"), Font.PLAIN, 12)

  /** Small sized monospaced font. */
  val smallFont = monospaceFont.deriveFont(10.0f)

  /** Normal sized monospaced font. */
  val normalFont = monospaceFont

  /** Large sized monospaced font. */
  val largeFont = monospaceFont.deriveFont(14.0f)

  /** Dimensions for a character using the small font. */
  val smallFontDims = dimensions(smallFont)

  /** Dimensions for a character using the normal font. */
  val normalFontDims = dimensions(normalFont)

  /** Dimensions for a character using the large font. */
  val largeFontDims = dimensions(largeFont)

  /**
   * Minimum width required for text elements. Value was chosen to allow typical messages to
   * display with a reasonable level of wrapping.
   */
  val minWidthForText = smallFontDims.width * "Warnings: abcdef".length

  /**
   * Minimum width required for text elements. Value was chosen to allow the typical legend with
   * stats to show cleanly. It also keeps the cutoff below the level of sizes that are frequently
   * used in practice.
   */
  val minWidthForStats = smallFontDims.width * 45

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

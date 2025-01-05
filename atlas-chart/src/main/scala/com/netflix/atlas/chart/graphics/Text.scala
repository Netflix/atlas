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

import java.awt.Font
import java.awt.Graphics2D
import java.awt.font.LineBreakMeasurer
import java.awt.font.TextAttribute
import java.text.AttributedString

/**
  * Draw text with a single font and simple alignment.
  *
  * @param str
  *     The text to display.
  * @param font
  *     Font to use when rendering.
  * @param alignment
  *     Basic alignment setting to use when laying out the text in the provided space. Defaults to
  *     center.
  * @param style
  *     Style to use for rendering the text.
  */
case class Text(
  str: String,
  font: Font = ChartSettings.normalFont,
  alignment: TextAlignment = TextAlignment.CENTER,
  style: Style = Style.default
) extends Element
    with VariableHeight {

  lazy val dims: ChartSettings.Dimensions = ChartSettings.dimensions(font)

  def truncate(width: Int): Text = {
    val maxChars = (width - Text.rightPadding) / dims.width
    if (str.length < maxChars) this
    else {
      if (maxChars < 5) copy(str = "")
      else {
        copy(str = str.substring(0, maxChars - 5) + "...")
      }
    }
  }

  override def minHeight: Int = dims.height

  override def computeHeight(g: Graphics2D, width: Int): Int = {
    val attrStr = new AttributedString(str)
    attrStr.addAttribute(TextAttribute.FONT, font)
    val iterator = attrStr.getIterator
    val measurer = new LineBreakMeasurer(iterator, g.getFontRenderContext)

    val wrap = width - Text.rightPadding
    var y = 0.0f
    while (measurer.getPosition < str.length) {
      val layout = measurer.nextLayout(wrap.toFloat)
      y += layout.getAscent + layout.getDescent + layout.getLeading
    }
    math.ceil(y).toInt
  }

  override def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    style.configure(g)

    val attrStr = new AttributedString(str)
    attrStr.addAttribute(TextAttribute.FONT, font)
    val iterator = attrStr.getIterator
    val measurer = new LineBreakMeasurer(iterator, g.getFontRenderContext)

    val width = x2 - x1
    val wrap = width - Text.rightPadding
    var y = y1.toFloat
    while (measurer.getPosition < str.length) {
      val layout = measurer.nextLayout(wrap.toFloat)
      y += layout.getAscent
      alignment match {
        case TextAlignment.LEFT =>
          layout.draw(g, x1 + 4.0f, y)
        case TextAlignment.RIGHT =>
          val rect = layout.getBounds
          val x = x1 + width - 4.0f - rect.getWidth.toFloat
          layout.draw(g, x, y)
        case TextAlignment.CENTER =>
          val rect = layout.getBounds
          val x = x1 + (width - rect.getWidth.toFloat) / 2
          layout.draw(g, x, y)
      }
      y += layout.getDescent + layout.getLeading
    }
  }
}

object Text {
  private val rightPadding = 8
}

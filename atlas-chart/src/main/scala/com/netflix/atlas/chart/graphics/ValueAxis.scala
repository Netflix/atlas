/*
 * Copyright 2015 Netflix, Inc.
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

import java.awt.Graphics2D

sealed trait ValueAxis extends Element with FixedWidth {

  import ValueAxis._

  override def width: Int = labelHeight + tickLabelWidth + tickMarkLength + 1

  def min: Double
  def max: Double

  def valueScale: Scales.DoubleFactory

  def scale(y1: Int, y2: Int): Scales.DoubleScale = valueScale(min, max, y1, y2)

  def ticks(y1: Int, y2: Int): List[ValueTick] = {
    val numTicks = (y2 - y1) / minTickLabelHeight
    Ticks.value(min, max, numTicks)
  }

  protected def angle: Double

  protected def drawLabel(text: Text, g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    val transform = g.getTransform
    val centerX = (x2 - x1) / 2 + x1
    val centerY = (y2 - y1) / 2 + y1

    val width = y2 - y1
    val truncated = text.truncate(width)
    val height = truncated.computeHeight(g, width)
    g.rotate(angle, centerX, centerY)
    truncated.draw(g, centerX - width / 2, centerY - height / 2, centerX + width / 2, centerY + height / 2)
    g.setTransform(transform)
  }
}

case class LeftValueAxis(
    min: Double,
    max: Double,
    label: Option[Text] = None,
    style: Style = Style.default,
    valueScale: Scales.DoubleFactory = Scales.ylinear) extends ValueAxis {

  import ValueAxis._

  protected def angle: Double = -Math.PI / 2.0

  def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {

    style.configure(g)
    g.drawLine(x2, y1, x2, y2)
    val yscale = scale(y1, y2)
    val majorTicks = ticks(y1, y2).filter(_.major)
    majorTicks.foreach { tick =>
      val py = yscale(tick.v)
      g.drawLine(x2, py, x2 - tickMarkLength, py)
      val txt = Text(tick.label, font = Constants.smallFont, alignment = TextAlignment.RIGHT, style = style)
      val txtH = Constants.smallFontDims.height
      val ty = py - txtH / 2
      txt.draw(g, x1, ty, x2 - tickMarkLength - 1, ty + txtH)
    }

    val offset = if (majorTicks.isEmpty) 0.0 else majorTicks.head.offset
    if (offset == 0.0) {
      label.foreach { t => drawLabel(t, g, x1, y1, x1 + labelHeight, y2) }
    } else {
      val offsetStr = s"[$offset+y]"
      val labelStr = label.fold(offsetStr)(t => s"$offsetStr ${t.str}")
      val labelTxt = Text(labelStr)
      drawLabel(labelTxt, g, x1, y1, x1 + labelHeight, y2)
    }
  }
}

case class RightValueAxis(
    min: Double,
    max: Double,
    label: Option[Text] = None,
    style: Style = Style.default,
    valueScale: Scales.DoubleFactory = Scales.ylinear) extends ValueAxis {

  import ValueAxis._

  protected def angle: Double = Math.PI / 2.0

  def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    style.configure(g)
    g.drawLine(x1, y1, x1, y2)
    val yscale = scale(y1, y2)
    val majorTicks = ticks(y1, y2).filter(_.major)
    majorTicks.foreach { tick =>
      val py = yscale(tick.v)
      g.drawLine(x1, py, x1 + tickMarkLength, py)
      val txt = Text(tick.label, font = Constants.smallFont, alignment = TextAlignment.LEFT, style = style)
      val txtH = Constants.smallFontDims.height
      val ty = py - txtH / 2
      txt.draw(g, x1 + tickMarkLength + 1, ty, x2, ty + txtH)
    }

    val offset = if (majorTicks.isEmpty) 0.0 else majorTicks.head.offset
    if (offset == 0.0) {
      label.foreach { t => drawLabel(t, g, x2 - labelHeight, y1, x2, y2) }
    } else {
      val offsetStr = s"[$offset+y]"
      val labelStr = label.fold(offsetStr)(t => s"$offsetStr ${t.str}")
      val labelTxt = Text(labelStr)
      drawLabel(labelTxt, g, x2 - labelHeight, y1, x2, y2)
    }
  }
}

object ValueAxis {

  val labelHeight = Constants.normalFontDims.height

  /**
   * Width of value tick labels. The assumption is a monospace font with 7 characters. The 7 is
   * for:
   *
   * - `[sign][3digits][decimal point][1digit][suffix]`: e.g., `-102.3K`
   * - `-1.0e-5`
   */
  val tickLabelWidth = Constants.smallFontDims.width * 7

  val tickMarkLength = 4

  val minTickLabelHeight = Constants.smallFontDims.height * 3
}


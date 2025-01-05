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

import java.awt.Color
import java.awt.Graphics2D
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.format.TextStyle
import java.util.Locale

/**
  * Draws a time based X-axis.
  *
  * @param style
  *     Style to use for the axis and corresponding labels.
  * @param start
  *     Start time in milliseconds since the epoch.
  * @param end
  *     End time in milliseconds since the epoch.
  * @param step
  *     Step size in milliseconds.
  * @param zone
  *     Time zone to use for the labels. This is a presentation detail only and can sometimes
  *     result in duplicates. For example, during a daylight savings transition the same hour
  *     can be used for multiple tick marks. Defaults to UTC.
  * @param alpha
  *     Alpha setting to use for the horizontal line of the axis. If the time axis is right next to
  *     the chart, then increasing the transparency can help make it easier to see lines that are
  *     right next to axis. Defaults to 40.
  * @param showZone
  *     If set to true, then the abbreviation for the time zone will be shown to the left of the
  *     axis labels.
  */
case class TimeAxis(
  style: Style,
  start: Long,
  end: Long,
  step: Long,
  zone: ZoneId = ZoneOffset.UTC,
  alpha: Int = 40,
  showZone: Boolean = true
) extends Element
    with FixedHeight {

  override def height: Int = 10 + ChartSettings.smallFontDims.height

  private val transition = {
    val s = Instant.ofEpochMilli(start)
    zone.getRules.nextTransition(s)
  }

  private val transitionTime = {
    if (transition == null) Long.MaxValue else transition.getInstant.toEpochMilli
  }

  def scale(p1: Int, p2: Int): Scales.LongScale = {
    Scales.time(start - step, end - step, step, p1, p2)
  }

  def ticks(x1: Int, x2: Int): List[TimeTick] = {

    // The first interval will displayed will end at the start time. For calculating ticks the
    // start time is adjusted so we can see minor ticks within the first interval
    val numTicks = (x2 - x1) / TimeAxis.minTickLabelWidth
    Ticks.time(start - step, end, zone, numTicks)
  }

  def draw(g: Graphics2D, x1: Int, y1: Int, x2: Int, y2: Int): Unit = {
    val txtH = ChartSettings.smallFontDims.height
    val labelPadding = TimeAxis.minTickLabelWidth / 2

    // Horizontal line across the bottom of the chart. The main horizontal line for the axis is
    // made faint so it is easier to see lines in the chart that are directly against the axis.
    style.withAlpha(alpha).configure(g)
    g.drawLine(x1, y1, x2, y1)

    style.configure(g)
    val xscale = scale(x1, x2)
    val majorTicks = ticks(x1, x2).filter(_.major)
    var indicatedTransition = false
    majorTicks.foreach { tick =>
      val px = xscale(tick.timestamp)
      if (px >= x1 && px <= x2) {
        // Vertical tick mark
        g.drawLine(px, y1, px, y1 + 4)

        // Label for the tick mark
        if (tick.timestamp >= transitionTime && !indicatedTransition) {
          indicatedTransition = true
          val before = transition.getOffsetBefore
          val after = transition.getOffsetAfter
          val delta = Duration.ofSeconds(after.getTotalSeconds - before.getTotalSeconds)
          val label = (if (delta.isNegative) "" else "+") + delta.toString.substring(2)
          val txt =
            Text(label, font = ChartSettings.smallFont, style = style.copy(color = Color.RED))
          txt.draw(g, px - labelPadding, y1 + txtH / 2, px + labelPadding, y1 + txtH)
        } else {
          val txt = Text(tick.label, font = ChartSettings.smallFont, style = style)
          txt.draw(g, px - labelPadding, y1 + txtH / 2, px + labelPadding, y1 + txtH)
        }
      }
    }

    // Show short form of time zone as a label for the axis
    if (showZone) {
      val name = zone.getDisplayName(TextStyle.NARROW_STANDALONE, Locale.US)
      val zoneLabel =
        Text(name, font = ChartSettings.smallFont, style = style, alignment = TextAlignment.RIGHT)
      val labelW = (name.length + 2) * ChartSettings.smallFontDims.width
      val padding = labelPadding + 2
      zoneLabel.draw(g, x1 - labelW - padding, y1 + txtH / 2, x1 - padding, y1 + txtH)
    }
  }
}

object TimeAxis {
  private val minTickLabelWidth = " 00:00 ".length * ChartSettings.smallFontDims.width
}

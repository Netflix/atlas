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

import com.netflix.atlas.chart.model.Scale

object Scales {

  type DoubleFactory = (Double, Double, Int, Int) => DoubleScale

  type DoubleScale = Double => Int
  type LongScale = Long => Int

  def factory(s: Scale): DoubleFactory = s match {
    case Scale.LINEAR      => ylinear
    case Scale.LOGARITHMIC => ylogarithmic
  }

  def linear(d1: Double, d2: Double, r1: Int, r2: Int): DoubleScale = {
    val pixelSpan = (d2 - d1) / (r2 - r1)
    v => ((v - d1) / pixelSpan).toInt + r1
  }

  def ylinear(d1: Double, d2: Double, r1: Int, r2: Int): DoubleScale = {
    val std = linear(d1, d2, r1, r2)
    v => r2 - std(v) + r1
  }

  private def log10(value: Double): Double = value match {
    case v if v > 0.0 => Math.log10(v)
    case v if v < 0.0 => -1.0 * Math.log10(-1.0 * v)
    case _            => 0.0
  }

  private def simpleLog(d1: Double, d2: Double, r1: Int, r2: Int): DoubleScale = {
    val pixelSpan = log10(d2 - d1) / (r2 - r1)
    v => (log10(v - d1) / pixelSpan).toInt + r1
  }

  private def negativeLog(d1: Double, d2: Double, r1: Int, r2: Int): DoubleScale = {
    val range = r2 - r1
    val lg = simpleLog(math.abs(d2), math.abs(d1), 0, range)
    v => r2 + lg(v)
  }

  def logarithmic(d1: Double, d2: Double, r1: Int, r2: Int): DoubleScale = {
    if (d1 >= 0.0) {
      simpleLog(d1, d2, r1, r2)
    } else if (d1 < 0.0 && d2 <= 0.0) {
      negativeLog(d1, d2, r1, r2)
    } else {
      val mid = (r2 - r1) / 2 + r1
      val pos = simpleLog(0.0, d2, mid, r2)
      val neg = negativeLog(d1, 0.0, r1, mid)
      v => if (v >= 0.0) pos(v) else neg(v)
    }
  }

  def ylogarithmic(d1: Double, d2: Double, r1: Int, r2: Int): DoubleScale = {
    val std = logarithmic(d1, d2, r1, r2)
    v => r2 - std(v) + r1
  }

  def time(d1: Double, d2: Double, step: Long, r1: Int, r2: Int): LongScale = {
    val dr = (d2 - d1) / step
    val pixelsPerStep = (r2 - r1) / dr
    v => ((v - d1) / step * pixelsPerStep).toInt + r1
  }
}

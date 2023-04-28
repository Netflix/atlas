/*
 * Copyright 2014-2023 Netflix, Inc.
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

/**
  * Helper functions for creating the scales to map data values and times to the
  * pixel location for the image.
  */
object Scales {

  /**
    * Factory for creating a value scale based on the min and max values for the input
    * data and the min and max pixel location.
    */
  type DoubleFactory = (Double, Double, Int, Int) => DoubleScale

  type InvertedFactory = (Double, Double, Int, Int) => InvertedScale

  /** Maps a double value to a pixel location. Typically used for the value scales. */
  type DoubleScale = Double => Int

  /** Maps a long value to a pixel location. Typically used for time scales. */
  type LongScale = Long => Int

  /** Maps a pixel location to the max value covered by that pixel. */
  type InvertedScale = Int => Double

  /** Returns the appropriate Y-value scale factory for the scale enum type. */
  def factory(s: Scale): DoubleFactory = s match {
    case Scale.LINEAR      => yscale(linear)
    case Scale.LOGARITHMIC => yscale(logarithmic)
    case Scale.POWER_2     => yscale(power(2.0))
    case Scale.SQRT        => yscale(power(0.5))
  }

  /** Factory for a linear mapping. */
  def linear(d1: Double, d2: Double, r1: Int, r2: Int): DoubleScale = {
    val pixelSpan = (d2 - d1) / (r2 - r1)
    v => ((v - d1) / pixelSpan).toInt + r1
  }

  private def log10(value: Double): Double = {
    value match {
      case v if v > 0.0 => math.log10(v + 1.0)
      case v if v < 0.0 => -math.log10(-(v - 1.0))
      case _            => 0.0
    }
  }

  /**
    * Factory for a logarithmic mapping. This is using logarithm for the purposes of
    * visualization, so `vizlog(0) == 0` and for `v < 0`, `vizlog(v) == -log(-v)`.
    */
  def logarithmic(d1: Double, d2: Double, r1: Int, r2: Int): DoubleScale = {
    val lg1 = log10(d1)
    val lg2 = log10(d2)
    val scale = linear(lg1, lg2, r1, r2)
    v => scale(log10(v))
  }

  private def pow(value: Double, exp: Double): Double = {
    value match {
      case v if v > 0.0 => math.pow(v, exp)
      case v if v < 0.0 => -math.pow(-v, exp)
      case _            => 0.0
    }
  }

  /** Factory for a power mapping. */
  def power(exp: Double)(d1: Double, d2: Double, r1: Int, r2: Int): DoubleScale = {
    val p1 = pow(d1, exp)
    val p2 = pow(d2, exp)
    val scale = linear(p1, p2, r1, r2)
    v => scale(pow(v, exp))
  }

  /**
    * Converts a value scale to what is needed for the Y-Axis. Takes into account that
    * the pixel coordinates increase in the opposite direction from the view needed for
    * showing to the user.
    */
  def yscale(s: DoubleFactory)(d1: Double, d2: Double, r1: Int, r2: Int): DoubleScale = {
    val std = s(d1, d2, r1, r2)
    v => r2 - std(v) + r1
  }

  /** Factory for creating a mapping for time values. */
  def time(t1: Long, t2: Long, step: Long, r1: Int, r2: Int): LongScale = {
    val d1 = t1.toDouble
    val d2 = t2.toDouble
    val dr = (d2 - d1) / step
    val pixelsPerStep = (r2 - r1) / dr
    v => ((v - d1) / step * pixelsPerStep).toInt + r1
  }

  /** Returns the appropriate inverted scale factory for the scale enum type. */
  def inverted(s: Scale): InvertedFactory = s match {
    case Scale.LINEAR      => invertedScale(invertedLinear)
    case Scale.LOGARITHMIC => invertedScale(invertedLogarithmic)
    case Scale.POWER_2     => invertedScale(invertedPower(2.0))
    case Scale.SQRT        => invertedScale(invertedPower(0.5))
  }

  /** Factory for an inverted linear mapping. */
  def invertedLinear(d1: Double, d2: Double, r1: Int, r2: Int): InvertedScale = {
    val pixelSpan = (d2 - d1) / (r2 - r1)
    v => (v - r1) * pixelSpan + d1
  }

  /** Invert the log10 operation. */
  private def pow10(value: Double): Double = {
    value match {
      case v if v > 0.0 => math.pow(10, v) + 1.0
      case v if v < 0.0 => -math.pow(10, -v) - 1.0
      case _            => 0.0
    }
  }

  /** Factory for an inverted logarithmic mapping. */
  def invertedLogarithmic(d1: Double, d2: Double, r1: Int, r2: Int): InvertedScale = {
    val lg1 = log10(d1)
    val lg2 = log10(d2)
    val scale = invertedLinear(lg1, lg2, r1, r2)
    v => pow10(scale(v))
  }

  /** Factory for an inverted power mapping. */
  def invertedPower(exp: Double)(d1: Double, d2: Double, r1: Int, r2: Int): InvertedScale = {
    val p1 = pow(d1, exp)
    val p2 = pow(d2, exp)
    val scale = invertedLinear(p1, p2, r1, r2)
    v => pow(scale(v), 1.0 / exp)
  }

  /**
    * Inverts the logic of the normal scales to map from a pixel location to a position on
    * the value range. This is typically used for finding ticks based on a discrete set like
    * a set of colors.
    */
  def invertedScale(s: InvertedFactory)(d1: Double, d2: Double, r1: Int, r2: Int): InvertedScale = {
    s(d1, d2, r1, r2)
  }
}

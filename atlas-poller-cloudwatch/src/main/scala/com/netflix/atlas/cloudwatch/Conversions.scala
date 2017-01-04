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
package com.netflix.atlas.cloudwatch

import com.amazonaws.services.cloudwatch.model.Datapoint
import com.amazonaws.services.cloudwatch.model.StandardUnit

/**
  * Helpers for creating conversion functions to extract the value from a cloudwatch
  * datapoint and normalize it to follow Atlas conventions.
  */
object Conversions {

  /**
    * Create a new conversion based on a simple comma separated list of known mappings.
    * The first element should be the statistic to extract. Allowed values are: `min`,
    * `max`, `sum`, `count`, and `avg`.
    *
    * This can optionally be followed by a rate conversion, e.g., `sum,rate`. The rate
    * will divide the value by the sample period from the cloudwatch metadata to get a
    * rate per second for the value. The rate conversion is unit aware, so if the unit
    * for the datapoint is already a rate, then no conversion will take place.
    *
    * In addition to the conversions specified by name, a unit conversion will automatically
    * be applied to the data. This ensures that a base unit is reported.
    */
  def fromName(name: String): Conversion = {
    val conversions = name.split(",").toList.reverse
    unit(from(name, conversions))
  }

  private def from(name: String, conversions: List[String]): Conversion = {
    conversions match {
      case "min"    :: Nil => min
      case "max"    :: Nil => max
      case "sum"    :: Nil => sum
      case "count"  :: Nil => count
      case "avg"    :: Nil => avg
      case "rate"   :: cs  => rate(from(name, cs))
      case v        :: cs  => throw new IllegalArgumentException(s"unknown conversion '$v' ($name)")
      case Nil             => throw new IllegalArgumentException(s"empty conversion list ($name)")
    }
  }

  private def min:   Conversion = (_, d) => d.getMinimum
  private def max:   Conversion = (_, d) => d.getMaximum
  private def sum:   Conversion = (_, d) => d.getSum
  private def count: Conversion = (_, d) => d.getSampleCount
  private def avg:   Conversion = (_, d) => d.getSum / d.getSampleCount

  private def unit(f: Conversion): Conversion = (m, d) => {
    val factor = unitFactor(d.getUnit)
    val newDatapoint = new Datapoint()
      .withMinimum(d.getMinimum * factor)
      .withMaximum(d.getMaximum * factor)
      .withSum(d.getSum * factor)
      .withSampleCount(d.getSampleCount)
      .withTimestamp(d.getTimestamp)
      .withUnit(d.getUnit)
    f(m, newDatapoint)
  }

  /**
    * Convert the result to a rate per second if it is not already. If the unit
    * for the datapoint indicates it is already a rate, then the value will not
    * be modified.
    */
  private def rate(f: Conversion): Conversion = (m, d) => {
    val v = f(m, d)
    val unit = d.getUnit
    if (unit.endsWith("/Second")) v else v / m.category.period
  }

  /** Modifies a conversion `f` to multiply the result by `v`. */
  def multiply(f: Conversion, v: Double): Conversion = (m, d) => f(m, d) * v

  /** Modifies a datapoint so that it has the specified unit. */
  def toUnit(f: Conversion, unit: StandardUnit): Conversion = (m, d) => {
    val newDatapoint = new Datapoint()
      .withMinimum(d.getMinimum)
      .withMaximum(d.getMaximum)
      .withSum(d.getSum)
      .withSampleCount(d.getSampleCount)
      .withTimestamp(d.getTimestamp)
      .withUnit(unit)
    f(m, newDatapoint)
  }

  private def unitFactor(unit: String): Double = {
    StandardUnit.fromValue(unit) match {
      case StandardUnit.Count           => 1.0

      //  Use a base unit of bytes
      case StandardUnit.Bits            => 1.0  / 8.0
      case StandardUnit.Kilobits        => 1e3  / 8.0
      case StandardUnit.Megabits        => 1e6  / 8.0
      case StandardUnit.Gigabits        => 1e9  / 8.0
      case StandardUnit.Terabits        => 1e12 / 8.0

      // Use a base unit of bytes. Assumes that the raw input is using
      // a decimal multiple, i.e., multiples of 1000 not 1024.
      case StandardUnit.Bytes           => 1.0
      case StandardUnit.Kilobytes       => 1e3
      case StandardUnit.Megabytes       => 1e6
      case StandardUnit.Gigabytes       => 1e9
      case StandardUnit.Terabytes       => 1e12

      case StandardUnit.CountSecond     => 1.0

      // Use base unit of bytes/second
      case StandardUnit.BitsSecond      => 1.0  / 8.0
      case StandardUnit.KilobitsSecond  => 1e3  / 8.0
      case StandardUnit.MegabitsSecond  => 1e6  / 8.0
      case StandardUnit.GigabitsSecond  => 1e9  / 8.0
      case StandardUnit.TerabitsSecond  => 1e12 / 8.0

      // Use base unit of bytes/second
      case StandardUnit.BytesSecond     => 1.0
      case StandardUnit.KilobytesSecond => 1e3
      case StandardUnit.MegabytesSecond => 1e6
      case StandardUnit.GigabytesSecond => 1e9
      case StandardUnit.TerabytesSecond => 1e12

      // Use base unit of seconds
      case StandardUnit.Microseconds    => 1e-6
      case StandardUnit.Milliseconds    => 1e-3
      case StandardUnit.Seconds         => 1.0

      case StandardUnit.Percent         => 1.0
      case StandardUnit.None            => 1.0
    }
  }

}

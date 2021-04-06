/*
 * Copyright 2014-2021 Netflix, Inc.
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

import software.amazon.awssdk.services.cloudwatch.model.Datapoint
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit

/**
  * Helpers for creating conversion functions to extract the value from a cloudwatch
  * datapoint and normalize it to follow Atlas conventions.
  */
object Conversions {

  /**
    * Determine the Atlas DS type based on the conversion. Anything that has a rate
    * conversion will use a rate, otherwise it will be treated as a gauge.
    */
  def determineDsType(name: String): String = {
    if (name.contains("rate")) "rate" else "gauge"
  }

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
      case "min" :: Nil    => min
      case "max" :: Nil    => max
      case "sum" :: Nil    => sum
      case "count" :: Nil  => count
      case "avg" :: Nil    => avg
      case "rate" :: cs    => rate(from(name, cs))
      case "percent" :: cs => multiply(from(name, cs), 100.0)
      case v :: cs         => throw new IllegalArgumentException(s"unknown conversion '$v' ($name)")
      case Nil             => throw new IllegalArgumentException(s"empty conversion list ($name)")
    }
  }

  private def min: Conversion = (_, d) => d.minimum

  private def max: Conversion = (_, d) => d.maximum

  private def sum: Conversion = (_, d) => d.sum

  private def count: Conversion = (_, d) => d.sampleCount

  private def avg: Conversion = (_, d) => d.sum / d.sampleCount

  private def unit(f: Conversion): Conversion = (m, d) => {
    val factor = unitFactor(d.unit)
    val newDatapoint = Datapoint
      .builder()
      .minimum(d.minimum * factor)
      .maximum(d.maximum * factor)
      .sum(d.sum * factor)
      .sampleCount(d.sampleCount)
      .timestamp(d.timestamp)
      .unit(d.unit)
      .build()
    f(m, newDatapoint)
  }

  /**
    * Convert the result to a rate per second if it is not already. If the unit
    * for the datapoint indicates it is already a rate, then the value will not
    * be modified.
    */
  private def rate(f: Conversion): Conversion = (m, d) => {
    val v = f(m, d)
    val unit = d.unitAsString()
    if (unit.endsWith("/Second")) v else v / m.category.period
  }

  /** Modifies a conversion `f` to multiply the result by `v`. */
  def multiply(f: Conversion, v: Double): Conversion = (m, d) => f(m, d) * v

  /** Modifies a datapoint so that it has the specified unit. */
  def toUnit(f: Conversion, unit: StandardUnit): Conversion = (m, d) => {
    val newDatapoint = Datapoint
      .builder()
      .minimum(d.minimum)
      .maximum(d.maximum)
      .sum(d.sum)
      .sampleCount(d.sampleCount)
      .timestamp(d.timestamp)
      .unit(unit)
      .build()
    f(m, newDatapoint)
  }

  private def unitFactor(unit: StandardUnit): Double = {
    unit match {
      case StandardUnit.COUNT => 1.0

      //  Use a base unit of bytes
      case StandardUnit.BITS     => 1.0 / 8.0
      case StandardUnit.KILOBITS => 1e3 / 8.0
      case StandardUnit.MEGABITS => 1e6 / 8.0
      case StandardUnit.GIGABITS => 1e9 / 8.0
      case StandardUnit.TERABITS => 1e12 / 8.0

      // Use a base unit of bytes. Assumes that the raw input is using
      // a decimal multiple, i.e., multiples of 1000 not 1024.
      case StandardUnit.BYTES     => 1.0
      case StandardUnit.KILOBYTES => 1e3
      case StandardUnit.MEGABYTES => 1e6
      case StandardUnit.GIGABYTES => 1e9
      case StandardUnit.TERABYTES => 1e12

      case StandardUnit.COUNT_SECOND => 1.0

      // Use base unit of bytes/second
      case StandardUnit.BITS_SECOND     => 1.0 / 8.0
      case StandardUnit.KILOBITS_SECOND => 1e3 / 8.0
      case StandardUnit.MEGABITS_SECOND => 1e6 / 8.0
      case StandardUnit.GIGABITS_SECOND => 1e9 / 8.0
      case StandardUnit.TERABITS_SECOND => 1e12 / 8.0

      // Use base unit of bytes/second
      case StandardUnit.BYTES_SECOND     => 1.0
      case StandardUnit.KILOBYTES_SECOND => 1e3
      case StandardUnit.MEGABYTES_SECOND => 1e6
      case StandardUnit.GIGABYTES_SECOND => 1e9
      case StandardUnit.TERABYTES_SECOND => 1e12

      // Use base unit of seconds
      case StandardUnit.MICROSECONDS => 1e-6
      case StandardUnit.MILLISECONDS => 1e-3
      case StandardUnit.SECONDS      => 1.0

      case StandardUnit.PERCENT => 1.0
      case StandardUnit.NONE    => 1.0
      case _                    => 1.0
    }
  }

}

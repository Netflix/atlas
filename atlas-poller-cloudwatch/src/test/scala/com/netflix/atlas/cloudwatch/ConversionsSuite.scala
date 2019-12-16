/*
 * Copyright 2014-2019 Netflix, Inc.
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

import java.util.Date

import com.amazonaws.services.cloudwatch.model.Datapoint
import com.amazonaws.services.cloudwatch.model.StandardUnit
import com.netflix.atlas.core.model.Query
import org.scalatest.funsuite.AnyFunSuite

class ConversionsSuite extends AnyFunSuite {

  private val dp = new Datapoint()
    .withMinimum(1.0)
    .withMaximum(5.0)
    .withSum(6.0)
    .withSampleCount(2.0)
    .withTimestamp(new Date)
    .withUnit(StandardUnit.None)

  private def newDatapoint(v: Double, unit: StandardUnit = StandardUnit.None): Datapoint = {
    new Datapoint()
      .withMinimum(v)
      .withMaximum(v)
      .withSum(v)
      .withSampleCount(1.0)
      .withTimestamp(new Date)
      .withUnit(unit)
  }

  test("min") {
    val cnv = Conversions.fromName("min")
    val v = cnv(null, dp)
    assert(v === 1.0)
  }

  test("max") {
    val cnv = Conversions.fromName("max")
    val v = cnv(null, dp)
    assert(v === 5.0)
  }

  test("sum") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, dp)
    assert(v === 6.0)
  }

  test("count") {
    val cnv = Conversions.fromName("count")
    val v = cnv(null, dp)
    assert(v === 2.0)
  }

  test("avg") {
    val cnv = Conversions.fromName("avg")
    val v = cnv(null, dp)
    assert(v === 3.0)
  }

  test("rate") {
    val cnv = Conversions.fromName("sum,rate")
    val meta = MetricMetadata(
      MetricCategory("NFLX/Test", 300, 1, 3, None, Nil, Nil, Query.True),
      MetricDefinition("test", "test-alias", cnv, false, Map.empty),
      Nil
    )
    val v = cnv(meta, dp)
    assert(v === 6.0 / 300.0)
  }

  test("rate already") {
    val cnv = Conversions.fromName("sum,rate")
    val meta = MetricMetadata(
      MetricCategory("NFLX/Test", 300, 1, 3, None, Nil, Nil, Query.True),
      MetricDefinition("test", "test-alias", cnv, false, Map.empty),
      Nil
    )
    val v = cnv(meta, newDatapoint(6.0, StandardUnit.BytesSecond))
    assert(v === 6.0)
  }

  test("bad conversion") {
    intercept[IllegalArgumentException] {
      Conversions.fromName("foo")
    }
  }

  test("empty conversion") {
    intercept[IllegalArgumentException] {
      Conversions.fromName("")
    }
  }

  test("missing conversion") {
    intercept[IllegalArgumentException] {
      Conversions.fromName("rate") // Rate must be used with another conversion
    }
  }

  test("unit count") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.Count))
    assert(v === 42.0)
  }

  test("unit bits") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.Bits))
    assert(v === 42.0 / 8.0)
  }

  test("unit kilobits") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.Kilobits))
    assert(v === 1e3 * 42.0 / 8.0)
  }

  test("unit megabits") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.Megabits))
    assert(v === 1e6 * 42.0 / 8.0)
  }

  test("unit gigabits") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.Gigabits))
    assert(v === 1e9 * 42.0 / 8.0)
  }

  test("unit terabits") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.Terabits))
    assert(v === 1e12 * 42.0 / 8.0)
  }

  test("unit bytes") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.Bytes))
    assert(v === 42.0)
  }

  test("unit kilobytes") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.Kilobytes))
    assert(v === 1e3 * 42.0)
  }

  test("unit megabytes") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.Megabytes))
    assert(v === 1e6 * 42.0)
  }

  test("unit gigabytes") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.Gigabytes))
    assert(v === 1e9 * 42.0)
  }

  test("unit terabytes") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.Terabytes))
    assert(v === 1e12 * 42.0)
  }

  test("unit bits/second") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.BitsSecond))
    assert(v === 42.0 / 8.0)
  }

  test("unit kilobits/second") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.KilobitsSecond))
    assert(v === 1e3 * 42.0 / 8.0)
  }

  test("unit megabits/second") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.MegabitsSecond))
    assert(v === 1e6 * 42.0 / 8.0)
  }

  test("unit gigabits/second") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.GigabitsSecond))
    assert(v === 1e9 * 42.0 / 8.0)
  }

  test("unit terabits/second") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.TerabitsSecond))
    assert(v === 1e12 * 42.0 / 8.0)
  }

  test("unit bytes/second") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.BytesSecond))
    assert(v === 42.0)
  }

  test("unit kilobytes/second") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.KilobytesSecond))
    assert(v === 1e3 * 42.0)
  }

  test("unit megabytes/second") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.MegabytesSecond))
    assert(v === 1e6 * 42.0)
  }

  test("unit gigabytes/second") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.GigabytesSecond))
    assert(v === 1e9 * 42.0)
  }

  test("unit terabytes/second") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.TerabytesSecond))
    assert(v === 1e12 * 42.0)
  }

  test("unit microseconds") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.Microseconds))
    assert(v === 1e-6 * 42.0)
  }

  test("unit milliseconds") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.Milliseconds))
    assert(v === 1e-3 * 42.0)
  }

  test("unit seconds") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.Seconds))
    assert(v === 42.0)
  }

  test("unit percent") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.Percent))
    assert(v === 42.0)
  }

  test("ratio to percent") {
    val cnv = Conversions.fromName("sum,percent")
    val v = cnv(null, newDatapoint(0.42, StandardUnit.Percent))
    assert(v === 42.0)
  }

  test("unit none") {
    val cnv = Conversions.fromName("sum")
    val v = cnv(null, newDatapoint(42.0, StandardUnit.None))
    assert(v === 42.0)
  }

  test("multiply") {
    val cnv = Conversions.multiply(Conversions.fromName("sum"), 100.0)
    val v = cnv(null, newDatapoint(42.0))
    assert(v === 4200.0)
  }

  test("dstype for max") {
    assert(Conversions.determineDsType("max") === "gauge")
  }

  test("dstype for sum") {
    assert(Conversions.determineDsType("sum") === "gauge")
  }

  test("dstype for count") {
    assert(Conversions.determineDsType("count") === "gauge")
  }

  test("dstype for sum,rate") {
    assert(Conversions.determineDsType("sum,rate") === "rate")
  }

  test("dstype for count,rate") {
    assert(Conversions.determineDsType("count,rate") === "rate")
  }
}

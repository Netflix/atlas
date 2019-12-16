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

import java.time.Duration
import java.time.Instant
import java.util.Date

import com.amazonaws.services.cloudwatch.model.Datapoint
import com.amazonaws.services.cloudwatch.model.StandardUnit
import com.netflix.atlas.cloudwatch.CloudWatchPoller.MetricData
import com.netflix.atlas.core.model.Query
import org.scalatest.funsuite.AnyFunSuite

class MetricDataSuite extends AnyFunSuite {

  private val definition =
    MetricDefinition("test", "alias", Conversions.fromName("sum"), false, Map.empty)
  private val category =
    MetricCategory("namespace", 60, 1, 5, None, List("dimension"), List(definition), Query.True)
  private val metadata = MetricMetadata(category, definition, Nil)

  private val monotonicMetadata = metadata.copy(definition = definition.copy(monotonicValue = true))

  private val metadataWithTimeout =
    metadata.copy(category = category.copy(timeout = Some(Duration.ofMinutes(2))))

  private def datapoint(v: Double, c: Double = 1.0): Option[Datapoint] = {
    val d = new Datapoint()
      .withMinimum(v)
      .withMaximum(v)
      .withSum(v * c)
      .withSampleCount(c)
      .withTimestamp(new Date())
      .withUnit(StandardUnit.None)
    Some(d)
  }

  test("access datapoint with no current value") {
    val data = MetricData(metadata, None, None, None)
    assert(data.datapoint().getSum === 0.0)
  }

  test("access datapoint with current value") {
    val data = MetricData(metadata, None, datapoint(1.0), None)
    assert(data.datapoint().getSum === 1.0)
  }

  test("category with timeout, first datapoint not yet received") {
    val now = Instant.now()
    val data = MetricData(metadataWithTimeout, None, None, None)
    assert(data.datapoint(now).getSum.isNaN)
  }

  test("category with timeout, datapoint with current value and not timed out") {
    val now = Instant.now()
    val data = MetricData(metadataWithTimeout, None, datapoint(1.0), Some(now.minusSeconds(60)))
    assert(data.datapoint(now).getSum === 1.0)
  }

  // current and timed out shouldn't be possible, but fail open by ensuring the current value is
  // reported
  test("category with timeout, datapoint with current value and timed out") {
    val now = Instant.now()
    val data = MetricData(metadataWithTimeout, None, datapoint(1.0), Some(now.minusSeconds(600)))
    assert(data.datapoint(now).getSum === 1.0)
  }

  test("category with timeout, datapoint with current and previous value and not timed out") {
    val now = Instant.now()
    val data =
      MetricData(metadataWithTimeout, datapoint(2.0), datapoint(1.0), Some(now.minusSeconds(60)))
    assert(data.datapoint(now).getSum === 1.0)
  }

  // current and timed out shouldn't be possible, but fail open by ensuring the current value is
  // reported
  test("category with timeout, datapoint with current and previous value and timed out") {
    val now = Instant.now()
    val data =
      MetricData(metadataWithTimeout, datapoint(2.0), datapoint(1.0), Some(now.minusSeconds(600)))
    assert(data.datapoint(now).getSum === 1.0)
  }

  test("category with timeout, datapoint with only previous value and not timed out") {
    val now = Instant.now()
    val data = MetricData(metadataWithTimeout, datapoint(1.0), None, Some(now.minusSeconds(60)))
    assert(data.datapoint(now).getSum === 0.0)
  }

  test("category with timeout, datapoint with only previous value and timed out") {
    val now = Instant.now()
    val data = MetricData(metadataWithTimeout, datapoint(1.0), None, Some(now.minusSeconds(600)))
    assert(data.datapoint(now).getSum.isNaN)
  }

  test("category with timeout, datapoint with no current or previous value and not timed out") {
    val now = Instant.now()
    val data = MetricData(metadataWithTimeout, None, None, Some(now.minusSeconds(60)))
    assert(data.datapoint(now).getSum === 0.0)
  }

  test("category with timeout, datapoint with no current or previous value and timed out") {
    val now = Instant.now()
    val data = MetricData(metadataWithTimeout, None, None, Some(now.minusSeconds(600)))
    assert(data.datapoint(now).getSum.isNaN)
  }

  test("access monotonic datapoint with no previous or current value") {
    val data = MetricData(monotonicMetadata, None, None, None)
    assert(data.datapoint().getSum.isNaN)
  }

  test("access monotonic datapoint with no current value") {
    val data = MetricData(monotonicMetadata, datapoint(1.0), None, None)
    assert(data.datapoint().getSum.isNaN)
  }

  test("access monotonic datapoint with no previous value") {
    val data = MetricData(monotonicMetadata, None, datapoint(1.0), None)
    assert(data.datapoint().getSum.isNaN)
  }

  test("access monotonic datapoint, current is larger") {
    val data = MetricData(monotonicMetadata, datapoint(1.0), datapoint(2.0), None)
    assert(data.datapoint().getSum === 1.0)
  }

  test("access monotonic datapoint, previous is larger") {
    val data = MetricData(monotonicMetadata, datapoint(2.0), datapoint(1.0), None)
    assert(data.datapoint().getSum === 0.0)
  }

  test("access monotonic datapoint, previous equals current") {
    val data = MetricData(monotonicMetadata, datapoint(1.0), datapoint(1.0), None)
    assert(data.datapoint().getSum === 0.0)
  }

  test("access monotonic datapoint, current is larger, previous dup") {
    val data = MetricData(monotonicMetadata, datapoint(1.0, 3), datapoint(2.0), None)
    assert(data.datapoint().getSum === 1.0)
  }
}

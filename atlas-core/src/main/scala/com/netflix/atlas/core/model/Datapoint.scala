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
package com.netflix.atlas.core.model

/**
  * Time series with a single value.
  *
  * @param tags
  *     Metadata for the identifying the datapoint.
  * @param timestamp
  *     Timestamp for the data point. The time is the end of an interval that
  *     starts at `timestamp - step`.
  * @param value
  *     Value for the interval.
  * @param step
  *     Step size for the datapoint. Defaults to the configured step size for the
  *     service.
  */
case class Datapoint(
  tags: Map[String, String],
  timestamp: Long,
  value: Double,
  step: Long = Datapoint.step
) extends TimeSeries
    with TimeSeq {

  require(tags != null, "tags cannot be null")
  require(timestamp >= 0L, s"invalid timestamp: $timestamp")

  def id: ItemId = TaggedItem.computeId(tags)

  def label: String = TimeSeries.toLabel(tags)

  def data: TimeSeq = this

  def dsType: DsType = DsType(tags)

  def apply(t: Long): Double = {
    if (t == timestamp) value else Double.NaN
  }

  def toTuple: DatapointTuple = {
    DatapointTuple(id, tags, timestamp, value)
  }
}

object Datapoint {
  private final val step = DefaultSettings.stepSize
}

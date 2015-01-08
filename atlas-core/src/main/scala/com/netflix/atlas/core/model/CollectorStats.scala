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
package com.netflix.atlas.core.model

object CollectorStats {
  val unknown = CollectorStats(-1L, -1L, -1L, -1L)

  def apply(vs: Iterable[CollectorStats]): CollectorStats = {
    val builder = new CollectorStatsBuilder
    vs.foreach(builder.update)
    builder.result
  }
}

/**
 * Summary stats for how much data was processed by a collector.
 *
 * @param inputLines        number of lines in the input to the collector
 * @param inputDatapoints   number of datapoints in the input to the collector
 * @param outputLines       number of lines in the result output
 * @param outputDatapoints  number of datapoints in the result output
 */
case class CollectorStats(
  inputLines: Long,
  inputDatapoints: Long,
  outputLines: Long,
  outputDatapoints: Long)

/** Helper for accumulating stats for a collector. */
class CollectorStatsBuilder {
  private var inputLines: Long = 0L
  private var inputDatapoints: Long = 0L
  private var outputLines: Long = 0L
  private var outputDatapoints: Long = 0L

  def updateInput(datapoints: Int) {
    inputLines += 1
    inputDatapoints += datapoints
  }

  def updateInput(blocks: List[Block]) {
    val size = blocks.size
    if (size > 0) {
      val b = blocks.head
      inputLines += 1
      inputDatapoints += b.size * size
    }
  }

  def updateOutput(datapoints: Int) {
    outputLines += 1
    outputDatapoints += datapoints
  }

  def update(s: CollectorStats) {
    if (s.inputLines >= 0) {
      inputLines += s.inputLines
      inputDatapoints += s.inputDatapoints
      outputLines += s.outputLines
      outputDatapoints += s.outputDatapoints
    }
  }

  def result: CollectorStats = {
    CollectorStats(inputLines, inputDatapoints, outputLines, outputDatapoints)
  }
}

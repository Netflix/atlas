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

sealed trait ConsolidationFunction {

  def name: String

  def compute(b: Block, pos: Int, aggr: Int, multiple: Int): Double

  override def toString: String = s":cf-$name"
}

object ConsolidationFunction {

  import java.lang.Double as JDouble

  sealed trait SumOrAvgCf extends ConsolidationFunction

  case object Avg extends SumOrAvgCf {

    def name: String = "avg"

    def compute(b: Block, pos: Int, aggr: Int, multiple: Int): Double = {
      val end = pos + multiple
      var total = 0.0
      var count = 0
      var nanCount = 0
      var i = pos
      while (i < end) {
        val v = b.get(i, aggr)
        if (!JDouble.isNaN(v)) {
          total += v
          count += 1
        } else {
          nanCount += 1
        }
        i += 1
      }
      if (count == 0) Double.NaN else total / (count + nanCount)
    }
  }

  case object Sum extends SumOrAvgCf {

    def name: String = "sum"

    def compute(b: Block, pos: Int, aggr: Int, multiple: Int): Double = {
      val end = pos + multiple
      var total = 0.0
      var count = 0
      var i = pos
      while (i < end) {
        val v = b.get(i, aggr)
        if (!JDouble.isNaN(v)) {
          total += v
          count += 1
        }
        i += 1
      }
      if (count == 0) Double.NaN else total
    }
  }

  case object Min extends ConsolidationFunction {

    def name: String = "min"

    def compute(b: Block, pos: Int, aggr: Int, multiple: Int): Double = {
      val end = pos + multiple
      var min = JDouble.MAX_VALUE
      var count = 0
      var i = pos
      while (i < end) {
        val v = b.get(i, aggr)
        if (!JDouble.isNaN(v)) {
          min = math.min(min, v)
          count += 1
        }
        i += 1
      }
      if (count == 0) Double.NaN else min
    }
  }

  case object Max extends ConsolidationFunction {

    def name: String = "max"

    def compute(b: Block, pos: Int, aggr: Int, multiple: Int): Double = {
      val end = pos + multiple
      var max = -JDouble.MAX_VALUE
      var count = 0
      var i = pos
      while (i < end) {
        val v = b.get(i, aggr)
        if (!JDouble.isNaN(v)) {
          max = math.max(max, v)
          count += 1
        }
        i += 1
      }
      if (count == 0) Double.NaN else max
    }
  }
}

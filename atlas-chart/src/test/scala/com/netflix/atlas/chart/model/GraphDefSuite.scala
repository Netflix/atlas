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
package com.netflix.atlas.chart.model

import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.FunctionTimeSeq
import com.netflix.atlas.core.model.TimeSeries
import munit.FunSuite

class GraphDefSuite extends FunSuite {

  val step = 60000

  def constant(v: Double): TimeSeries = {
    TimeSeries(Map("name" -> v.toString), new FunctionTimeSeq(DsType.Gauge, step, _ => v))
  }

  def constantSeriesDef(value: Double): LineDef = LineDef(constant(value))

  def label(vs: LineDef*): List[LineDef] = {
    vs.toList.zipWithIndex.map {
      case (v, i) =>
        v.copy(data = v.data.withLabel(i.toString), color = Palette.default.colors(i))
    }
  }

  def strip(expected: String): String = {
    expected.stripMargin.split("\n").mkString("")
  }

  test("to_from_json") {}
}

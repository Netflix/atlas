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
package com.netflix.atlas.chart

import java.time.ZoneOffset
import java.time.ZonedDateTime

import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.FunctionTimeSeq
import com.netflix.atlas.core.model.TimeSeries
import com.netflix.atlas.core.util.Streams
import munit.FunSuite

class JsonGraphEngineSuite extends FunSuite {

  val step = 60000

  def constant(v: Double): TimeSeries = {
    TimeSeries(Map("name" -> v.toString), new FunctionTimeSeq(DsType.Gauge, step, _ => v))
  }

  def constantSeriesDef(value: Double): LineDef = LineDef(constant(value))

  def label(vs: LineDef*): List[LineDef] = {
    vs.toList.zipWithIndex.map { case (v, i) => v.copy(data = v.data.withLabel(i.toString)) }
  }

  def strip(expected: String): String = {
    expected.stripMargin.split("\n").mkString("")
  }

  def process(engine: GraphEngine, expected: String): Unit = {
    val data = PlotDef(label(constantSeriesDef(42), constantSeriesDef(Double.NaN)))

    val graphDef = GraphDef(
      plots = List(data),
      startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant,
      endTime = ZonedDateTime.of(2012, 1, 1, 0, 3, 0, 0, ZoneOffset.UTC).toInstant
    )

    val bytes = Streams.byteArray { out =>
      engine.write(graphDef, out)
    }
    val json = new String(bytes, "UTF-8")
    assertEquals(json, strip(expected))
  }

  test("json") {
    val expected =
      """{
        |"start":1325376000000,
        |"step":60000,
        |"legend":["0","1"],
        |"metrics":[{"name":"42.0"},{"name":"NaN"}],
        |"values":[[42.0,"NaN"],[42.0,"NaN"],[42.0,"NaN"]],
        |"notices":[]
        |}"""

    process(new JsonGraphEngine, expected)
  }

  test("std.json") {
    val expected =
      """{
        |"start":1325376000000,
        |"step":60000,
        |"legend":["0","1"],
        |"metrics":[{"name":"42.0"},{"name":"NaN"}],
        |"values":[[42.0,"NaN"],[42.0,"NaN"],[42.0,"NaN"]],
        |"notices":[]
        |}"""

    process(new StdJsonGraphEngine, expected)
  }

}

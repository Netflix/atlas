/*
 * Copyright 2014-2026 Netflix, Inc.
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
import com.netflix.atlas.core.model.CollectorStats
import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.FunctionTimeSeq
import com.netflix.atlas.core.model.TimeSeries
import com.netflix.atlas.core.util.Streams
import munit.FunSuite

class JsonGraphEngineSuite extends FunSuite {

  val step = 60000

  def constant(v: Double, s: Long = step): TimeSeries = {
    TimeSeries(Map("name" -> v.toString), new FunctionTimeSeq(DsType.Gauge, s, _ => v))
  }

  def constantSeriesDef(value: Double, s: Long = step): LineDef = LineDef(constant(value, s))

  def label(vs: LineDef*): List[LineDef] = {
    vs.toList.zipWithIndex.map { case (v, i) => v.copy(data = v.data.withLabel(i.toString)) }
  }

  def strip(expected: String): String = {
    expected.stripMargin.replace("\r", "").split("\n").mkString("")
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

  test("stats.json") {
    val expected =
      """{
        |"start":1325376000000,
        |"end":1325376180000,
        |"step":60000,
        |"legend":["0","1"],
        |"metrics":[{"name":"42.0"},{"name":"NaN"}],
        |"stats":[{"count":3,"avg":42.0,"total":126.0,"max":42.0,"min":42.0,"last":42.0},{"count":0,"avg":"NaN","total":"NaN","max":"NaN","min":"NaN","last":"NaN"}],
        |"notices":[]
        |}"""

    process(new StatsJsonGraphEngine, expected)
  }

  test("json with explain stats") {
    val data = PlotDef(label(constantSeriesDef(42)))
    val graphDef = GraphDef(
      plots = List(data),
      startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant,
      endTime = ZonedDateTime.of(2012, 1, 1, 0, 3, 0, 0, ZoneOffset.UTC).toInstant,
      loadTime = 123,
      stats = CollectorStats(10, 5, 20, 10)
    )
    val bytes = Streams.byteArray { out =>
      new JsonGraphEngine().write(graphDef, out)
    }
    val json = new String(bytes, "UTF-8")
    val expected =
      """{
        |"start":1325376000000,
        |"step":60000,
        |"legend":["0"],
        |"metrics":[{"name":"42.0"}],
        |"values":[[42.0],[42.0],[42.0]],
        |"notices":[],
        |"explain":{"dataFetchTime":123,"inputLines":10,"intermediateLines":20,"graphLines":1,"inputDatapoints":5,"intermediateDatapoints":10,"graphDatapoints":4}
        |}"""
    assertEquals(json, strip(expected))
  }

  test("json subsecond step explain does not divide by zero") {
    val subsecondStep = 500L
    val data = PlotDef(label(constantSeriesDef(42, subsecondStep)))
    val start = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    val end = start.plusMillis(1000)
    val graphDef = GraphDef(
      plots = List(data),
      startTime = start,
      endTime = end,
      step = subsecondStep,
      loadTime = 50,
      stats = CollectorStats(1, 1, 2, 2)
    )
    val bytes = Streams.byteArray { out =>
      new JsonGraphEngine().write(graphDef, out)
    }
    val json = new String(bytes, "UTF-8")
    val expected =
      """{
        |"start":1325376000000,
        |"step":500,
        |"legend":["0"],
        |"metrics":[{"name":"42.0"}],
        |"values":[[42.0],[42.0]],
        |"notices":[],
        |"explain":{"dataFetchTime":50,"inputLines":1,"intermediateLines":2,"graphLines":1,"inputDatapoints":1,"intermediateDatapoints":2,"graphDatapoints":3}
        |}"""
    assertEquals(json, strip(expected))
  }

  test("json large step does not overflow") {
    val largeStep = 2592000000L // 30 days > Int.MaxValue millis
    val data = PlotDef(label(constantSeriesDef(42, largeStep)))
    val start = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    val end = start.plusMillis(largeStep * 2)
    val graphDef = GraphDef(
      plots = List(data),
      startTime = start,
      endTime = end,
      step = largeStep
    )
    val bytes = Streams.byteArray { out =>
      new JsonGraphEngine().write(graphDef, out)
    }
    val json = new String(bytes, "UTF-8")
    val expected =
      s"""{
        |"start":1325376000000,
        |"step":$largeStep,
        |"legend":["0"],
        |"metrics":[{"name":"42.0"}],
        |"values":[[42.0],[42.0]],
        |"notices":[]
        |}"""
    assertEquals(json, strip(expected))
  }
}

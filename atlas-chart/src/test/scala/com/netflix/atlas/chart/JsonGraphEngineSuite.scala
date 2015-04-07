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
package com.netflix.atlas.chart

import java.time.ZoneOffset
import java.time.ZonedDateTime

import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.FunctionTimeSeq
import com.netflix.atlas.core.model.TimeSeries
import com.netflix.atlas.core.util.Streams
import org.scalatest.FunSuite


class JsonGraphEngineSuite extends FunSuite {

  val step = 60000

  def constant(v: Double): TimeSeries = {
    TimeSeries(Map("name" -> v.toString), new FunctionTimeSeq(DsType.Gauge, step, _ => v))
  }

  def constantSeriesDef(value: Double) : SeriesDef = {
    val seriesDef = new SeriesDef
    seriesDef.data = constant(value)
    seriesDef
  }

  def label(vs: SeriesDef*): List[SeriesDef] = {
    vs.zipWithIndex.foreach { case (v, i) => v.label = i.toString }
    vs.toList
  }

  def strip(expected: String): String = {
    expected.stripMargin.split("\n").mkString("")
  }

  def process(engine: GraphEngine, expected: String) {
    val plotDef = new PlotDef
    plotDef.series = label(constantSeriesDef(42), constantSeriesDef(Double.NaN))

    val graphDef = new GraphDef
    graphDef.startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.endTime = ZonedDateTime.of(2012, 1, 1, 0, 3, 0, 0, ZoneOffset.UTC).toInstant
    graphDef.plots = List(plotDef)

    val bytes = Streams.byteArray { out => engine.write(graphDef, out) }
    val json = new String(bytes, "UTF-8")
    assert(json === strip(expected))
  }

  test("json") {
    val expected =
      """{
        |"start":1325376000000,
        |"step":60000,
        |"legend":["0","1"],
        |"metrics":[{},{}],
        |"values":[[42.000000,NaN],[42.000000,NaN],[42.000000,NaN],[42.000000,NaN]],
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
        |"metrics":[{},{}],
        |"values":[[42.000000,"NaN"],[42.000000,"NaN"],[42.000000,"NaN"],[42.000000,"NaN"]],
        |"notices":[]
        |}"""

    process(new StdJsonGraphEngine, expected)
  }

}

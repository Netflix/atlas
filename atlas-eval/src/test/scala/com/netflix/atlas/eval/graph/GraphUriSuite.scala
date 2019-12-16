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
package com.netflix.atlas.eval.graph

import akka.http.scaladsl.model.Uri
import com.netflix.atlas.chart.model.PlotBound
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.StyleExpr
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite

class GraphUriSuite extends AnyFunSuite {

  private val grapher = Grapher(ConfigFactory.load())

  private def parseUri(uri: String): GraphConfig = {
    grapher.toGraphConfig(Uri(uri))
  }

  test("simple expr") {
    val cfg = parseUri("/api/v1/graph?q=name,foo,:eq,:sum")
    assert(cfg.exprs === List(StyleExpr(DataExpr.Sum(Query.Equal("name", "foo")), Map.empty)))
  }

  test("empty title") {
    val cfg = parseUri("/api/v1/graph?q=name,foo,:eq,:sum&title=")
    assert(cfg.flags.title === None)
  }

  test("with title") {
    val cfg = parseUri("/api/v1/graph?q=name,foo,:eq,:sum&title=foo")
    assert(cfg.flags.title === Some("foo"))
  }

  test("empty ylabel") {
    val cfg = parseUri("/api/v1/graph?q=name,foo,:eq,:sum&ylabel=")
    assert(cfg.flags.axes(0).ylabel === None)
  }

  test("with ylabel") {
    val cfg = parseUri("/api/v1/graph?q=name,foo,:eq,:sum&ylabel=foo")
    assert(cfg.flags.axes(0).ylabel === Some("foo"))
  }

  test("empty ylabel.1") {
    val cfg = parseUri("/api/v1/graph?q=name,foo,:eq,:sum&ylabel.1=")
    assert(cfg.flags.axes(1).ylabel === None)
  }

  test("empty ylabel.1 with ylabel") {
    val cfg = parseUri("/api/v1/graph?q=name,foo,:eq,:sum&ylabel.1=&ylabel=foo")
    assert(cfg.flags.axes(1).ylabel === None)
  }

  test("lower bound") {
    val cfg = parseUri("/api/v1/graph?q=name,foo,:eq,:sum&l=0")
    assert(cfg.flags.axes(0).newPlotDef().lower === PlotBound.Explicit(0.0))
  }

  test("lower bound auto-data") {
    val cfg = parseUri("/api/v1/graph?q=name,foo,:eq,:sum&l=auto-data")
    assert(cfg.flags.axes(0).newPlotDef().lower === PlotBound.AutoData)
  }

  test("lower bound auto-style") {
    val cfg = parseUri("/api/v1/graph?q=name,foo,:eq,:sum&l=auto-style")
    assert(cfg.flags.axes(0).newPlotDef().lower === PlotBound.AutoStyle)
  }

  test("lower bound default") {
    val cfg = parseUri("/api/v1/graph?q=name,foo,:eq,:sum")
    assert(cfg.flags.axes(0).newPlotDef().lower === PlotBound.AutoStyle)
  }
}

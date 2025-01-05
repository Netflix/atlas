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
package com.netflix.atlas.eval.graph

import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.http.scaladsl.model.headers.Origin
import com.netflix.atlas.chart.model.PlotBound
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.StyleExpr
import com.typesafe.config.ConfigFactory
import munit.FunSuite

import java.util.UUID

class GraphUriSuite extends FunSuite {

  private val grapher = Grapher(ConfigFactory.load())

  private def parseUri(uri: String): GraphConfig = {
    grapher.toGraphConfig(Uri(uri))
  }

  test("simple expr") {
    val cfg = parseUri("/api/v1/graph?q=name,foo,:eq,:sum")
    assertEquals(cfg.exprs, List(StyleExpr(DataExpr.Sum(Query.Equal("name", "foo")), Map.empty)))
  }

  test("empty title") {
    val cfg = parseUri("/api/v1/graph?q=name,foo,:eq,:sum&title=")
    assertEquals(cfg.flags.title, None)
  }

  test("with title") {
    val cfg = parseUri("/api/v1/graph?q=name,foo,:eq,:sum&title=foo")
    assertEquals(cfg.flags.title, Some("foo"))
  }

  test("empty ylabel") {
    val cfg = parseUri("/api/v1/graph?q=name,foo,:eq,:sum&ylabel=")
    assertEquals(cfg.flags.axes(0).ylabel, None)
  }

  test("with ylabel") {
    val cfg = parseUri("/api/v1/graph?q=name,foo,:eq,:sum&ylabel=foo")
    assertEquals(cfg.flags.axes(0).ylabel, Some("foo"))
  }

  test("empty ylabel.1") {
    val cfg = parseUri("/api/v1/graph?q=name,foo,:eq,:sum&ylabel.1=")
    assertEquals(cfg.flags.axes(1).ylabel, None)
  }

  test("empty ylabel.1 with ylabel") {
    val cfg = parseUri("/api/v1/graph?q=name,foo,:eq,:sum&ylabel.1=&ylabel=foo")
    assertEquals(cfg.flags.axes(1).ylabel, None)
  }

  test("lower bound") {
    val cfg = parseUri("/api/v1/graph?q=name,foo,:eq,:sum&l=0")
    assertEquals(cfg.flags.axes(0).newPlotDef().lower, PlotBound.Explicit(0.0))
  }

  test("lower bound auto-data") {
    val cfg = parseUri("/api/v1/graph?q=name,foo,:eq,:sum&l=auto-data")
    assertEquals(cfg.flags.axes(0).newPlotDef().lower, PlotBound.AutoData)
  }

  test("lower bound auto-style") {
    val cfg = parseUri("/api/v1/graph?q=name,foo,:eq,:sum&l=auto-style")
    assertEquals(cfg.flags.axes(0).newPlotDef().lower, PlotBound.AutoStyle)
  }

  test("lower bound default") {
    val cfg = parseUri("/api/v1/graph?q=name,foo,:eq,:sum")
    assertEquals(cfg.flags.axes(0).newPlotDef().lower, PlotBound.AutoStyle)
  }

  test("lower bound default") {
    val cfg = parseUri("/api/v1/graph?q=name,foo,:eq,:sum")
    assertEquals(cfg.flags.axes(0).newPlotDef().lower, PlotBound.AutoStyle)
  }

  test("invalid theme") {
    val e = intercept[IllegalArgumentException] {
      parseUri("/api/v1/graph?q=name,foo,:eq,:sum&theme=foo")
    }
    assertEquals(e.getMessage, "invalid theme: foo")
  }

  test("hints: none") {
    val cfg = parseUri("/api/v1/graph?q=name,foo,:eq,:sum")
    assertEquals(cfg.flags.hints, Set.empty[String])
  }

  test("hints: empty") {
    val cfg = parseUri("/api/v1/graph?q=name,foo,:eq,:sum&hints=")
    assertEquals(cfg.flags.hints, Set.empty[String])
  }

  test("hints: single") {
    val cfg = parseUri("/api/v1/graph?q=name,foo,:eq,:sum&hints=a")
    assertEquals(cfg.flags.hints, Set("a"))
  }

  test("hints: multiple") {
    val cfg = parseUri("/api/v1/graph?q=name,foo,:eq,:sum&hints=a,b,c")
    assertEquals(cfg.flags.hints, Set("a", "b", "c"))
  }

  test("hints: multiple messy") {
    val cfg = parseUri("/api/v1/graph?q=name,foo,:eq,:sum&hints=a,b,%20a%20%20,b,b,b,c")
    assertEquals(cfg.flags.hints, Set("a", "b", "c"))
  }

  test("sanitize id") {
    val cfg = parseUri(s"/api/v1/graph?q=name,foo,:eq,:sum&id=${UUID.randomUUID()}")
    assertEquals(cfg.id, "default")
  }

  private def parseRequest(uri: String, origin: String): GraphConfig = {
    val request = HttpRequest(uri = Uri(uri), headers = List(Origin(origin)))
    grapher.toGraphConfig(request)
  }

  test("use CORS origin as default id") {
    val cfg = parseRequest("/api/v1/graph?q=name,foo,:eq,:sum", "http://foo.netflix.com")
    assertEquals(cfg.id, "foo.netflix.com")
  }

  test("explicit id parameter takes precedence over CORS origin") {
    val cfg = parseRequest("/api/v1/graph?q=name,foo,:eq,:sum&id=bar", "http://foo.netflix.com")
    assertEquals(cfg.id, "bar")
  }

  test("sanitize id from CORS origin") {
    val cfg = parseRequest(s"/api/v1/graph?q=name,foo,:eq,:sum", "http://[::1]:80")
    assertEquals(cfg.id, "default")
  }
}

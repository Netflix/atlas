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
package com.netflix.atlas.eval.graph

import com.netflix.atlas.chart.Colors
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.LineStyle
import com.netflix.atlas.chart.model.Palette
import com.netflix.atlas.core.model.ArrayTimeSeq
import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.ResultSet
import com.netflix.atlas.core.model.TimeSeries
import com.netflix.atlas.eval.model.LineStyleMetadata
import com.typesafe.config.ConfigFactory
import munit.FunSuite
import org.apache.pekko.http.scaladsl.model.Uri

import java.awt.Color

class PresentationStyleSuite extends FunSuite {

  private val grapher = Grapher(ConfigFactory.load())

  private val base = "/api/v1/graph?e=2012-01-01T00:00&s=e-5m&hints=presentation-metadata"

  private val byCluster = "name,sps,:eq,(,cluster,),:by"

  // The hash code for polygenelubricants is Int.MinValue
  private val labels = List("a", "b", "c", "polygenelubricants")

  private def config(params: String): GraphConfig = {
    grapher.toGraphConfig(Uri(s"$base&$params"))
  }

  private def style(params: String): String => LineStyleMetadata = {
    val cfg = config(params)
    cfg.presentationStyle(cfg.exprs.head).get
  }

  private def colors(params: String): List[Color] = {
    val f = style(params)
    labels.map(label => f(label).color)
  }

  private def hashColors(palette: String): List[Color] = {
    val p = Palette.create(palette)
    labels.map(label => p.colors(label.hashCode))
  }

  test("not enabled without the hint") {
    val cfg = grapher.toGraphConfig(Uri(s"/api/v1/graph?q=$byCluster"))
    assertEquals(cfg.presentationStyle(cfg.exprs.head), None)
  }

  test("default palette for the theme") {
    assertEquals(colors(s"q=$byCluster"), hashColors("armytage"))
    assertEquals(colors(s"q=$byCluster&theme=dark"), hashColors("light24"))
  }

  test("global palette") {
    assertEquals(colors(s"q=$byCluster&palette=reds"), hashColors("reds"))
  }

  test("axis palette") {
    assertEquals(colors(s"q=$byCluster&palette=reds&palette.0=blues"), hashColors("blues"))
  }

  test("expression palette") {
    assertEquals(colors(s"q=$byCluster,greens,:palette&palette=reds"), hashColors("greens"))
  }

  test("hash prefix is ignored") {
    assertEquals(colors(s"q=$byCluster,hash:greens,:palette"), hashColors("greens"))
    assertEquals(colors(s"q=$byCluster&palette=hash:reds"), hashColors("reds"))
  }

  test("offset uses offset palette for the theme") {
    assertEquals(colors(s"q=$byCluster,1h,:offset"), hashColors("bw"))
  }

  test("alpha is applied to palette colors") {
    val expected = hashColors("armytage").map(c => Colors.withAlpha(c, 0x40))
    assertEquals(colors(s"q=$byCluster,40,:alpha"), expected)
  }

  test("explicit color with alpha") {
    val expected = Colors.withAlpha(Color.RED, 0x40)
    assertEquals(colors(s"q=$byCluster,f00,:color,40,:alpha"), labels.map(_ => expected))
  }

  test("named color is resolved based on theme") {
    val cfg = config(s"q=$byCluster,blue1,:color")
    val expected = cfg.settings.resolveColor("light", "blue1")
    assertEquals(colors(s"q=$byCluster,blue1,:color"), labels.map(_ => expected))
  }

  test("line style defaults to stack for stacked axis") {
    assertEquals(style(s"q=$byCluster")("a").lineStyle, LineStyle.LINE)
    assertEquals(style(s"q=$byCluster&stack=1")("a").lineStyle, LineStyle.STACK)
    assertEquals(style(s"q=$byCluster,area,:ls&stack=1")("a").lineStyle, LineStyle.AREA)
  }

  test("heatmap uses single color for the axis") {
    val expected = Palette.create("armytage").colors("heatmap0".hashCode)
    assertEquals(colors(s"q=$byCluster,heatmap,:ls"), labels.map(_ => expected))
  }

  test("invalid palette fails when creating the style") {
    val cfg = config(s"q=$byCluster,bogus,:palette")
    intercept[Exception] {
      cfg.presentationStyle(cfg.exprs.head)
    }
  }

  test("invalid line width fails when creating the style") {
    val cfg = config(s"q=$byCluster,-1,:lw")
    intercept[IllegalArgumentException] {
      cfg.presentationStyle(cfg.exprs.head)
    }
  }

  test("invalid alpha fails when creating the style") {
    val cfg = config(s"q=$byCluster,4,:alpha")
    intercept[IllegalArgumentException] {
      cfg.presentationStyle(cfg.exprs.head)
    }
  }

  private def renderedLines(cfg: GraphConfig): List[LineDef] = {
    val gdef = grapher.create(
      cfg,
      s => {
        val data = labels.map { label =>
          val seq = new ArrayTimeSeq(DsType.Gauge, cfg.startMillis, cfg.stepSize, Array(1.0))
          TimeSeries(Map("name" -> "sps", "cluster" -> label), label, seq)
        }
        ResultSet(s.expr, data)
      }
    )
    gdef.plots.flatMap(_.lines)
  }

  // Palette colors only match the graph if the graph is also using hash selection
  private val consistencyCases = List(
    s"q=$byCluster&palette=hash:reds",
    s"q=$byCluster&palette=hash:reds&palette.0=hash:blues",
    s"q=$byCluster,hash:greens,:palette",
    s"q=$byCluster,40,:alpha&palette=hash:reds",
    s"q=$byCluster,f00,:color,40,:alpha",
    s"q=$byCluster,blue1,:color&theme=dark",
    s"q=$byCluster&palette=hash:reds&stack=1",
    s"q=$byCluster,heatmap,:ls&palette=hash:reds",
    s"q=$byCluster,heatmap,:ls,40,:alpha&heatmap_palette=reds",
    s"q=$byCluster,2,:lw,1,:axis&palette=hash:reds",
    s"q=$byCluster,40,:alpha&palette=hash:reds&vision=protanopia",
    s"q=$byCluster,f00,:color&vision=deuteranopia"
  )

  consistencyCases.foreach { params =>
    test(s"consistent with graph: $params") {
      val cfg = config(params)
      val f = cfg.presentationStyle(cfg.exprs.head).get
      val lines = renderedLines(cfg)
      assertEquals(lines.size, labels.size)
      lines.foreach { line =>
        val metadata = f(line.data.label)
        assertEquals(metadata.color, line.color, line.data.label)
        assertEquals(metadata.lineStyle, line.lineStyle, line.data.label)
        assertEquals(metadata.lineWidth, line.lineWidth, line.data.label)
      }
    }
  }
}

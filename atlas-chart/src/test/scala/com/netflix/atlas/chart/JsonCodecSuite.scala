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

import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.chart.model.Layout
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.core.model.TimeSeries
import munit.FunSuite

import java.io.ByteArrayInputStream
import java.time.Instant
import java.time.ZoneId

class JsonCodecSuite extends FunSuite {

  private def graphDef(hints: Set[String]): GraphDef = {
    val step = 60_000L
    GraphDef(
      plots = List(PlotDef(List(LineDef(TimeSeries.noData(step))))),
      startTime = Instant.ofEpochMilli(0L),
      endTime = Instant.ofEpochMilli(step),
      renderingHints = hints
    )
  }

  private def fromJson(json: String): GraphDef = {
    JsonCodec.decode(new ByteArrayInputStream(json.getBytes("UTF-8")))
  }

  test("decode - graph-metadata: required only") {
    val json =
      """
        |[
        |    {
        |        "type": "graph-metadata",
        |        "startTime": 0,
        |        "endTime": 60000,
        |        "timezones": [
        |            "Z"
        |        ],
        |        "step": 60000,
        |        "width": 400,
        |        "height": 200,
        |        "layout": "CANVAS",
        |        "zoom": 1,
        |        "legendType": "LABELS_WITH_STATS",
        |        "onlyGraph": false,
        |        "warnings": []
        |    }
        |]
        |""".stripMargin
    val gdef = fromJson(json)
    assertEquals(gdef.startTime.toEpochMilli, 0L)
    assertEquals(gdef.endTime.toEpochMilli, 60_000L)
    assertEquals(gdef.timezones, List(ZoneId.of("Z")))
    assertEquals(gdef.step, 60_000L)
    assertEquals(gdef.width, 400)
    assertEquals(gdef.height, 200)
    assertEquals(gdef.layout, Layout.CANVAS)
    assertEquals(gdef.zoom, 1.0)
    assertEquals(gdef.legendType.toString, "LABELS_WITH_STATS")
    assertEquals(gdef.onlyGraph, false)
    assertEquals(gdef.warnings, List.empty)
    assertEquals(gdef.themeName, "light")
  }

  test("rendering hint: none") {
    val gdef = graphDef(Set.empty)
    val str = JsonCodec.encode(gdef)
    assert(str.contains(""""type":"graph-image""""))
  }

  test("rendering hint: no-image") {
    val gdef = graphDef(Set("no-image"))
    val str = JsonCodec.encode(gdef)
    assert(!str.contains(""""type":"graph-image""""))
  }
}

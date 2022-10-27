/*
 * Copyright 2014-2022 Netflix, Inc.
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
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.core.model.TimeSeries
import munit.FunSuite

import java.time.Instant

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

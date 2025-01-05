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
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Threads
import org.openjdk.jmh.infra.Blackhole

/**
  * Check performance of graph engines.
  *
  * ```
  * > jmh:run -prof jmh.extras.JFR -wi 10 -i 10 -f1 -t1 .*GraphEngines.*
  * ...
  * [info] Benchmark                 Mode  Cnt       Score      Error  Units
  * [info] GraphEngines.json        thrpt   10  133984.991 ± 7703.042  ops/s
  * [info] GraphEngines.v2json      thrpt   10  142775.471 ± 6897.508  ops/s
  * ```
  */
@State(Scope.Thread)
class GraphEngines {

  val step = 60000

  def constant(v: Double): TimeSeries = {
    TimeSeries(Map("name" -> v.toString), new FunctionTimeSeq(DsType.Gauge, step, _ => v))
  }

  def constantSeriesDef(value: Double): LineDef = LineDef(constant(value))

  private val data = PlotDef(List(constantSeriesDef(42), constantSeriesDef(1.0 / 3.0)))

  private val graphDef = GraphDef(
    plots = List(data),
    startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant,
    endTime = ZonedDateTime.of(2012, 1, 1, 0, 12, 0, 0, ZoneOffset.UTC).toInstant
  )

  val json = new JsonGraphEngine()
  val v2json = new V2JsonGraphEngine()

  @Threads(1)
  @Benchmark
  def json(bh: Blackhole): Unit = {
    val bytes = Streams.byteArray { out =>
      json.write(graphDef, out)
    }
    bh.consume(bytes)
  }

  @Threads(1)
  @Benchmark
  def v2json(bh: Blackhole): Unit = {
    val bytes = Streams.byteArray { out =>
      v2json.write(graphDef, out)
    }
    bh.consume(bytes)
  }
}

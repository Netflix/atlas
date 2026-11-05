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
import java.util.regex.Pattern

import com.netflix.atlas.chart.model.GraphDef
import com.netflix.atlas.chart.model.LineDef
import com.netflix.atlas.chart.model.PlotDef
import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.FunctionTimeSeq
import com.netflix.atlas.core.model.TimeSeries
import com.netflix.atlas.core.util.Streams
import munit.FunSuite

class CsvGraphEngineSuite extends FunSuite {

  private val step = 60000

  private def line(v: Double, label: String): LineDef = {
    val ts = TimeSeries(Map("name" -> "test"), new FunctionTimeSeq(DsType.Gauge, step, _ => v))
    LineDef(ts.withLabel(label))
  }

  private def process(engine: GraphEngine, lines: List[LineDef]): String = {
    val graphDef = GraphDef(
      plots = List(PlotDef(lines)),
      startTime = ZonedDateTime.of(2012, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant,
      endTime = ZonedDateTime.of(2012, 1, 1, 0, 2, 0, 0, ZoneOffset.UTC).toInstant
    )
    val bytes = Streams.byteArray { out =>
      engine.write(graphDef, out)
    }
    new String(bytes, "UTF-8")
  }

  test("csv") {
    val expected =
      """"timestamp","a","b"
        |2012-01-01T00:00:00Z,42.000000,NaN
        |2012-01-01T00:01:00Z,42.000000,NaN
        |""".stripMargin
    val actual = process(new CommaSepGraphEngine, List(line(42.0, "a"), line(Double.NaN, "b")))
    assertEquals(actual, expected)
  }

  test("txt") {
    val expected =
      """"timestamp","a","b"
        |2012-01-01T00:00:00Z,42.000000,NaN
        |2012-01-01T00:01:00Z,42.000000,NaN
        |""".stripMargin.replace(",", "\t")
    val actual = process(new TabSepGraphEngine, List(line(42.0, "a"), line(Double.NaN, "b")))
    assertEquals(actual, expected)
  }

  private def assertColumnCounts(output: String, sep: String, expected: Int): Unit = {
    val lines = output.split("\n").toList
    assertEquals(lines.size, 3)
    lines.foreach { line =>
      assertEquals(line.split(Pattern.quote(sep), -1).length, expected, line)
    }
  }

  test("csv: quotes in label are escaped") {
    val actual = process(new CommaSepGraphEngine, List(line(1.0, "say \"hi\""), line(2.0, "b")))
    val header = actual.split("\n").head
    assertEquals(header, "\"timestamp\",\"say \"\"hi\"\"\",\"b\"")
  }

  test("csv: line breaks in label are replaced") {
    val actual = process(new CommaSepGraphEngine, List(line(1.0, "a\nb\r\nc"), line(2.0, "d")))
    val header = actual.split("\n").head
    assertEquals(header, "\"timestamp\",\"a b  c\",\"d\"")
    assertColumnCounts(actual, ",", 3)
  }

  test("csv: unicode line separators in label are replaced") {
    val label = "a\u0085b c d\u000be\ff"
    val actual = process(new CommaSepGraphEngine, List(line(1.0, label), line(2.0, "g")))
    val header = actual.split("\n").head
    assertEquals(header, "\"timestamp\",\"a b c d e f\",\"g\"")
  }

  test("csv: comma in label is kept in quoted field") {
    val actual = process(new CommaSepGraphEngine, List(line(1.0, "a,b"), line(2.0, "c")))
    val header = actual.split("\n").head
    assertEquals(header, "\"timestamp\",\"a,b\",\"c\"")
  }

  test("txt: quotes in label are escaped") {
    val actual = process(new TabSepGraphEngine, List(line(1.0, "say \"hi\""), line(2.0, "b")))
    val header = actual.split("\n").head
    assertEquals(header, "\"timestamp\"\t\"say \"\"hi\"\"\"\t\"b\"")
  }

  test("txt: tabs and line breaks in label are replaced") {
    val actual = process(new TabSepGraphEngine, List(line(1.0, "a\tb\nc"), line(2.0, "d")))
    val header = actual.split("\n").head
    assertEquals(header, "\"timestamp\"\t\"a b c\"\t\"d\"")
    assertColumnCounts(actual, "\t", 3)
  }
}

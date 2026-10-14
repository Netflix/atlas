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
package com.netflix.atlas.eval.model

import java.awt.Color
import com.netflix.atlas.chart.model.LineStyle
import com.netflix.atlas.chart.model.Palette
import com.netflix.atlas.core.model.ArrayTimeSeq
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.DsType
import com.netflix.atlas.core.model.EvalContext
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.model.TagKey
import com.netflix.atlas.core.model.TimeSeries
import munit.FunSuite

class TimeSeriesMessageSuite extends FunSuite {

  private val baseMessage = TimeSeriesMessage(
    id = "12345",
    query = "name,sps,:eq,(,cluster,),:by",
    groupByKeys = List("cluster"),
    start = 0L,
    end = 60000L,
    step = 60000L,
    label = "test",
    tags = Map("name" -> "sps", "cluster" -> "www"),
    data = ArrayData(Array(42.0)),
    None,
    Nil
  )

  test("json encoding with empty group by") {
    val input = baseMessage.copy(groupByKeys = Nil)
    assert(!input.toJson.contains("groupByKeys"))
  }

  test("json encoding with group by") {
    val input = baseMessage
    assert(input.toJson.contains(""""groupByKeys":["cluster"]"""))
  }

  private val context = EvalContext(0L, 120_000L, 60_000L)

  private def newTimeSeries(label: String): TimeSeries = {
    val data = new ArrayTimeSeq(DsType.Gauge, 0L, 60_000L, Array(1.0, 2.0))
    TimeSeries(Map("name" -> "sps", "cluster" -> label), label, data)
  }

  private def newStyleExpr(settings: Map[String, String]): StyleExpr = {
    val expr = DataExpr.GroupBy(DataExpr.Sum(Query.Equal("name", "sps")), List("cluster"))
    StyleExpr(expr, settings)
  }

  private val armytage = Palette.create("armytage")

  test("factory message fields") {
    val expr = newStyleExpr(Map.empty)
    val msg = new TimeSeriesMessage.Factory(expr, None).create(context, newTimeSeries("a"))
    assertEquals(msg.query, expr.toString)
    assertEquals(msg.groupByKeys, List("cluster"))
    assertEquals(msg.label, "a")
    assertEquals(msg.tags, Map("name" -> "sps", "cluster" -> "a", TagKey.offset -> "0w"))
    assertEquals(List(msg.start, msg.end, msg.step), List(0L, 120_000L, 60_000L))
    assertEquals(msg.data.asInstanceOf[ArrayData].values.toList, List(1.0, 2.0))
    assertEquals(msg.styleMetadata, None)
  }

  test("factory id depends on tags and query") {
    val factory = new TimeSeriesMessage.Factory(newStyleExpr(Map.empty), None)
    val a = factory.create(context, newTimeSeries("a"))
    assertEquals(factory.create(context, newTimeSeries("a")).id, a.id)
    assertNotEquals(factory.create(context, newTimeSeries("b")).id, a.id)
    val other = new TimeSeriesMessage.Factory(newStyleExpr(Map.empty), None, Some("foo"))
    assertNotEquals(other.create(context, newTimeSeries("a")).id, a.id)
  }

  test("factory uses style function with the label") {
    val style =
      (label: String) => LineStyleMetadata(0, armytage.colors(label.hashCode), LineStyle.LINE, 1.0f)
    val factory = new TimeSeriesMessage.Factory(newStyleExpr(Map.empty), Some(style))
    List("a", "b", "c").foreach { label =>
      val msg = factory.create(context, newTimeSeries(label))
      assertEquals(msg.styleMetadata, Some(style(label)))
    }
  }

  test("factory uses precomputed expression string") {
    val factory = new TimeSeriesMessage.Factory(newStyleExpr(Map.empty), None, Some("foo"))
    assertEquals(factory.create(context, newTimeSeries("a")).query, "foo")
  }

  test("apply with palette") {
    // The hash code for polygenelubricants is Int.MinValue
    List("a", "b", "polygenelubricants").foreach { label =>
      val msg =
        TimeSeriesMessage(newStyleExpr(Map.empty), context, newTimeSeries(label), Some("armytage"))
      val expected = LineStyleMetadata(0, armytage.colors(label.hashCode), LineStyle.LINE, 1.0f)
      assertEquals(msg.styleMetadata, Some(expected))
    }
  }

  test("apply with explicit color") {
    val expr = newStyleExpr(Map("color" -> "f00"))
    val msg = TimeSeriesMessage(expr, context, newTimeSeries("a"), Some("armytage"))
    assertEquals(msg.styleMetadata.map(_.color), Some(Color.RED))
  }
}

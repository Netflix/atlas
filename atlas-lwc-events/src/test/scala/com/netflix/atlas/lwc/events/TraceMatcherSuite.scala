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
package com.netflix.atlas.lwc.events

import com.fasterxml.jackson.core.JsonGenerator
import com.netflix.atlas.core.model.TraceQuery
import com.netflix.atlas.json.Json
import munit.FunSuite

import scala.util.Random

class TraceMatcherSuite extends FunSuite {

  import TraceMatcherSuite.*

  private def mkSpan(
    app: String,
    spanId: String,
    parentId: String,
    start: Long,
    duration: Long
  ): TestSpan = {
    TestSpan(
      Map("app" -> app),
      spanId,
      parentId,
      start,
      duration
    )
  }

  /**
    *  a - b
    *    - c - e - g
    *        - f
    *    - d
    */
  private val sampleTrace = List(
    mkSpan("a", "1", null, 0L, 100L),
    mkSpan("b", "2", "1", 1L, 25L),
    mkSpan("c", "3", "1", 5L, 90L),
    mkSpan("d", "4", "1", 15L, 50L),
    mkSpan("e", "5", "3", 8L, 50L),
    mkSpan("f", "6", "3", 55L, 20L),
    mkSpan("g", "7", "5", 4L, 5L)
  )

  private def checkMatch(query: TraceQuery, shouldMatch: Boolean): Unit = {
    assertEquals(TraceMatcher.matches(query, sampleTrace), shouldMatch)
    assertEquals(TraceMatcher.matches(query, sampleTrace.reverse), shouldMatch)
    assertEquals(TraceMatcher.matches(query, Random.shuffle(sampleTrace)), shouldMatch)
  }

  test("simple") {
    checkMatch(ExprUtils.parseTraceEventsQuery("app,a,:eq").q, true)
    checkMatch(ExprUtils.parseTraceEventsQuery("app,z,:eq").q, false)
  }

  test("span-and") {
    val q1 = ExprUtils.parseTraceEventsQuery("app,a,:eq,app,e,:eq,:span-and").q
    checkMatch(q1, true)

    val q2 = ExprUtils.parseTraceEventsQuery("app,a,:eq,app,z,:eq,:span-and").q
    checkMatch(q2, false)
  }

  test("span-or") {
    val q1 = ExprUtils.parseTraceEventsQuery("app,a,:eq,app,e,:eq,:span-or").q
    checkMatch(q1, true)

    val q2 = ExprUtils.parseTraceEventsQuery("app,a,:eq,app,z,:eq,:span-or").q
    checkMatch(q2, true)

    val q3 = ExprUtils.parseTraceEventsQuery("app,y,:eq,app,z,:eq,:span-or").q
    checkMatch(q3, false)
  }

  test("child") {
    val q1 = ExprUtils.parseTraceEventsQuery("app,a,:eq,app,c,:eq,:child").q
    checkMatch(q1, true)

    val q2 = ExprUtils.parseTraceEventsQuery("app,c,:eq,app,e,:eq,:child").q
    checkMatch(q2, true)

    val q3 = ExprUtils.parseTraceEventsQuery("app,a,:eq,app,e,:eq,:child").q
    checkMatch(q3, false)
  }

  test("transitive child") {
    val q1 = ExprUtils.parseTraceEventsQuery("app,a,:eq,app,c,:eq,:child,app,e,:eq,:child").q
    checkMatch(q1, true)

    val q2 = ExprUtils.parseTraceEventsQuery("app,a,:eq,app,b,:eq,:child,app,e,:eq,:child").q
    checkMatch(q2, false)

    val q3 = ExprUtils
      .parseTraceEventsQuery("app,a,:eq,app,c,:eq,:child,app,e,:eq,:child,app,g,:eq,:child")
      .q
    checkMatch(q3, true)
  }
}

object TraceMatcherSuite {

  case class TestSpan(
    tags: Map[String, String],
    spanId: String,
    parentId: String,
    timestamp: Long,
    duration: Long
  ) extends LwcEvent.Span {

    override def rawEvent: Any = this

    override def extractValue(key: String): Any = tags.getOrElse(key, null)

    override def encode(gen: JsonGenerator): Unit = {
      Json.encode(gen, this)
    }

    override def encodeAsRow(columns: List[String], gen: JsonGenerator): Unit = {
      throw new UnsupportedOperationException()
    }
  }
}

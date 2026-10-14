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

import com.typesafe.config.ConfigFactory
import munit.FunSuite
import org.apache.pekko.http.scaladsl.model.Uri

import java.time.Instant

class GraphConfigSuite extends FunSuite {

  private val grapher = new Grapher(DefaultSettings(ConfigFactory.load("graph-config-test.conf")))

  private def newGraphConfig(s: String, e: String, step: Long): GraphConfig = {
    val uri = s"http://localhost/api/v1/graph?q=name,sps,:eq,:sum&s=$s&e=$e&step=${step}ms"
    grapher.toGraphConfig(Uri(uri))
  }

  private def newGraphConfig(s: Long, e: Long, step: Long): GraphConfig = {
    newGraphConfig(s.toString, e.toString, step)
  }

  private val oneMinute = 60_000
  private val end = System.currentTimeMillis() / oneMinute * oneMinute
  private val start = end - 30 * oneMinute

  test("increase step, minute boundary") {
    val newStep = oneMinute
    val c1 = newGraphConfig(start, end, 5_000)
    val c2 = c1.withStep(newStep)
    val expected = newGraphConfig(start, end, newStep)
    assertEquals(c2.evalContext, expected.evalContext)
    assertEquals(c1.evalContext.increaseStep(newStep), expected.evalContext)
  }

  test("increase step, boundary change") {
    val newStep = oneMinute
    val s = start + 5_000
    val e = end + 5_000
    val c1 = newGraphConfig(s, e, 5_000)
    val c2 = c1.withStep(newStep)
    val expected = newGraphConfig(s, e, newStep)
    assertEquals(c2.evalContext, expected.evalContext)
    assertEquals(c1.evalContext.increaseStep(newStep), expected.evalContext)
  }

  test("increase step, boundary change 15s") {
    val newStep = 15_000
    val s = start + 5_000
    val e = end + 5_000
    val c1 = newGraphConfig(s, e, 5_000)
    val c2 = c1.withStep(newStep)
    val expected = newGraphConfig(s, e, newStep)
    assertEquals(c2.evalContext, expected.evalContext)
    assertEquals(c1.evalContext.increaseStep(newStep), expected.evalContext)
  }

  test("increase step, boundary change 20s") {
    val newStep = 20_000
    val s = start - 5_000
    val e = end - 5_000
    val c1 = newGraphConfig(s, e, 5_000)
    val c2 = c1.withStep(newStep)
    val expected = newGraphConfig(s, e, newStep)
    assertEquals(c2.evalContext, expected.evalContext)
    assertEquals(c1.evalContext.increaseStep(newStep), expected.evalContext)
  }

  private def checkIncreaseStep(s: String, e: String): Unit = {
    val c1 = newGraphConfig(s, e, 5_000)
    val c2 = c1.withStep(oneMinute)
    val expected = newGraphConfig(s, e, oneMinute)
    assertEquals(c2.evalContext, expected.evalContext)
    assertEquals(c1.evalContext.increaseStep(oneMinute), expected.evalContext)
  }

  test("increase step, before epoch") {
    checkIncreaseStep("1960-01-01T00:00Z", "1960-01-01T00:30Z")
  }

  test("increase step, before epoch, not on step boundary") {
    // Times before the epoch should round down to the step boundary like other times
    checkIncreaseStep("1959-12-31T23:59:53Z", "1960-01-01T00:30:07Z")
  }

  test("round to step, before epoch") {
    val c = newGraphConfig("1959-12-31T23:59:53Z", "1960-01-01T00:30:07Z", 5_000)
    assertEquals(c.fstart, Instant.parse("1959-12-31T23:59:50Z"))
    assertEquals(c.fend, Instant.parse("1960-01-01T00:30:05Z"))
  }

  test("increase step, near epoch") {
    // Timestamp in millis is small enough that it would be treated as seconds
    checkIncreaseStep("1970-01-02T00:00Z", "1970-01-02T00:30Z")
  }

  test("increase step, far future") {
    // Timestamp in millis is large enough that it would be treated as micros
    checkIncreaseStep("2500-01-01T00:00Z", "2500-01-01T00:30Z")
  }
}

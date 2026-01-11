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
package com.netflix.atlas.core.db

import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.DataVocabulary
import com.netflix.atlas.core.model.DefaultSettings
import com.netflix.atlas.core.model.EvalContext
import com.netflix.atlas.core.model.ItemIdCalculator
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.spectator.api.DefaultRegistry
import com.netflix.spectator.api.ManualClock
import com.typesafe.config.ConfigFactory
import munit.FunSuite

class BlockBoundarySuite extends FunSuite {

  private val interpreter = new Interpreter(DataVocabulary.allWords)

  private val step = DefaultSettings.stepSize

  private val clock = new ManualClock()
  private val registry = new DefaultRegistry(clock)

  private var db: MemoryDatabase = _

  override def beforeEach(context: BeforeEach): Unit = {
    clock.setWallTime(0L)

    db = new MemoryDatabase(
      registry,
      ConfigFactory.parseString("""
          |block-size = 60
          |num-blocks = 3
          |rebuild-frequency = 10s
          |test-mode = true
          |intern-while-building = true
        """.stripMargin)
    )
  }

  private def parseExpr(str: String): DataExpr = {
    interpreter.execute(str).stack match {
      case (expr: DataExpr) :: Nil => expr
      case _                       => throw new MatchError(s"invalid expr: $str")
    }
  }

  test("late arriving datapoint at block boundary when currentPos wraps to 0") {
    val tags = Map("name" -> "test", "atlas.dstype" -> "sum")
    val id = ItemIdCalculator.compute(tags)

    val start = step
    val end = start + step * 240
    (start until end by step).foreach { t =>
      clock.setWallTime(t)
      db.update(id, tags, t, 1.0)
      // Simulate data arriving a bit late with an updated sum that will overwrite the
      // previous value
      if (t - step >= start)
        db.update(id, tags, t - step, 2.0)
      if (t - 2 * step >= start)
        db.update(id, tags, t - 2 * step, 3.0)
      db.rebuild()
    }
    // Update last positions, so all values should be 3
    db.update(id, tags, end - step, 3.0)
    db.update(id, tags, end - 2 * step, 3.0)

    val ctxt = EvalContext(end - step * 180, end, step)
    val expr = parseExpr("name,test,:eq,:sum")
    val results = db.execute(ctxt, expr)
    results.foreach { r =>
      val data = r.data.bounded(ctxt.start, ctxt.end)
      data.data.foreach { v =>
        assertEqualsDouble(v, 3.0, 1.0e-12)
      }
    }
  }

  test("first update at exact hour boundary") {
    val tags = Map("name" -> "test", "atlas.dstype" -> "sum")
    val id = ItemIdCalculator.compute(tags)

    // First update arrives at exactly 1 hour mark (step * 60)
    // This is the boundary between blocks and should be handled correctly
    val hourBoundary = step * 60 // 3600000ms = 1 hour

    clock.setWallTime(hourBoundary)
    db.update(id, tags, hourBoundary, 1.0)
    db.update(id, tags, hourBoundary, 2.0)
    db.update(id, tags, hourBoundary, 3.0)

    // Add more data points
    clock.setWallTime(hourBoundary + step)
    db.update(id, tags, hourBoundary + step, 1.0)

    db.rebuild()

    // Query including the hour boundary
    val ctxt = EvalContext(hourBoundary, hourBoundary + step, step)
    val expr = parseExpr("name,test,:eq,:sum")
    val results = db.execute(ctxt, expr)
    results.foreach { r =>
      val data = r.data.bounded(ctxt.start, ctxt.end)
      val firstValue = data.data.head
      assertEqualsDouble(firstValue, 3.0, 1.0e-12)
    }
  }
}

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
package com.netflix.atlas.core.model

import java.time.zone.ZoneRulesException

import com.netflix.atlas.core.stacklang.Interpreter
import munit.FunSuite

class TimeSpanSuite extends FunSuite {

  private val step = 60000L
  private val start = 0L
  private val end = 10L * step
  private val context = EvalContext(start, end, step, Map.empty)

  private val interpreter = Interpreter(MathVocabulary.allWords)

  private def eval(program: String): Array[Double] = {
    val results = interpreter.execute(program).stack.collect {
      case ModelExtractors.TimeSeriesType(t) => t
    }
    assertEquals(results.size, 1)
    val span = results.head
    span.eval(context, Nil).data.head.data.bounded(start, end).data
  }

  test("relative start time") {
    val actual = eval(s"tz,UTC,:set,e-5m,1970-01-01T00:08,:time-span")
    val expected = Array[Double](0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0)
    assertEquals(actual.toSeq, expected.toSeq)
  }

  test("relative end time") {
    val actual = eval(s"tz,UTC,:set,1970-01-01T00:02,s+1m,:time-span")
    val expected = Array[Double](0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    assertEquals(actual.toSeq, expected.toSeq)
  }

  test("graph relative start time") {
    val actual = eval(s"tz,UTC,:set,gs,s+1m,:time-span")
    val expected = Array[Double](1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    assertEquals(actual.toSeq, expected.toSeq)
  }

  test("graph relative end time") {
    val actual = eval(s"tz,UTC,:set,e-5m,ge,:time-span")
    val expected = Array[Double](0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0)
    assertEquals(actual.toSeq, expected.toSeq)
  }

  test("invalid time") {
    val e = intercept[IllegalArgumentException] {
      eval(s"tz,UTC,:set,foo42,now,:time-span")
    }
    assertEquals(e.getMessage, "invalid date foo42")
  }

  test("invalid time zone") {
    val e = intercept[ZoneRulesException] {
      eval(s"tz,foo,:set,e-5m,now,:time-span")
    }
    assertEquals(e.getMessage, "Unknown time-zone ID: foo")
  }

  test("start is after end") {
    val e = intercept[IllegalArgumentException] {
      eval(s"tz,UTC,:set,42,0,:time-span")
    }
    assertEquals(e.getMessage, "requirement failed: start must be <= end")
  }

  test("start is relative to itself") {
    val e = intercept[IllegalArgumentException] {
      eval(s"tz,UTC,:set,s-5m,now,:time-span")
    }
    assertEquals(e.getMessage, "start time is relative to itself")
  }

  test("end is relative to itself") {
    val e = intercept[IllegalArgumentException] {
      eval(s"tz,UTC,:set,gs,e-5m,:time-span")
    }
    assertEquals(e.getMessage, "end time is relative to itself")
  }

  test("start/end are relative to each other") {
    val e = intercept[IllegalArgumentException] {
      eval(s"tz,UTC,:set,e-5m,s+5m,:time-span")
    }
    assertEquals(e.getMessage, "start and end time are relative to each other")
  }
}

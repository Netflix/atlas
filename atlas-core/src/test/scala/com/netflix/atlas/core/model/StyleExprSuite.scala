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

import com.netflix.atlas.core.stacklang.Interpreter

import java.time.Duration
import munit.FunSuite

class StyleExprSuite extends FunSuite {

  private val interpreter = Interpreter(StyleVocabulary.allWords)

  private val oneDay = Duration.ofDays(1)
  private val oneWeek = Duration.ofDays(7)

  test("perOffset") {
    val expr = StyleExpr(DataExpr.Sum(Query.True), Map("offset" -> "(,0h,1d,1w,)"))
    val expected = List(
      StyleExpr(DataExpr.Sum(Query.True), Map.empty),
      StyleExpr(DataExpr.Sum(Query.True).withOffset(oneDay), Map.empty),
      StyleExpr(DataExpr.Sum(Query.True).withOffset(oneWeek), Map.empty)
    )
    assertEquals(expr.perOffset, expected)
  }

  test("perOffset empty") {
    val expr = StyleExpr(DataExpr.Sum(Query.True), Map("offset" -> "(,)"))
    val expected = List(expr)
    assertEquals(expr.perOffset, expected)
  }

  test("perOffset not specified") {
    val expr = StyleExpr(DataExpr.Sum(Query.True), Map.empty)
    val expected = List(expr)
    assertEquals(expr.perOffset, expected)
  }

  private def newExpr(legend: String, sed: String): StyleExpr = {
    StyleExpr(DataExpr.Sum(Query.True), Map("legend" -> legend, "sed" -> sed))
  }

  private def newTimeSeries(label: String, tags: Map[String, String]): TimeSeries = {
    val data = new FunctionTimeSeq(DsType.Gauge, 1, _ => Double.NaN)
    LazyTimeSeries(tags, label, data)
  }

  test("decode after substitute") {
    val expr = newExpr(s"$$b", "hex,:decode")
    val ts = newTimeSeries("foo", Map("a" -> "1", "b" -> "one_21_25_26_3F"))
    assertEquals(expr.legend(ts), "one!%&?")
  }

  private def check(sed: String, expected: String): Unit = {
    test(sed) {
      val expr = newExpr(s"$$b", sed)
      val ts = newTimeSeries("foo", Map("a" -> "1", "2" -> "two", "b" -> "one_21_25_26_3F"))
      assertEquals(expr.legend(ts), expected)
    }
  }

  check("^([a-z]+).*$,prefix [$1],:s", "prefix [one]")
  check("^(?<prefix>[a-z]+).*$,prefix [$prefix],:s", "prefix [one]")
  check("^(?<prefix>[A-Z]*).*$,prefix [$prefix],:s", "prefix []")
  check("^(?<prefix>[a-z]+).*$,prefix [$1],:s", "prefix [one]")
  check("^(?<prefix>[A-Z]*).*$,prefix [$1],:s", "prefix []")
  check("^(?<prefix>[A-Z]*).*$,prefix [$2],:s", "prefix [two]")
  check("^(?<a>[a-z]+).*$,$a [$1],:s", "one [one]")
  check("_.*,,:s", "one")
  check("(_[A-F0-9]{2}), $1,:s,hex,:decode", "one ! % & ?")
  check("^([a-z]+).*$,$()prefix [$1],:s", "$prefix [one]")
  check("^([a-z]+).*$,\\prefix [$1],:s", "\\prefix [one]")
  check("^([a-z]+).*$,$a [$1],:s", "1 [one]")
  check("hex,:decode,(_[A-F0-9]{2}), $1,:s", "one!%&?")
  check("hex,:decode,%,_25,:s", "one!_25&?")
  check("hex,:decode,%,_25,:s,hex,:decode", "one!%&?")
  check("none,:decode,%,_25,:s,hex,:decode", "one!%&?")

  private def eval(str: String): StyleExpr = {
    interpreter.execute(str).stack match {
      case ModelExtractors.PresentationType(t) :: Nil => t
      case _                                          => throw new MatchError(str)
    }
  }

  test("alpha and color are preserved with exprString") {
    val expr = eval(":true,ff0000,:color,40,:alpha")
    val result = eval(expr.toString)
    assert(expr.settings == result.settings)
  }

  test("alpha and palette are preserved with exprString") {
    val expr = eval(":true,reds,:palette,40,:alpha")
    val result = eval(expr.toString)
    assert(expr.settings == result.settings)
  }
}

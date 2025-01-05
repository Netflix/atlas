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
import munit.FunSuite

class FinalGroupingSuite extends FunSuite {

  private val interpreter = Interpreter(StyleVocabulary.allWords)

  private def eval(s: String): List[String] = {
    val expr = interpreter.execute(s).stack match {
      case ModelExtractors.PresentationType(t) :: Nil => t
      case _                                          => throw new IllegalArgumentException(s)
    }
    expr.expr.finalGrouping
  }

  test("constant") {
    assertEquals(eval("42"), Nil)
  }

  test("aggregate") {
    assertEquals(eval("name,sps,:eq,:sum"), Nil)
  }

  test("data group by") {
    assertEquals(eval("name,sps,:eq,(,cluster,),:by"), List("cluster"))
  }

  test("data group by with multiple keys") {
    assertEquals(eval("name,sps,:eq,(,app,region,device,),:by"), List("app", "region", "device"))
  }

  test("data group by with math aggregate") {
    val expr = "name,sps,:eq,(,app,region,device,),:by,:max"
    val expected = Nil
    assertEquals(eval(expr), expected)
  }

  test("data group by with math group by") {
    val expr = "name,sps,:eq,(,app,region,device,),:by,:max,(,region,device,),:by"
    val expected = List("region", "device")
    assertEquals(eval(expr), expected)
  }

  test("aggregate with percentiles") {
    val expr = "name,sps,:eq,(,50,),:percentiles"
    val expected = List("percentile")
    assertEquals(eval(expr), expected)
  }

  test("data group by with percentiles") {
    val expr = "name,sps,:eq,(,app,),:by,(,50,),:percentiles"
    val expected = List("percentile", "app")
    assertEquals(eval(expr), expected)
  }
}

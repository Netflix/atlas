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
package com.netflix.atlas.eval.graph

import com.netflix.atlas.core.model.CustomVocabulary
import com.netflix.atlas.core.model.ModelExtractors
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.stacklang.Interpreter
import com.typesafe.config.ConfigFactory
import munit.FunSuite

class SimpleLegendsSuite extends FunSuite {

  private val notSet = "__NOT_SET__"

  private val interpreter = Interpreter(new CustomVocabulary(ConfigFactory.load()).allWords)

  private def eval(str: String): List[StyleExpr] = {
    interpreter
      .execute(str)
      .stack
      .map {
        case ModelExtractors.PresentationType(t) => t
        case v                                   => throw new MatchError(v)
      }
      .reverse
      .flatMap(_.perOffset)
  }

  private def legends(str: String): List[String] = {
    SimpleLegends.generate(eval(str)).map(_.settings.getOrElse("legend", notSet))
  }

  test("honor explicit legend") {
    assertEquals(legends("name,cpu,:eq,:sum,foo,:legend"), List("foo"))
  }

  test("just math") {
    assertEquals(legends("4,5,:add,10,:mul"), List(notSet))
  }

  test("reqular query and just math") {
    assertEquals(legends("name,cpu,:eq,:sum,seconds,:time"), List("cpu", notSet))
  }

  test("prefer just name") {
    assertEquals(legends("name,cpu,:eq,:sum"), List("cpu"))
    assertEquals(legends("name,cpu,:eq,id,user,:eq,:and,:sum"), List("cpu"))
  }

  test("use group by keys") {
    assertEquals(legends("name,cpu,:eq,:sum,(,app,id,),:by"), List("$(app) $(id)"))
  }

  test("name with math") {
    assertEquals(legends("name,cpu,:eq,:sum,4,:add,6,:mul,:abs"), List("cpu"))
  }

  test("name regex") {
    assertEquals(legends("name,cpu,:re,:sum"), List("cpu"))
  }

  test("name not present") {
    assertEquals(legends("id,user,:eq,:sum"), List("user"))
  }

  test("name with offsets") {
    val expr = "name,cpu,:eq,:sum,(,0h,1w,),:offset"
    assertEquals(legends(expr), List("cpu", "cpu (offset=$(atlas.offset))"))
  }

  test("name with avg") {
    assertEquals(legends("name,cpu,:eq,:avg"), List("cpu"))
  }

  test("name with dist avg") {
    assertEquals(legends("name,cpu,:eq,:dist-avg"), List("cpu"))
  }

  test("name with dist-stddev") {
    assertEquals(legends("name,cpu,:eq,:dist-stddev"), List("cpu"))
  }

  test("name not clause") {
    assertEquals(legends("name,cpu,:eq,:not,:sum"), List("!cpu"))
  }

  test("name starts clause") {
    assertEquals(legends("name,sys.cpu,:starts,:sum"), List("sys.cpu"))
  }

  test("name contains clause") {
    assertEquals(legends("name,sys.cpu,:contains,:sum"), List("sys.cpu"))
  }

  test("name with node avg") {
    assertEquals(legends("name,cpu,:eq,:node-avg"), List("cpu"))
  }

  test("name with node avg and grouping") {
    assertEquals(legends("name,cpu,:eq,:node-avg,(,app,),:by"), List("$(app)"))
  }

  test("name with node avg and grouping with special chars") {
    assertEquals(legends("name,cpu,:eq,:node-avg,(,foo:bar,),:by"), List("$(foo:bar)"))
  }

  test("name with node avg and nested grouping") {
    val expr = "name,cpu,:eq,:node-avg,(,app,region,),:by,:max,(,region,),:by"
    assertEquals(legends(expr), List("$(region)"))
  }

  test("group by with offsets") {
    val expr = "name,cpu,:eq,:sum,(,id,),:by,(,0h,1w,),:offset"
    assertEquals(legends(expr), List("$(id)", "$(id) (offset=$(atlas.offset))"))
  }

  test("complex: same name and math") {
    assertEquals(legends("name,cpu,:eq,:sum,:dup,:add"), List("cpu"))
  }

  test("complex: not clause") {
    val expr = "name,cpu,:eq,:dup,id,user,:eq,:and,:sum,:swap,id,user,:eq,:not,:and,:sum"
    assertEquals(legends(expr), List("user", "!user"))
  }

  test("complex: different names and math") {
    assertEquals(legends("name,cpu,:eq,:sum,name,disk,:eq,:sum,:and"), List(notSet))
  }

  test("multi: different names") {
    assertEquals(legends("name,cpu,:eq,:sum,name,disk,:eq,:sum"), List("cpu", "disk"))
  }

  test("multi: same name further restricted") {
    val vs = legends(
      "name,cpu,:eq,:sum," +
        "name,cpu,:eq,id,user,:eq,:and,:sum," +
        "name,cpu,:eq,id,system,:eq,:and,:sum," +
        "name,cpu,:eq,id,idle,:eq,:and,:sum,"
    )
    assertEquals(vs, List("cpu", "user", "system", "idle"))
  }

  test("multi: same name with math") {
    val vs = legends("name,cpu,:eq,:sum,:dup,4,:add")
    assertEquals(vs, List("cpu", "cpu"))
  }
}

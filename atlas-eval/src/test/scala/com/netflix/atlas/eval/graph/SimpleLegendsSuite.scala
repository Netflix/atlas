/*
 * Copyright 2014-2021 Netflix, Inc.
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
import org.scalatest.funsuite.AnyFunSuite

class SimpleLegendsSuite extends AnyFunSuite {

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
    assert(legends("name,cpu,:eq,:sum,foo,:legend") === List("foo"))
  }

  test("just math") {
    assert(legends("4,5,:add,10,:mul") === List(notSet))
  }

  test("reqular query and just math") {
    assert(legends("name,cpu,:eq,:sum,seconds,:time") === List("cpu", notSet))
  }

  test("prefer just name") {
    assert(legends("name,cpu,:eq,:sum") === List("cpu"))
    assert(legends("name,cpu,:eq,id,user,:eq,:and,:sum") === List("cpu"))
  }

  test("use group by keys") {
    assert(legends("name,cpu,:eq,:sum,(,app,id,),:by") === List("$app $id"))
  }

  test("name with math") {
    assert(legends("name,cpu,:eq,:sum,4,:add,6,:mul,:abs") === List("cpu"))
  }

  test("name regex") {
    assert(legends("name,cpu,:re,:sum") === List("cpu"))
  }

  test("name not present") {
    assert(legends("id,user,:eq,:sum") === List("user"))
  }

  test("name with offsets") {
    val expr = "name,cpu,:eq,:sum,(,0h,1w,),:offset"
    assert(legends(expr) === List("cpu", "cpu (offset=$atlas.offset)"))
  }

  test("name with avg") {
    assert(legends("name,cpu,:eq,:avg") === List("cpu"))
  }

  test("name with dist avg") {
    assert(legends("name,cpu,:eq,:dist-avg") === List("cpu"))
  }

  test("name with dist-stddev") {
    assert(legends("name,cpu,:eq,:dist-stddev") === List("cpu"))
  }

  test("name not clause") {
    assert(legends("name,cpu,:eq,:not,:sum") === List("!cpu"))
  }

  test("name with node avg") {
    assert(legends("name,cpu,:eq,:node-avg") === List("cpu"))
  }

  test("group by with offsets") {
    val expr = "name,cpu,:eq,:sum,(,id,),:by,(,0h,1w,),:offset"
    assert(legends(expr) === List("$id", "$id (offset=$atlas.offset)"))
  }

  test("complex: same name and math") {
    assert(legends("name,cpu,:eq,:sum,:dup,:add") === List("cpu"))
  }

  test("complex: not clause") {
    val expr = "name,cpu,:eq,:dup,id,user,:eq,:and,:sum,:swap,id,user,:eq,:not,:and,:sum"
    assert(legends(expr) === List("user", "!user"))
  }

  test("complex: different names and math") {
    assert(legends("name,cpu,:eq,:sum,name,disk,:eq,:sum,:and") === List(notSet))
  }

  test("multi: different names") {
    assert(legends("name,cpu,:eq,:sum,name,disk,:eq,:sum") === List("cpu", "disk"))
  }

  test("multi: same name further restricted") {
    val vs = legends(
      "name,cpu,:eq,:sum," +
      "name,cpu,:eq,id,user,:eq,:and,:sum," +
      "name,cpu,:eq,id,system,:eq,:and,:sum," +
      "name,cpu,:eq,id,idle,:eq,:and,:sum,"
    )
    assert(vs === List("cpu", "user", "system", "idle"))
  }

  test("multi: same name with math") {
    val vs = legends("name,cpu,:eq,:sum,:dup,4,:add")
    assert(vs === List("cpu", "cpu"))
  }
}

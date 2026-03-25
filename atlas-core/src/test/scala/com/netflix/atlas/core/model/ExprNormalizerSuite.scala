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
package com.netflix.atlas.core.model

import com.netflix.atlas.core.stacklang.Interpreter
import com.typesafe.config.ConfigFactory
import munit.FunSuite

class ExprNormalizerSuite extends FunSuite {

  private val normalizer = new ExprNormalizer(
    ConfigFactory.load().getConfig("atlas.core.normalize")
  )

  private val interpreter = Interpreter(StyleVocabulary.allWords)

  private def normalize(expr: String): List[String] = {
    val exprs = interpreter.execute(expr).stack.collect {
      case ModelDataTypes.PresentationType(t) => t
    }
    exprs.map(normalizer.normalizeToString).reverse
  }

  test("simple expression") {
    assertEquals(normalize("name,sps,:eq"), List("name,sps,:eq,:sum"))
  }

  test("multiple expressions") {
    assertEquals(
      normalize("name,sps,:eq,:dup,2,:mul,:swap"),
      List("name,sps,:eq,:sum,2.0,:mul", "name,sps,:eq,:sum")
    )
  }

  test("simplify duplicate and") {
    assertEquals(normalize("name,sps,:eq,:dup,:and"), List("name,sps,:eq,:sum"))
  }

  test("simplify in-clause with eq") {
    assertEquals(
      normalize("name,sps,:eq,name,(,sps,),:in,:and"),
      List("name,sps,:eq,:sum")
    )
  }

  test("dedup in-clause values") {
    assertEquals(
      normalize("name,sps,:eq,name,(,sps,sps,),:in,:and"),
      List("name,sps,:eq,:sum")
    )
  }

  test("merge identical in-clauses") {
    assertEquals(
      normalize("name,(,sps1,sps2,),:in,name,(,sps2,sps1,),:in,:and"),
      List("name,(,sps1,sps2,),:in,:sum")
    )
  }

  test("prefix keys ordered by position") {
    val expr = "nf.cluster,foo,:eq,nf.app,bar,:eq,:and,:sum"
    val expected = "nf.app,bar,:eq,nf.cluster,foo,:eq,:and,:sum"
    assertEquals(normalize(expr), List(expected))
  }

  test("multiple prefix keys in order") {
    val expr = "nf.cluster,c,:eq,name,n,:eq,nf.stack,s,:eq,nf.app,a,:eq,:and,:and,:and,:sum"
    val expected = "name,n,:eq,nf.app,a,:eq,:and,nf.stack,s,:eq,:and,nf.cluster,c,:eq,:and,:sum"
    assertEquals(normalize(expr), List(expected))
  }

  test("prefix key before regular key") {
    val expr = "app,foo,:eq,name,bar,:eq,:and,:sum"
    val expected = "name,bar,:eq,app,foo,:eq,:and,:sum"
    assertEquals(normalize(expr), List(expected))
  }

  test("regular keys lexically ordered") {
    val expr = "zoo,z,:eq,app,a,:eq,foo,f,:eq,:and,:and,:sum"
    val expected = "app,a,:eq,foo,f,:eq,:and,zoo,z,:eq,:and,:sum"
    assertEquals(normalize(expr), List(expected))
  }

  test("suffix key after regular key") {
    val expr = "statistic,count,:eq,app,foo,:eq,:and,:sum"
    val expected = "app,foo,:eq,statistic,count,:eq,:and,:sum"
    assertEquals(normalize(expr), List(expected))
  }

  test("suffix key after prefix key") {
    val expr = "statistic,count,:eq,name,foo,:eq,:and,:sum"
    val expected = "name,foo,:eq,statistic,count,:eq,:and,:sum"
    assertEquals(normalize(expr), List(expected))
  }

  test("prefix, regular, and suffix keys together") {
    val expr = "statistic,count,:eq,app,foo,:eq,name,bar,:eq,:and,:and,:sum"
    val expected = "name,bar,:eq,app,foo,:eq,:and,statistic,count,:eq,:and,:sum"
    assertEquals(normalize(expr), List(expected))
  }

  test("legend var normalization") {
    val expr = "name,sps,:eq,:sum,$name,:legend"
    val result = normalize(expr)
    assert(result.head.contains("$(name)"))
  }

  test("normalize removes :const") {
    assertEquals(normalize("42"), List("42.0"))
  }

  test("normalize removes :line") {
    assertEquals(normalize("name,sps,:eq,:sum"), List("name,sps,:eq,:sum"))
  }
}

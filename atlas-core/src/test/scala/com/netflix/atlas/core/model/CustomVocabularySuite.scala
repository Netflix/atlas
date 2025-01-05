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
import com.typesafe.config.ConfigFactory
import munit.FunSuite

class CustomVocabularySuite extends FunSuite {

  private val cpuUser = "name,cpuUser,:eq"
  private val numInstances = "name,aws.numInstances,:eq"

  private val config = ConfigFactory.parseString(s"""
      |atlas.core.vocabulary {
      |  words = [
      |    {
      |      name = "square"
      |      body = ":dup,:mul"
      |      examples = []
      |    }
      |  ]
      |
      |  custom-averages = [
      |    {
      |      name = "node-avg"
      |      base-query = "$numInstances"
      |      keys = ["app", "cluster", "asg", "node", "region", "zone"]
      |    }
      |  ]
      |}
    """.stripMargin)

  private val vocab = new CustomVocabulary(config)
  private val interpreter = Interpreter(vocab.allWords)

  private def eval(program: String): TimeSeriesExpr = {
    val result = interpreter.execute(program)
    result.stack match {
      case ModelExtractors.TimeSeriesType(v) :: Nil => v
      case v                                        => throw new MatchError(v)
    }
  }

  test("custom word: square") {
    val expr = eval("2,:square")
    val expected = eval("2,2,:mul")
    assertEquals(expr, expected)
  }

  test("simple average") {
    val expr = eval(s"$cpuUser,:node-avg").rewrite {
      case MathExpr.NamedRewrite("node-avg", _, _, e, _, _) => e
    }
    val expected = eval(s"$cpuUser,:sum,$numInstances,:sum,:div")
    assertEquals(expr, expected)
  }

  test("expr with cluster") {
    val expr = eval(s"$cpuUser,cluster,foo,:eq,:and,:node-avg").rewrite {
      case MathExpr.NamedRewrite("node-avg", _, _, e, _, _) => e
    }
    val expected = eval(s"$cpuUser,:sum,$numInstances,:sum,:div,cluster,foo,:eq,:cq")
    assertEquals(expr, expected)
  }

  test("expr with cq using non-infrastructure tags") {
    val expr = eval(s"$cpuUser,:node-avg,core,1,:eq,:cq").rewrite {
      case MathExpr.NamedRewrite("node-avg", _, _, e, _, _) => e
    }
    val expected = eval(s"$cpuUser,core,1,:eq,:and,:sum,$numInstances,:sum,:div")
    assertEquals(expr, expected)
  }

  test("expr grouped by infrastructure tags") {
    val expr = eval(s"$cpuUser,cluster,foo,:eq,:and,:node-avg,(,zone,),:by").rewrite {
      case MathExpr.NamedRewrite("node-avg", _, _, e, _, _) => e
    }
    val expected =
      eval(s"$cpuUser,:sum,(,zone,),:by,$numInstances,:sum,(,zone,),:by,:div,cluster,foo,:eq,:cq")
    assertEquals(expr, expected)
  }

  test("expr grouped by non-infrastructure tags") {
    val expr = eval(s"$cpuUser,cluster,foo,:eq,:and,:node-avg,(,name,),:by").rewrite {
      case MathExpr.NamedRewrite("node-avg", _, _, e, _, _) => e
    }
    val expected = eval(s"$cpuUser,:sum,(,name,),:by,$numInstances,:sum,:div,cluster,foo,:eq,:cq")
    assertEquals(expr, expected)
  }

  test("expr grouped by non-infrastructure tags with offset") {
    val displayExpr = eval(s"$cpuUser,cluster,foo,:eq,:and,:node-avg,(,name,),:by,1h,:offset")
    val evalExpr = displayExpr.rewrite {
      case MathExpr.NamedRewrite("node-avg", _, _, e, _, _) => e
    }
    val expected = eval(
      s"$cpuUser,:sum,(,name,),:by,PT1H,:offset,$numInstances,:sum,PT1H,:offset,:div,cluster,foo,:eq,:cq"
    )
    assertEquals(evalExpr, expected)
    assertEquals(
      displayExpr.toString,
      s"$cpuUser,cluster,foo,:eq,:and,:node-avg,PT1H,:offset,(,name,),:by"
    )
  }

  test("expr with cq") {
    val e1 = eval(s"$cpuUser,cluster,api,:eq,:and,:node-avg")
    val e2 = eval(s"$cpuUser,:node-avg,:list,(,cluster,api,:eq,:cq,),:each")
    assertEquals(e1, e2)
  }

  test("expr with group by") {
    val e1 = eval("name,(,a,b,c,),:in,app,beacon,:eq,zone,1c,:eq,:and,:and,:node-avg,(,name,),:by")
    val e2 = eval("name,(,a,b,c,),:in,:node-avg,(,name,),:by,app,beacon,:eq,zone,1c,:eq,:and,:cq")
    assertEquals(e1, e2)
  }

  test("expr with not") {
    val expr = eval(s"$cpuUser,foo,bar,:eq,:not,:and,cluster,foo,:eq,:and,:node-avg").rewrite {
      case MathExpr.NamedRewrite("node-avg", _, _, e, _, _) => e
    }
    val expected =
      eval(s"$cpuUser,foo,bar,:eq,:not,:and,:sum,$numInstances,:sum,:div,cluster,foo,:eq,:cq")
    assertEquals(expr, expected)
  }

  test("group by mixed keys") {
    val expr = eval("name,(,a,b,c,),:in,app,beacon,:eq,:and,:node-avg,(,name,asg,),:by").rewrite {
      case MathExpr.NamedRewrite("node-avg", _, _, e, _, _) => e
    }
    val expected = eval(
      s"name,(,a,b,c,),:in,:sum,(,name,asg,),:by,$numInstances,:sum,(,asg,),:by,:div,app,beacon,:eq,:cq"
    )
    assertEquals(expr, expected)
  }

  test("cg after style") {
    val context = interpreter.execute("foo,1,:eq,$foo,:legend,(,bar,),:cg")
    val context2 = interpreter.execute("foo,1,:eq,(,bar,),:by,$foo,:legend")
    assertEquals(context.stack, context2.stack)
  }

  test("cg after complex expression including style") {
    val complexExpr = "name,cpu,:eq,(,node,),:by,80,:gt,15,:rolling-count,10,:gt" +
      ",$node,:legend,region,east,:eq,:cq,(,region,),:cg"
    val context = interpreter.execute(complexExpr)

    val complexExprExplicit = "name,cpu,:eq,region,east,:eq,:and,(,node,region,),:by," +
      "80,:gt,15,:rolling-count,10,:gt,$node,:legend"
    val context2 = interpreter.execute(complexExprExplicit)
    assertEquals(context.stack, context2.stack)
  }
}

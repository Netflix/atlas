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
package com.netflix.atlas.eval.util

import com.netflix.atlas.core.model.CustomVocabulary
import com.netflix.atlas.core.model.ModelExtractors
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.stacklang.Interpreter
import com.typesafe.config.ConfigFactory
import munit.FunSuite

class HostRewriterSuite extends FunSuite {

  private val config = ConfigFactory.load()
  private val interpreter = Interpreter(new CustomVocabulary(config).allWords)

  private def interpret(str: String): List[StyleExpr] = {
    interpreter.execute(str).stack.reverse.flatMap {
      case ModelExtractors.PresentationType(t) => t.perOffset
      case v                                   => throw new MatchError(v)
    }
  }

  test("default shouldn't change the expr") {
    val rewriter = new HostRewriter(config.getConfig("atlas.eval.host-rewrite"))
    val exprs = interpret("name,sps,:eq,:sum")
    val host = "foo.example.com"
    assertEquals(rewriter.rewrite(host, exprs), exprs)
  }

  test("restrict by region extracted from host") {
    val regionConfig = ConfigFactory.parseString("""
        |pattern = "^foo\\.([^.]+)\\.example.com$"
        |key = "region"
        |""".stripMargin)
    val rewriter = new HostRewriter(regionConfig)
    val exprs = interpret("name,sps,:eq,:sum")
    val expected = interpret("name,sps,:eq,region,us-east-1,:eq,:and,:sum")
    val host = "foo.us-east-1.example.com"
    assertEquals(rewriter.rewrite(host, exprs), expected)
  }

  test("use first group if multiple in pattern") {
    val regionConfig =
      ConfigFactory.parseString("""
        |pattern = "^foo\\.([^.]+)\\.(example|example2).com$"
        |key = "region"
        |""".stripMargin)
    val rewriter = new HostRewriter(regionConfig)
    val exprs = interpret("name,sps,:eq,:sum")
    val expected = interpret("name,sps,:eq,region,us-east-1,:eq,:and,:sum")
    val host = "foo.us-east-1.example.com"
    assertEquals(rewriter.rewrite(host, exprs), expected)
  }

  test("no group in pattern") {
    val regionConfig = ConfigFactory.parseString("""
        |pattern = "^foo\\.example\\.com$"
        |key = "region"
        |""".stripMargin)
    val rewriter = new HostRewriter(regionConfig)
    val exprs = interpret("name,sps,:eq,:sum")
    val host = "foo.example.com"
    intercept[IndexOutOfBoundsException] {
      rewriter.rewrite(host, exprs)
    }
  }
}

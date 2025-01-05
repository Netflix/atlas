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
package com.netflix.atlas.core.stacklang

import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.model.StyleVocabulary
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole

/**
  * Simple test for escaping a stacklang expression.
  *
  * ```
  * > jmh:run -wi 10 -i 10 -f1 -t1 .*InterpreterEscape.*
  * ...
  * Benchmark           Mode  Cnt       Score       Error  Units
  * withEscapes        thrpt   10   956439.595 ±  5243.582  ops/s
  * withNoEscapes      thrpt   10  1031542.340 ± 25261.261  ops/s
  * ```
  */
@State(Scope.Thread)
class InterpreterEscape {

  import InterpreterEscape.*

  private val exprWithNoEscapes = parseExpr(
    """
      |name,sys.cpu.coreUtilization,:eq,
      |statistic,totalAmount,:eq,:and,
      |nf.app,foo,:eq,:and,
      |nf.asg,foo-v[0-9]+,:eq,:and,
      |:sum,
      |
      |name,sys.cpu.coreUtilization,:eq,
      |statistic,count,:eq,:and,
      |nf.app,foo,:eq,:and,
      |nf.asg,foo-v[0-9]+,:eq,:and,
      |:sum,
      |
      |:div,
      |average-latency,:legend
      |""".stripMargin
  )

  private val exprWithEscapes = parseExpr(
    """
      |name,sys.cpu.coreUtilization,:eq,
      |statistic,totalAmount,:eq,:and,
      |nf.app,foo,:eq,:and,
      |nf.asg,foo-v[0-9]{1\\u002c3},:eq,:and,
      |:sum,
      |
      |name,sys.cpu.coreUtilization,:eq,
      |statistic,count,:eq,:and,
      |nf.app,foo,:eq,:and,
      |nf.asg,foo-v[0-9]{1\\u002c3},:eq,:and,
      |:sum,
      |
      |:div,
      |average latency,:legend
      |""".stripMargin
  )

  @Benchmark
  def withNoEscapes(bh: Blackhole): Unit = {
    bh.consume(exprWithNoEscapes.toString)
  }

  @Benchmark
  def withEscapes(bh: Blackhole): Unit = {
    bh.consume(exprWithEscapes.toString)
  }

}

object InterpreterEscape {

  import com.netflix.atlas.core.model.ModelExtractors.*

  private val interpreter = Interpreter(StyleVocabulary.allWords)

  private def parseExpr(str: String): StyleExpr = {
    interpreter.execute(str).stack match {
      case PresentationType(expr) :: Nil => expr
      case _                             => throw new MatchError(str)
    }
  }
}

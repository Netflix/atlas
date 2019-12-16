/*
 * Copyright 2014-2019 Netflix, Inc.
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

import com.netflix.atlas.core.model.ModelExtractors
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.model.TimeSeriesExpr
import org.scalatest.funsuite.AnyFunSuite

abstract class BaseExamplesSuite extends AnyFunSuite {

  def vocabulary: Vocabulary

  protected val interpreter = new Interpreter(vocabulary.allWords)

  protected def eval(program: String): TimeSeriesExpr = {
    interpreter.execute(program).stack match {
      case ModelExtractors.TimeSeriesType(t) :: Nil => t
    }
  }

  for (w <- vocabulary.words; ex <- w.examples) {
    if (ex.startsWith("UNK:")) {
      test(s"noException -- $ex,:${w.name}") {
        val prg = ex.substring("UNK:".length)
        try interpreter.execute(s"$prg,:${w.name}")
        catch {
          case e: IllegalArgumentException if e.getMessage.startsWith("unknown word ") =>
          case e: Exception                                                            => throw e
        }
      }
    } else if (ex.startsWith("ERROR:")) {
      test(s"exception -- $ex,:${w.name}") {
        val prg = ex.substring("ERROR:".length)
        intercept[Exception] { interpreter.execute(s"$prg,:${w.name}") }
      }
    } else {
      test(s"noException -- $ex,:${w.name}") {
        interpreter.execute(s"$ex,:${w.name}")
      }

      test(s"toString(item) -- $ex,:${w.name}") {
        val stack = interpreter.execute(s"$ex,:${w.name}").stack
        stack.foreach { item =>
          val prg = item match {
            case vs: List[_] => vs
            case v           => List(v)
          }
          val stack2 = interpreter.execute(Interpreter.toString(prg)).stack
          assert(stack2 === prg)
        }
      }

      test(s"finalGrouping and isGrouped match -- $ex,:${w.name}") {
        interpreter.execute(s"$ex,:${w.name}").stack.foreach {
          case s: StyleExpr      => assert(s.expr.finalGrouping.nonEmpty === s.expr.isGrouped)
          case t: TimeSeriesExpr => assert(t.finalGrouping.nonEmpty === t.isGrouped)
          case _                 =>
        }
      }

      // Exclude offset because list form is lazily evaluated and breaks the comparison
      if (w.name != "offset") {
        test(s"toString(stack) -- $ex,:${w.name}") {
          val stack = interpreter.execute(s"$ex,:${w.name}").stack
          assert(stack === interpreter.execute(Interpreter.toString(stack)).stack)
        }
      }
    }
  }
}

/*
 * Copyright 2015 Netflix, Inc.
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

import org.scalatest.FunSuite

abstract class BaseExamplesSuite extends FunSuite {

  def vocabulary: List[Word]

  val i = new Interpreter(vocabulary)

  for (w <- vocabulary; ex <- w.examples) {
    if (ex.startsWith("UNK:")) {
      test(s"noException -- $ex,:${w.name}") {
        val prg = ex.substring("UNK:".length)
        try i.execute(s"$prg,:${w.name}") catch {
          case e: IllegalArgumentException if e.getMessage.startsWith("unknown word ") =>
          case e: Exception => throw e
        }
      }
    } else if (ex.startsWith("ERROR:")) {
      test(s"exception -- $ex,:${w.name}") {
        val prg = ex.substring("ERROR:".length)
        intercept[Exception] { i.execute(s"$prg,:${w.name}") }
      }
    } else {
      test(s"noException -- $ex,:${w.name}") {
        i.execute(s"$ex,:${w.name}")
      }

      test(s"toString(item) -- $ex,:${w.name}") {
        val stack = i.execute(s"$ex,:${w.name}").stack
        stack.foreach { item =>
          val prg = item match {
            case vs: List[_] => vs
            case v           => List(v)
          }
          val stack2 = i.execute(Interpreter.toString(prg)).stack
          assert(stack2 === prg)
        }
      }

      // Exclude offset because list form is lazily evaluated and breaks the comparison
      if (w.name != "offset") {
        test(s"toString(stack) -- $ex,:${w.name}") {
          val stack = i.execute(s"$ex,:${w.name}").stack
          assert(stack === i.execute(Interpreter.toString(stack)).stack)
        }
      }
    }
  }
}


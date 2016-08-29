/*
 * Copyright 2014-2016 Netflix, Inc.
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
package com.netflix.atlas.lwcapi

import com.netflix.atlas.core.model.{ModelExtractors, StyleVocabulary}
import com.netflix.atlas.core.stacklang.Interpreter

import scala.collection.mutable

case class ExpressionSyntaxException(message: String) extends IllegalArgumentException(message)

class ExpressionSplitter {
  private var interpreter = Interpreter(StyleVocabulary.allWords)

  def split(expression: String) : Set[String] = {
    val ret = mutable.TreeSet[String]()
    val context = interpreter.execute(expression)
    context.stack.foreach {
      case ModelExtractors.PresentationType(s) =>
        s.expr.dataExprs.foreach { ret += _.toString }
      case default => {
        val klass = default.getClass
        throw ExpressionSyntaxException(s"Unprocessed expression: $default ($klass)")
      }
    }
    ret.toSet
  }

  def split(expressionWithFrequency: ExpressionWithFrequency) : Set[ExpressionWithFrequency] = {
    split(expressionWithFrequency.expression).map(e => ExpressionWithFrequency(e, expressionWithFrequency.frequency))
  }
}

object ExpressionSplitter {
  def apply() = new ExpressionSplitter()
}


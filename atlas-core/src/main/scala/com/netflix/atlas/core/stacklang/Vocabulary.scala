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

import scala.util.Failure
import scala.util.Success
import scala.util.Try

trait Vocabulary {

  def name: String

  def dependsOn: List[Vocabulary]

  def words: List[Word]

  /** Return a flattened list of all dependency vocabularies. */
  def dependencies: List[Vocabulary] = dependsOn.flatMap(d => d :: d.dependencies)

  /**
    * Return a flattened list of all words from this vocabulary plus words from all dependencies.
    */
  def allWords: List[Word] = dependsOn.flatMap(_.allWords) ::: words

  private def renderStack(vs: List[Any]): String = {
    val rows = vs.zipWithIndex.map {
      case (v, i) =>
        f"${i + 1}%5d. ${v.toString}%s"
    }
    rows.reverse.mkString("\n|")
  }

  private def renderExample(example: String, name: String): String = {
    val expr = if (example.startsWith("ERROR:")) example.substring("ERROR:".length) else example

    val in = renderStack(Interpreter(allWords).execute(expr).stack)

    val out = Try(Interpreter(allWords).execute(s"$expr,:$name")) match {
      case Success(c) => renderStack(c.stack)
      case Failure(t) => s"${t.getClass.getSimpleName}: ${t.getMessage}"
    }

    s"""
      |```
      |Expr: $expr,:$name
      |
      | In:
      |$in
      |Out:
      |$out
      |```
    """.stripMargin.trim
  }

  private def renderWord(w: Word): String = {
    s"""
      |## ${w.name}
      |
      |**Signature:** `${w.signature}`
      |
      |${w.summary}
      |
      |**Examples**
      |
      |${w.examples.map(ex => renderExample(ex, w.name)).mkString("\n|\n|")}
    """.stripMargin.trim
  }

  def toMarkdown: String = {
    allWords.sortWith(_.name < _.name).map(renderWord).mkString("\n\n")
  }
}

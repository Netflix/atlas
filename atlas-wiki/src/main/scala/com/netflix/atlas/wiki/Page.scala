/*
 * Copyright 2014-2017 Netflix, Inc.
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
package com.netflix.atlas.wiki

import java.io.File

import com.netflix.atlas.core.model.ModelExtractors.PresentationType
import com.netflix.atlas.core.model.StyleExpr
import com.netflix.atlas.core.model.StyleVocabulary
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.stacklang.Vocabulary
import com.netflix.atlas.core.stacklang.Word

import scala.util.Failure
import scala.util.Success
import scala.util.Try

trait Page {
  def name: String
  def path: Option[String]
  def content(graph: GraphHelper): String

  def file(baseDir: File): File = {
    val fname = s"$name.md"
    new File(baseDir, path.fold(fname)(v => s"$v/$fname"))
  }
}

trait SimplePage extends Page {
  def name: String = getClass.getSimpleName
  def path: Option[String] = None
}

trait StackWordPage extends Page {
  def vocab: Vocabulary
  def word: Word

  def name: String = s"${vocab.name}-${Utils.fileName(word.name)}"
  def path: Option[String] = Some("stacklang")

  def signature: String = s"`${word.signature}`"
  def summary: String = word.summary

  def examples(graph: GraphHelper): String = {
    word.examples.map(ex => renderExample(ex, word.name, graph)).mkString("\n|\n|")
  }

  def content(graph: GraphHelper): String = {
    s"""
       |## Signature
       |
       |$signature
       |
       |## Summary
       |
       |$summary
       |
       |## Examples
       |
       |${examples(graph)}
    """.stripMargin.trim
  }

  private def zipStacks(in: List[Any], out: List[Any]): List[(Option[Any], Option[Any])] = {
    in.map(v => Some(v)).zipAll(out.map(v => Some(v)), None, None)
  }

  private def imageUri(expr: StyleExpr, params: String): String = {
    s"/api/v1/graph?q=${expr.toString}&w=200&h=100&e=2014-02-20T15:00&$params"
  }

  private def renderCell(opt: Option[Any], graph: GraphHelper, params: String): String = opt match {
    case Some(v: String)           => s"<td>$v</td>"
    case Some(v: Number)           => s"<td>$v</td>"
    case Some(PresentationType(v)) => s"<td>${graph.imageHtml(imageUri(v, params))}</td>"
    case Some(v)                   => s"<td>$v</td>"
    case None                      => "<td></td>"
  }

  private def renderExample(example: String, name: String, graph: GraphHelper): String = {
    val expr = if (example.startsWith("ERROR:")) example.substring("ERROR:".length) else example

    val in = Interpreter(StyleVocabulary.allWords).execute(expr).stack

    val out = Try(Interpreter(StyleVocabulary.allWords).execute(s"$expr,:$name")) match {
      case Success(c) => c.stack
      case Failure(t) => List(s":bangbang: <b>${t.getClass.getSimpleName}:</b> ${t.getMessage}")
    }

    val params = if (name.startsWith("cf-")) "step=5m" else ""

    val buf = new StringBuilder
    buf.append(s"### `$example,:$name`\n\n")
    buf.append("<table><thead><th>Pos</th><th>Input</th><th>Output</th></thead><tbody>")
    zipStacks(in, out).zipWithIndex.foreach { case ((i, o), p) =>
      buf.append(s"<tr>\n<td>$p</td>\n")
        .append(s"${renderCell(i, graph, params)}\n")
        .append(s"${renderCell(o, graph, params)}\n<s/tr>")
    }
    buf.append("</tbody></table>\n")
    buf.toString()
  }
}

case class BasicStackWordPage(vocab: Vocabulary, word: Word) extends StackWordPage

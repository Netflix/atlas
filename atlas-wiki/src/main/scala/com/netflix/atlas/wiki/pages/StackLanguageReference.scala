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
package com.netflix.atlas.wiki.pages

import com.netflix.atlas.core.model.StyleVocabulary
import com.netflix.atlas.wiki.GraphHelper
import com.netflix.atlas.wiki.Page

class `Stack-Language-Reference` extends Page {
  override def name: String = "Stack-Language-Reference"
  override def path: Option[String] = Some("stacklang")

  override def content(graph: GraphHelper): String =
    s"""
       |Reference for operations available in the stack language. Use the sidebar to navigate.
       |
       |$referenceTOC
     """.stripMargin

  private def referenceTOC: String = {
    import com.netflix.atlas.wiki._
    val vocab = StyleVocabulary
    val w2v = (vocab :: vocab.dependencies).flatMap(v => v.words.map(_ -> v.name)).toMap
    val words = vocab.allWords.groupBy(_.name)
    val sections = words.toList.sortWith(_._1 < _._1).map { case (name, ws) =>
      val items = ws.map { w =>
        val vocabName = w2v(w)
        s"* [${w.signature}]($vocabName-${Utils.fileName(name)})"
      }
      s"#### $name\n\n${items.mkString("\n")}\n"
    }
    sections.mkString("\n")
  }
}

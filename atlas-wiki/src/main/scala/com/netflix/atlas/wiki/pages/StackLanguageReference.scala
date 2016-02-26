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
package com.netflix.atlas.wiki.pages

import com.netflix.atlas.core.model.StyleVocabulary
import com.netflix.atlas.core.stacklang.Vocabulary
import com.netflix.atlas.wiki.GraphHelper
import com.netflix.atlas.wiki.Page

class StackLanguageReference(
    vocabs: List[Vocabulary],
    vocabDocs: Map[String, String]) extends Page {

  override def name: String = "Stack-Language-Reference"
  override def path: Option[String] = Some("stacklang")

  override def content(graph: GraphHelper): String =
    s"""
       |> [[Home]] ▸ Stack Language Reference
       |
       |Reference for operations available in the stack language. Operations can be browsed by
       |[category](#categories) or [alphabetically by name](#alphabetical-listing).
       |
       |## Categories
       |
       |$referenceCategories
       |
       |## Alphabetical Listing
       |
       |Alphabetical listing of all operations. This can be useful if you know the name, but
       |are unsure of the category.
       |
       |$referenceTOC
     """.stripMargin

  private def referenceCategories: String = {
    val builder = new StringBuilder
    vocabs.foreach { vocab =>
      builder.append(s"### [${vocab.name}](Reference-${vocab.name})\n\n")
      builder.append(vocabDocs(vocab.name))
      builder.append("\n\n")
    }
    builder.toString()
  }

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

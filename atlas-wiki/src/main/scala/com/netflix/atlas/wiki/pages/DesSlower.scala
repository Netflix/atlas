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

import com.netflix.atlas.core.model.StatefulVocabulary
import com.netflix.atlas.core.stacklang.Vocabulary
import com.netflix.atlas.core.stacklang.Word
import com.netflix.atlas.wiki.StackWordPage

case object DesSlower extends StackWordPage {
  val vocab: Vocabulary = StatefulVocabulary
  val word: Word = vocab.words.find(_.name == "des-slower").get

  override def signature: String =
    s"""
       |```
       |TimeSeriesExpr -- TimeSeriesExpr
       |```
     """.stripMargin

  override def summary: String =
    """
      |Helper for computing DES using settings to slowly adjust to the input line. See
      |[recommended values](DES#recommended-values) for more information. For most use-cases
      |the sliding DES variant [:sdes-slower](stateful-sdes‐slower) should be used instead.
    """.stripMargin.trim
}

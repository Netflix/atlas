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
package com.netflix.atlas.wiki.pages

import com.netflix.atlas.core.model.StyleVocabulary
import com.netflix.atlas.core.stacklang.Vocabulary
import com.netflix.atlas.core.stacklang.Word
import com.netflix.atlas.wiki.StackWordPage

case object DesEpicViz extends StackWordPage {
  val vocab: Vocabulary = StyleVocabulary
  val word: Word = vocab.words.find(_.name == "des-epic-viz").get

  override def signature: String =
    s"""
       |```
       |TimeSeriesExpr
       |training:Int
       |alpha:Double
       |beta:Double
       |maxPercent:Double
       |minPercent:Double
       |noise:Double         -- TimeSeriesExpr
       |```
     """.stripMargin

  override def summary: String =
    """
      |Helper for configuring [[DES]] in a manner compatible with legacy epic alerts. For more
      |information see the [epic macros](DES#epic-macros) section of the DES page.
    """.stripMargin.trim
}

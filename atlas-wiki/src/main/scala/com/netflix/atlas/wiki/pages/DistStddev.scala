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

import com.netflix.atlas.core.model.MathVocabulary
import com.netflix.atlas.core.stacklang.Vocabulary
import com.netflix.atlas.core.stacklang.Word
import com.netflix.atlas.wiki.StackWordPage

case object DistStddev extends StackWordPage {
  val vocab: Vocabulary = MathVocabulary
  val word: Word = vocab.words.find(_.name == "dist-stddev").get

  override def signature: String = s"`Query -- TimeSeriesExpr`"

  override def summary: String = """
      |Compute standard deviation for [timers](http://netflix.github.io/spectator/en/latest/intro/timer/)
      |and [distribution summaries](http://netflix.github.io/spectator/en/latest/intro/dist-summary/).
    """.stripMargin.trim
}

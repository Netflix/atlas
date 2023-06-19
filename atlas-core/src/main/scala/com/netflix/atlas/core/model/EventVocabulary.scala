/*
 * Copyright 2014-2023 Netflix, Inc.
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
package com.netflix.atlas.core.model

import com.netflix.atlas.core.stacklang.SimpleWord
import com.netflix.atlas.core.stacklang.Vocabulary
import com.netflix.atlas.core.stacklang.Word

object EventVocabulary extends Vocabulary {

  val name: String = "event"

  val dependsOn: List[Vocabulary] = List(QueryVocabulary)

  override def words: List[Word] = List(TableWord)

  case object TableWord extends SimpleWord {

    import ModelExtractors.*

    override def name: String = "table"

    override protected def matcher: PartialFunction[List[Any], Boolean] = {
      case StringListType(_) :: (_: Query) :: _ => true
    }

    override protected def executor: PartialFunction[List[Any], List[Any]] = {
      case StringListType(cs) :: (q: Query) :: stack => EventExpr.Table(q, cs) :: stack
    }

    override def signature: String = "q:Query columns:List -- EventExpr"

    override def summary: String =
      """
        |Find matching events and create a row by extracting the specified columns.
        |""".stripMargin

    override def examples: List[String] = List("name,sps,:eq")
  }
}

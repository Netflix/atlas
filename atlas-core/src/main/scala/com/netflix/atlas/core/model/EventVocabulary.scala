/*
 * Copyright 2014-2026 Netflix, Inc.
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

import scala.collection.immutable.ArraySeq

import com.netflix.atlas.core.stacklang.Context
import com.netflix.atlas.core.stacklang.TypedWord
import com.netflix.atlas.core.stacklang.Vocabulary
import com.netflix.atlas.core.stacklang.Word
import com.netflix.atlas.core.stacklang.ast.DataType
import com.netflix.atlas.core.stacklang.ast.Parameter

object EventVocabulary extends Vocabulary {

  import com.netflix.atlas.core.model.ModelDataTypes.*

  val name: String = "event"

  val dependsOn: List[Vocabulary] = List(QueryVocabulary)

  override def words: List[Word] = List(SampleWord, TableWord)

  case object TableWord extends TypedWord {

    override def name: String = "table"

    override def parameters: IndexedSeq[Parameter] = ArraySeq(
      Parameter("q", "query to match events", QueryType),
      Parameter("columns", "columns to extract", DataType.StringListType)
    )

    override def outputs: IndexedSeq[DataType] = ArraySeq(EventExprType)

    override def execute(context: Context, params: IndexedSeq[Any]): Context = {
      val q = params(0).asInstanceOf[Query]
      val cs = params(1).asInstanceOf[List[String]]
      context.copy(stack = EventExpr.Table(q, cs) :: context.stack)
    }

    override def summary: String =
      """
        |Find matching events and create a row by extracting the specified columns.
        |""".stripMargin

    override def examples: List[String] = List("level,ERROR,:eq,(,message,)")
  }

  case object SampleWord extends TypedWord {

    override def name: String = "sample"

    override def parameters: IndexedSeq[Parameter] = ArraySeq(
      Parameter("q", "query to match events", QueryType),
      Parameter("sampleBy", "keys to sample by", DataType.StringListType),
      Parameter("projectionKeys", "keys for sample projection", DataType.StringListType)
    )

    override def outputs: IndexedSeq[DataType] = ArraySeq(EventExprType)

    override def execute(context: Context, params: IndexedSeq[Any]): Context = {
      val q = params(0).asInstanceOf[Query]
      val by = params(1).asInstanceOf[List[String]]
      val pks = params(2).asInstanceOf[List[String]]
      context.copy(stack = EventExpr.Sample(q, by, pks) :: context.stack)
    }

    override def summary: String =
      """
        |Find matching events and sample based on a set of keys. The output will be a count
        |for the step interval along with some sample data for that group based on the projection
        |keys.
        |""".stripMargin

    override def examples: List[String] = List("level,ERROR,:eq,(,fingerprint,),(,message,)")
  }
}

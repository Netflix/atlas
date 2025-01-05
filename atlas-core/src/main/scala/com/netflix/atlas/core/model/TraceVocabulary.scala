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
package com.netflix.atlas.core.model

import com.netflix.atlas.core.stacklang.SimpleWord
import com.netflix.atlas.core.stacklang.Vocabulary
import com.netflix.atlas.core.stacklang.Word

object TraceVocabulary extends Vocabulary {

  import ModelExtractors.*

  val name: String = "trace"

  val dependsOn: List[Vocabulary] = List(StyleVocabulary)

  override def words: List[Word] = List(
    SpanAndWord,
    SpanOrWord,
    SpanFilterWord,
    SpanTimeSeriesWord,
    ChildWord
  )

  case object SpanAndWord extends SimpleWord {

    override def name: String = "span-and"

    override protected def matcher: PartialFunction[List[Any], Boolean] = {
      case TraceQueryType(_) :: TraceQueryType(_) :: _ => true
    }

    override protected def executor: PartialFunction[List[Any], List[Any]] = {
      case TraceQueryType(q2) :: TraceQueryType(q1) :: stack =>
        TraceQuery.SpanAnd(q1, q2) :: stack
    }

    override def signature: String = "q1:TraceQuery q2:TraceQuery -- TraceQuery"

    override def summary: String =
      """
        |Matches if the trace has a span that matches `q1` and a span that matches `q2`.
        |""".stripMargin

    override def examples: List[String] = List("app,foo,:eq,app,bar,:eq")
  }

  case object SpanOrWord extends SimpleWord {

    override def name: String = "span-or"

    override protected def matcher: PartialFunction[List[Any], Boolean] = {
      case TraceQueryType(_) :: TraceQueryType(_) :: _ => true
    }

    override protected def executor: PartialFunction[List[Any], List[Any]] = {
      case TraceQueryType(q2) :: TraceQueryType(q1) :: stack =>
        TraceQuery.SpanOr(q1, q2) :: stack
    }

    override def signature: String = "q1:TraceQuery q2:TraceQuery -- TraceQuery"

    override def summary: String =
      """
        |Matches if the trace has a span that matches `q1` or a span that matches `q2`.
        |""".stripMargin

    override def examples: List[String] = List("app,foo,:eq,app,bar,:eq")
  }

  case object ChildWord extends SimpleWord {

    override def name: String = "child"

    override protected def matcher: PartialFunction[List[Any], Boolean] = {
      case (_: Query) :: (_: Query) :: _                                 => true
      case (_: Query) :: (_: TraceQuery.Child) :: _                      => true
      case (_: Query) :: TraceQuery.SpanAnd(_, _: TraceQuery.Child) :: _ => true
    }

    override protected def executor: PartialFunction[List[Any], List[Any]] = {
      case (q2: Query) :: (q1: Query) :: stack =>
        TraceQuery.Child(q1, q2) :: stack
      case (q2: Query) :: (tq: TraceQuery.Child) :: stack =>
        TraceQuery.SpanAnd(tq, TraceQuery.Child(tq.q2, q2)) :: stack
      case (q2: Query) :: (s @ TraceQuery.SpanAnd(_, tq: TraceQuery.Child)) :: stack =>
        TraceQuery.SpanAnd(s, TraceQuery.Child(tq.q2, q2)) :: stack
    }

    override def signature: String = "q1:TraceQuery q2:TraceQuery -- TraceQuery"

    override def summary: String =
      """
        |Matches if the trace has a span that matches `q1` with a direct child span that
        |matches `q2`.
        |""".stripMargin

    override def examples: List[String] = List("app,foo,:eq,app,bar,:eq")
  }

  case object SpanFilterWord extends SimpleWord {

    override def name: String = "span-filter"

    override protected def matcher: PartialFunction[List[Any], Boolean] = {
      case (_: Query) :: TraceQueryType(_) :: _ => true
    }

    override protected def executor: PartialFunction[List[Any], List[Any]] = {
      case (f: Query) :: TraceQueryType(q) :: stack =>
        TraceQuery.SpanFilter(q, f) :: stack
    }

    override def signature: String = "q:TraceQuery f:Query -- SpanFilter"

    override def summary: String =
      """
        |Filter to select the set of spans from a trace to forward as events.
        |""".stripMargin

    override def examples: List[String] = List("app,foo,:eq,app,bar,:eq")
  }

  case object SpanTimeSeriesWord extends SimpleWord {

    override def name: String = "span-time-series"

    override protected def matcher: PartialFunction[List[Any], Boolean] = {
      case PresentationType(_) :: TraceQueryType(_) :: _ => true
    }

    override protected def executor: PartialFunction[List[Any], List[Any]] = {
      case PresentationType(f: StyleExpr) :: TraceQueryType(q) :: stack =>
        TraceQuery.SpanTimeSeries(q, f) :: stack
    }

    override def signature: String = "q:TraceQuery f:Query -- SpanFilter"

    override def summary: String =
      """
        |Time series based on data from a set of matching traces.
        |""".stripMargin

    override def examples: List[String] = List("app,foo,:eq,app,bar,:eq,:sum,ts,:legend")
  }
}

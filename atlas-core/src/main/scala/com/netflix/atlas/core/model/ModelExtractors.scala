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

import java.time.Duration

import com.netflix.atlas.core.model.DataExpr.AggregateFunction
import com.netflix.atlas.core.util.Strings

import scala.util.Try

object ModelExtractors {

  case object DurationType {

    def unapply(value: Any): Option[Duration] = value match {
      case v: String   => Try(Strings.parseDuration(v)).toOption
      case v: Duration => Some(v)
      case _           => None
    }
  }

  case object StringListType {

    def unapply(value: Any): Option[List[String]] = value match {
      case vs: List[?] if vs.forall(_.isInstanceOf[String]) => Some(vs.asInstanceOf[List[String]])
      case _                                                => None
    }
  }

  case object DoubleListType {

    def unapply(value: Any): Option[List[Double]] = value match {
      case StringListType(vs) => Try(vs.map(_.toDouble)).toOption
      case _                  => None
    }
  }

  case object AggrType {

    def unapply(value: Any): Option[AggregateFunction] = value match {
      case v: Query             => Some(DataExpr.Sum(v))
      case v: AggregateFunction => Some(v)
      case _                    => None
    }
  }

  case object DataExprType {

    def unapply(value: Any): Option[DataExpr] = value match {
      case v: Query    => Some(DataExpr.Sum(v))
      case v: DataExpr => Some(v)
      case _           => None
    }
  }

  case object TimeSeriesType {

    def unapply(value: Any): Option[TimeSeriesExpr] = value match {
      case v: String if Try(v.toDouble).isSuccess => Some(MathExpr.Constant(v.toDouble))
      case v: Query                               => Some(DataExpr.Sum(v))
      case v: TimeSeriesExpr                      => Some(v)
      case _                                      => None
    }
  }

  case object PresentationType {

    def unapply(value: Any): Option[StyleExpr] = value match {
      case TimeSeriesType(t) => Some(StyleExpr(t, Map.empty))
      case s: StyleExpr      => Some(s)
      case _                 => None
    }
  }

  case object EventExprType {

    def unapply(value: Any): Option[EventExpr] = value match {
      case e: EventExpr => Some(e)
      case q: Query     => Some(EventExpr.Raw(q))
      case _            => None
    }
  }

  case object TraceQueryType {

    def unapply(value: Any): Option[TraceQuery] = value match {
      case q: TraceQuery => Some(q)
      case q: Query      => Some(TraceQuery.Simple(q))
      case _             => None
    }
  }

  case object TraceFilterType {

    def unapply(value: Any): Option[TraceQuery.SpanFilter] = value match {
      case q: TraceQuery            => Some(toFilter(q))
      case q: Query                 => Some(toFilter(TraceQuery.Simple(q)))
      case f: TraceQuery.SpanFilter => Some(f)
      case _                        => None
    }

    private def toFilter(q: TraceQuery): TraceQuery.SpanFilter = {
      TraceQuery.SpanFilter(q, Query.True)
    }
  }

  case object TraceTimeSeriesType {

    def unapply(value: Any): Option[TraceQuery.SpanTimeSeries] = value match {
      case q: TraceQuery                => Some(toTimeSeries(q))
      case q: Query                     => Some(toTimeSeries(TraceQuery.Simple(q)))
      case t: TraceQuery.SpanTimeSeries => Some(t)
      case _                            => None
    }

    private def toTimeSeries(q: TraceQuery): TraceQuery.SpanTimeSeries = {
      TraceQuery.SpanTimeSeries(q, StyleExpr(DataExpr.Sum(Query.True), Map.empty))
    }
  }
}

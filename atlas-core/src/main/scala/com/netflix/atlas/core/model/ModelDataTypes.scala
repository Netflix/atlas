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

import scala.util.Try

import com.netflix.atlas.core.model.DataExpr.AggregateFunction
import com.netflix.atlas.core.stacklang.ast.DataType

object ModelDataTypes {

  /** Matches Query values on the stack. */
  case object QueryType extends DataType {

    def name: String = "Query"

    def extract(value: Any): Option[Any] = value match {
      case v: Query => Some(v)
      case _        => None
    }
  }

  /** Matches values that can be coerced to a ConsolidationFunction. */
  case object ConsolidationFunctionType extends DataType {

    def name: String = "ConsolidationFunction"
    def extract(value: Any): Option[Any] = unapply(value)

    def unapply(value: Any): Option[ConsolidationFunction] = value match {
      case "avg" => Some(ConsolidationFunction.Avg)
      case "sum" => Some(ConsolidationFunction.Sum)
      case "min" => Some(ConsolidationFunction.Min)
      case "max" => Some(ConsolidationFunction.Max)
      case _     => None
    }
  }

  /**
    * Matches values that can be coerced to an AggregateFunction. Query values will be
    * wrapped with a default sum aggregation.
    */
  case object AggrType extends DataType {

    def name: String = "AggregateFunction"
    def extract(value: Any): Option[Any] = unapply(value)

    def unapply(value: Any): Option[AggregateFunction] = value match {
      case v: Query             => Some(DataExpr.Sum(v))
      case v: AggregateFunction => Some(v)
      case _                    => None
    }
  }

  /**
    * Matches values that can be coerced to a DataExpr. Query values will be wrapped with
    * a default sum aggregation.
    */
  case object DataExprType extends DataType {

    def name: String = "DataExpr"
    def extract(value: Any): Option[Any] = unapply(value)

    def unapply(value: Any): Option[DataExpr] = value match {
      case v: Query    => Some(DataExpr.Sum(v))
      case v: DataExpr => Some(v)
      case _           => None
    }
  }

  /**
    * Matches values that can be coerced to TimeSeriesExpr. Strings that are valid doubles
    * will be converted to a constant, and Query values will be wrapped with a default sum
    * aggregation.
    */
  case object TimeSeriesExprType extends DataType {

    def name: String = "TimeSeriesExpr"
    def extract(value: Any): Option[Any] = unapply(value)

    def unapply(value: Any): Option[TimeSeriesExpr] = value match {
      case v: String =>
        Try(v.toDouble).toOption.map(d => MathExpr.Constant(d))
      case v: Query          => Some(DataExpr.Sum(v))
      case v: TimeSeriesExpr => Some(v)
      case _                 => None
    }
  }

  /**
    * Matches values that can be coerced to a StyleExpr. TimeSeriesExpr values will be
    * wrapped with empty settings.
    */
  case object PresentationType extends DataType {

    def name: String = "StyleExpr"
    def extract(value: Any): Option[Any] = unapply(value)

    def unapply(value: Any): Option[StyleExpr] = value match {
      case TimeSeriesExprType(t) => Some(StyleExpr(t, Map.empty))
      case s: StyleExpr          => Some(s)
      case _                     => None
    }
  }

  /**
    * Matches values that can be coerced to an EventExpr. Query values will be wrapped
    * with EventExpr.Raw.
    */
  case object EventExprType extends DataType {

    def name: String = "EventExpr"
    def extract(value: Any): Option[Any] = unapply(value)

    def unapply(value: Any): Option[EventExpr] = value match {
      case e: EventExpr => Some(e)
      case q: Query     => Some(EventExpr.Raw(q))
      case _            => None
    }
  }
}

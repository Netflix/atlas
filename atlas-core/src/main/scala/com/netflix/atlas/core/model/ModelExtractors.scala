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

import com.netflix.atlas.core.model.DataExpr.AggregateFunction

/** Use ModelDataTypes and DataType instead. */
@deprecated("Use ModelDataTypes and DataType instead", "1.9")
object ModelExtractors {

  /** Use DataType.DurationType instead. */
  @deprecated("Use DataType.DurationType instead", "1.9")
  case object DurationType {

    def unapply(value: Any): Option[java.time.Duration] =
      com.netflix.atlas.core.stacklang.ast.DataType.DurationType.unapply(value)
  }

  /** Use ModelDataTypes.ConsolidationFunctionType instead. */
  @deprecated("Use ModelDataTypes.ConsolidationFunctionType instead", "1.9")
  case object ConsolidationFunctionType {

    def unapply(value: Any): Option[ConsolidationFunction] =
      ModelDataTypes.ConsolidationFunctionType.unapply(value)
  }

  /** Use DataType.StringListType instead. */
  @deprecated("Use DataType.StringListType instead", "1.9")
  case object StringListType {

    def unapply(value: Any): Option[List[String]] =
      com.netflix.atlas.core.stacklang.ast.DataType.StringListType.unapply(value)
  }

  /** Use DataType.DoubleListType instead. */
  @deprecated("Use DataType.DoubleListType instead", "1.9")
  case object DoubleListType {

    def unapply(value: Any): Option[List[Double]] =
      com.netflix.atlas.core.stacklang.ast.DataType.DoubleListType.unapply(value)
  }

  /** Use ModelDataTypes.AggrType instead. */
  @deprecated("Use ModelDataTypes.AggrType instead", "1.9")
  case object AggrType {

    def unapply(value: Any): Option[AggregateFunction] =
      ModelDataTypes.AggrType.unapply(value)
  }

  /** Use ModelDataTypes.DataExprType instead. */
  @deprecated("Use ModelDataTypes.DataExprType instead", "1.9")
  case object DataExprType {

    def unapply(value: Any): Option[DataExpr] =
      ModelDataTypes.DataExprType.unapply(value)
  }

  /** Use ModelDataTypes.TimeSeriesExprType instead. */
  @deprecated("Use ModelDataTypes.TimeSeriesExprType instead", "1.9")
  case object TimeSeriesType {

    def unapply(value: Any): Option[TimeSeriesExpr] =
      ModelDataTypes.TimeSeriesExprType.unapply(value)
  }

  /** Use ModelDataTypes.PresentationType instead. */
  @deprecated("Use ModelDataTypes.PresentationType instead", "1.9")
  case object PresentationType {

    def unapply(value: Any): Option[StyleExpr] =
      ModelDataTypes.PresentationType.unapply(value)
  }

  /** Use ModelDataTypes.EventExprType instead. */
  @deprecated("Use ModelDataTypes.EventExprType instead", "1.9")
  case object EventExprType {

    def unapply(value: Any): Option[EventExpr] =
      ModelDataTypes.EventExprType.unapply(value)
  }
}

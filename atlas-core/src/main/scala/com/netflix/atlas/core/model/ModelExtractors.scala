/*
 * Copyright 2015 Netflix, Inc.
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
      case v: String         => Some(Strings.parseDuration(v))
      case v: Duration       => Some(v)
      case _                 => None
    }
  }

  case object StringListType {
    def unapply(value: Any): Option[List[String]] = value match {
      case vs: List[_] if vs.forall(_.isInstanceOf[String]) => Some(vs.asInstanceOf[List[String]])
      case _                                                => None
    }
  }

  case object AggrType {
    def unapply(value: Any): Option[AggregateFunction] = value match {
      case v: Query             => Some(DataExpr.Sum(v))
      case v: AggregateFunction => Some(v)
      case _                    => None
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
}

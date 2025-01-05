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
package com.netflix.atlas.core.stacklang

import scala.util.Try

object Extractors {

  case object IntType {

    def unapply(value: Any): Option[Int] = value match {
      case v: String => Try(v.toInt).toOption
      case v: Int    => Some(v)
      case _         => None
    }
  }

  case object DoubleType {

    def unapply(value: Any): Option[Double] = value match {
      case v: String => Try(v.toDouble).toOption
      case v: Double => Some(v)
      case _         => None
    }
  }
}

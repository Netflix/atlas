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
package com.netflix.atlas.core.stacklang.ast

import java.time.Duration

import scala.util.Try

import com.netflix.atlas.core.util.Strings

/**
  * Represents a type for a stack language parameter. Provides a name for display
  * and an extractor for type checking and coercion. Custom types can be created by
  * extending this trait.
  */
trait DataType {

  /** Display name for this type (e.g. "Int", "Double", "TimeSeriesExpr"). */
  def name: String

  /** Optional description providing a hint for diagnostic messages. */
  def description: String = ""

  /**
    * Attempt to extract/coerce a stack value to this type. Returns `Some` with the
    * extracted value if the value is compatible, `None` otherwise.
    */
  def extract(value: Any): Option[Any]
}

object DataType {

  /** Matches any value on the stack. */
  case object AnyType extends DataType {

    def name: String = "Any"
    def extract(value: Any): Option[Any] = Some(value)
  }

  /** Matches values that can be coerced to Int (strings, constants, etc.). */
  case object IntType extends DataType {

    def name: String = "Int"
    override def description: String = "integer value"
    def extract(value: Any): Option[Any] = unapply(value)

    def unapply(value: Any): Option[Int] = value match {
      case v: String   => Try(v.toInt).toOption
      case v: Number   => Some(v.intValue())
      case v: IsNumber => Some(v.toNumber.intValue())
      case _           => None
    }
  }

  /** Matches values that can be coerced to Double (strings, constants, etc.). */
  case object DoubleType extends DataType {

    def name: String = "Double"
    override def description: String = "numeric value"
    def extract(value: Any): Option[Any] = unapply(value)

    def unapply(value: Any): Option[Double] = value match {
      case v: String   => Try(v.toDouble).toOption
      case v: Number   => Some(v.doubleValue())
      case v: IsNumber => Some(v.toNumber.doubleValue())
      case _           => None
    }
  }

  /** Matches String values. */
  case object StringType extends DataType {

    def name: String = "String"

    def extract(value: Any): Option[Any] = value match {
      case s: String => Some(s)
      case _         => None
    }
  }

  /** Matches Color values. */
  case object ColorType extends DataType {

    def name: String = "Color"

    override def description: String =
      "hex color as 3-digit RGB (f00), 6-digit RGB (ff0000), or 8-digit ARGB (ffff0000)"

    def extract(value: Any): Option[Any] = value match {
      case s: String => Try(Strings.parseColor(s)).toOption
      case _         => None
    }
  }

  /**
    * Matches color strings: hex colors (3/6/8 digit) or named color identifiers
    * (e.g. blue1, red2). Returns the raw string without parsing to a Color object,
    * allowing named colors to be resolved later by the renderer.
    */
  case object ColorStringType extends DataType {

    def name: String = "Color"

    override def description: String =
      "hex color (f00, ff0000, ffff0000) or named color (blue1, red2)"

    private val NamePattern = "[a-zA-Z]+\\d*".r

    def extract(value: Any): Option[Any] = value match {
      case s: String =>
        Try(Strings.parseColor(s))
          .map(_ => s)
          .toOption
          .orElse(NamePattern.unapplySeq(s).map(_ => s))
      case _ => None
    }
  }

  /** Matches List values. */
  case object ListType extends DataType {

    def name: String = "List"

    def extract(value: Any): Option[Any] = value match {
      case v: List[?] => Some(v)
      case _          => None
    }
  }

  /** Matches values that can be coerced to Duration (strings or Duration instances). */
  case object DurationType extends DataType {

    def name: String = "Duration"
    override def description: String = "duration, e.g. 5m, 1h, PT30S"
    def extract(value: Any): Option[Any] = unapply(value)

    def unapply(value: Any): Option[Duration] = value match {
      case v: String   => Try(Strings.parseDuration(v)).toOption
      case v: Duration => Some(v)
      case _           => None
    }
  }

  /** Matches List values where all elements are strings. */
  case object StringListType extends DataType {

    def name: String = "StringList"
    override def description: String = "list of strings"
    def extract(value: Any): Option[Any] = unapply(value)

    def unapply(value: Any): Option[List[String]] = value match {
      case vs: List[?] if vs.forall(_.isInstanceOf[String]) => Some(vs.asInstanceOf[List[String]])
      case _                                                => None
    }
  }

  /** Matches List values where all elements can be coerced to Double. */
  case object DoubleListType extends DataType {

    def name: String = "DoubleList"
    override def description: String = "list of numbers"
    def extract(value: Any): Option[Any] = unapply(value)

    def unapply(value: Any): Option[List[Double]] = value match {
      case vs: List[?] =>
        val doubles = vs.map(DoubleType.unapply)
        if (doubles.forall(_.isDefined)) Some(doubles.map(_.get)) else None
      case _ => None
    }
  }
}

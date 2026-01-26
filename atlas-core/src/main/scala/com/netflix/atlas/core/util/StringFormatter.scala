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
package com.netflix.atlas.core.util

import java.text.DecimalFormat

/**
  * Secure string formatter with bounded width and precision to make it safe for usage with
  * arbitrary user inputs. Supports a subset of printf-style formatting with strict validation.
  *
  * Format spec: `%[index$][flags][width][.precision][type]`
  *
  * - `index$`: Optional 1-based positional index (e.g., %1$s, %2$d)
  * - `flags`: Optional flags:
  *   - `-`: Left align (right align is default)
  *   - `+`: Always show sign for numeric values
  *   - ` ` (space): Leading space for positive numbers
  *   - `0`: Zero padding for numeric values
  * - `width`: Optional field width (1-256)
  * - `.precision`: Optional precision (0-256 for strings, 0-32 for floats)
  * - `type`: Required type specifier:
  *   - `s`: String
  *   - `d`: Signed decimal integer
  *   - `u`: Unsigned decimal integer
  *   - `x`/`X`: Unsigned hexadecimal integer
  *   - `f`: Floating point in fixed notation
  *
  * Security limits:
  * - Maximum width: 256
  * - Maximum precision: 256 for strings, 32 for floats
  */
object StringFormatter {

  private val MaxWidth = 256
  private val MaxStringPrecision = 256
  private val MaxFloatPrecision = 32

  /**
    * Format a string with arguments using a safe printf-style pattern.
    *
    * @param pattern
    *     Format pattern with specifiers like `%s`, `%d`, `%f`, etc.
    * @param args
    *     Arguments to format into the pattern.
    * @return
    *     Formatted string.
    * @throws IllegalArgumentException
    *     If the pattern is invalid or arguments are incompatible.
    */
  def format(pattern: String, args: Any*): String = {
    val builder = new java.lang.StringBuilder(pattern.length * 2)
    var pos = 0
    var nextAutoIndex = 0

    while (pos < pattern.length) {
      val c = pattern.charAt(pos)
      if (c == '%') {
        if (pos + 1 < pattern.length && pattern.charAt(pos + 1) == '%') {
          builder.append('%')
          pos += 2
        } else {
          val parseResult = parseFormatSpec(pattern, pos + 1)
          val (spec, newPos, explicitIndex) = parseResult

          // Determine argument index
          val argIndex = explicitIndex match {
            case Some(idx) =>
              nextAutoIndex = scala.math.max(nextAutoIndex, idx + 1)
              idx
            case None =>
              val idx = nextAutoIndex
              nextAutoIndex += 1
              idx
          }

          if (argIndex >= args.length) {
            throw new IllegalArgumentException(
              s"missing argument at index $argIndex for format specifier at position $pos"
            )
          }

          formatValue(builder, spec, args(argIndex))
          pos = newPos
        }
      } else {
        builder.append(c)
        pos += 1
      }
    }
    builder.toString
  }

  /**
    * Parse a format specifier starting at the given position.
    * Returns (FormatSpec, newPosition, optionalExplicitIndex).
    */
  private def parseFormatSpec(
    pattern: String,
    startPos: Int
  ): (FormatSpec, Int, Option[Int]) = {
    var pos = startPos

    // Parse optional positional index (digits followed by '$')
    var explicitIndex: Option[Int] = None
    val indexStart = pos
    while (pos < pattern.length && pattern.charAt(pos).isDigit) {
      pos += 1
    }
    if (pos > indexStart && pos < pattern.length && pattern.charAt(pos) == '$') {
      explicitIndex = Some(pattern.substring(indexStart, pos).toInt - 1)
      pos += 1
    } else {
      // Not an index, reset position
      pos = indexStart
    }

    // Parse flags: -, +, space, 0
    var leftAlign = false
    var showSign = false
    var spaceSign = false
    var zeroPad = false
    var parsing = true
    while (parsing && pos < pattern.length) {
      pattern.charAt(pos) match {
        case '-' => leftAlign = true; pos += 1
        case '+' => showSign = true; pos += 1
        case ' ' => spaceSign = true; pos += 1
        case '0' => zeroPad = true; pos += 1
        case _   => parsing = false
      }
    }

    // Parse width (digits)
    var width: Option[Int] = None
    val widthStart = pos
    while (pos < pattern.length && pattern.charAt(pos).isDigit) {
      pos += 1
    }
    if (pos > widthStart) {
      val w = pattern.substring(widthStart, pos).toInt
      if (w < 1 || w > MaxWidth) {
        throw new IllegalArgumentException(s"width must be between 1 and $MaxWidth, got: $w")
      }
      width = Some(w)
    }

    // Parse precision (.digits)
    var precision: Option[Int] = None
    if (pos < pattern.length && pattern.charAt(pos) == '.') {
      pos += 1
      val precStart = pos
      while (pos < pattern.length && pattern.charAt(pos).isDigit) {
        pos += 1
      }
      if (pos > precStart) {
        precision = Some(pattern.substring(precStart, pos).toInt)
      } else {
        throw new IllegalArgumentException(s"missing precision value after '.' at position $pos")
      }
    }

    // Parse type character
    if (pos >= pattern.length) {
      throw new IllegalArgumentException(s"missing type specifier at position $pos")
    }
    val formatType = pattern.charAt(pos)
    if (!isValidType(formatType)) {
      throw new IllegalArgumentException(s"invalid type specifier '$formatType' at position $pos")
    }
    pos += 1

    // Validate precision against type
    precision.foreach { p =>
      val maxPrecision = formatType match {
        case 's' => MaxStringPrecision
        case 'f' => MaxFloatPrecision
        case _ =>
          throw new IllegalArgumentException(s"precision not allowed for type $formatType")
      }
      if (p < 0 || p > maxPrecision) {
        throw new IllegalArgumentException(
          s"precision must be between 0 and $maxPrecision for type $formatType, got: $p"
        )
      }
    }

    // Validate zero padding is only used with numeric types
    if (zeroPad && formatType == 's') {
      throw new IllegalArgumentException("zero padding not allowed for string type")
    }

    // Validate sign flags are only used with numeric types
    if ((showSign || spaceSign) && formatType == 's') {
      throw new IllegalArgumentException("sign flags not allowed for string type")
    }

    val spec = FormatSpec(leftAlign, showSign, spaceSign, zeroPad, width, precision, formatType)
    (spec, pos, explicitIndex)
  }

  private def isValidType(c: Char): Boolean = {
    c == 's' || c == 'd' || c == 'u' || c == 'x' || c == 'X' || c == 'f'
  }

  private case class FormatSpec(
    leftAlign: Boolean,
    showSign: Boolean,
    spaceSign: Boolean,
    zeroPad: Boolean,
    width: Option[Int],
    precision: Option[Int],
    formatType: Char
  )

  private def formatValue(builder: java.lang.StringBuilder, spec: FormatSpec, arg: Any): Unit = {
    val formatted = spec.formatType match {
      case 's' => formatString(arg, spec)
      case 'd' => formatInteger(arg, spec, signed = true, hex = false)
      case 'u' => formatInteger(arg, spec, signed = false, hex = false)
      case 'x' => formatInteger(arg, spec, signed = false, hex = true, uppercase = false)
      case 'X' => formatInteger(arg, spec, signed = false, hex = true, uppercase = true)
      case 'f' => formatFloat(arg, spec)
      case _   => throw new IllegalArgumentException(s"unsupported format type: ${spec.formatType}")
    }
    builder.append(formatted)
  }

  private def formatString(arg: Any, spec: FormatSpec): String = {
    val str = String.valueOf(arg)
    val truncated = spec.precision match {
      case Some(p) if p < str.length => str.substring(0, p)
      case _                         => str
    }
    applyWidth(truncated, spec)
  }

  private def formatInteger(
    arg: Any,
    spec: FormatSpec,
    signed: Boolean,
    hex: Boolean,
    uppercase: Boolean = false
  ): String = {
    val value = arg match {
      case n: Byte   => n.toLong
      case n: Short  => n.toLong
      case n: Int    => n.toLong
      case n: Long   => n
      case s: String => s.toLong
      case _         => throw new IllegalArgumentException(s"cannot format $arg as integer")
    }

    val (signPrefix, absValue) = if (signed) {
      if (value < 0) {
        // Handle Long.MinValue overflow
        val abs = if (value == Long.MinValue) Long.MaxValue + 1L else scala.math.abs(value)
        ("-", abs)
      } else if (spec.showSign) {
        ("+", value)
      } else if (spec.spaceSign) {
        (" ", value)
      } else {
        ("", value)
      }
    } else {
      ("", value & 0xFFFFFFFFFFFFFFFFL)
    }

    val numStr = if (hex) {
      val hexStr = java.lang.Long.toHexString(absValue)
      if (uppercase) hexStr.toUpperCase else hexStr
    } else {
      java.lang.Long.toUnsignedString(absValue)
    }

    val result = signPrefix + numStr
    applyWidth(result, spec, signPrefix.nonEmpty)
  }

  private def formatFloat(arg: Any, spec: FormatSpec): String = {
    val value = arg match {
      case n: Float  => n.toDouble
      case n: Double => n
      case s: String => s.toDouble
      case _         => throw new IllegalArgumentException(s"cannot format $arg as float")
    }

    val precision = spec.precision.getOrElse(6)
    val pattern = if (precision == 0) "0" else "0." + ("0" * precision)
    val df = new DecimalFormat(pattern)
    val numStr = df.format(scala.math.abs(value))

    val signPrefix = if (value < 0) {
      "-"
    } else if (spec.showSign) {
      "+"
    } else if (spec.spaceSign) {
      " "
    } else {
      ""
    }

    val result = signPrefix + numStr
    applyWidth(result, spec, signPrefix.nonEmpty)
  }

  private def applyWidth(
    str: String,
    spec: FormatSpec,
    hasSign: Boolean = false
  ): String = {
    spec.width match {
      case Some(w) if w > str.length =>
        val padding = w - str.length
        if (spec.leftAlign) {
          str + (" " * padding)
        } else if (spec.zeroPad && !hasSign) {
          ("0" * padding) + str
        } else if (spec.zeroPad && hasSign) {
          val sign = str.charAt(0).toString
          val rest = str.substring(1)
          sign + ("0" * padding) + rest
        } else {
          (" " * padding) + str
        }
      case _ => str
    }
  }
}

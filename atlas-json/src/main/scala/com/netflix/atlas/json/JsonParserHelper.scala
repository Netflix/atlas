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
package com.netflix.atlas.json

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken

object JsonParserHelper {

  @scala.annotation.tailrec
  def skip(parser: JsonParser, stop: JsonToken): Unit = {
    val t = parser.nextToken()
    if (t != null && t != stop) skip(parser, stop)
  }

  @scala.annotation.tailrec
  def skipTo(parser: JsonParser, token: JsonToken, stop: JsonToken): Boolean = {
    val t = parser.nextToken()
    if (t == null || t == token || t == stop) t == token else skipTo(parser, token, stop)
  }

  def skipNext(parser: JsonParser): Unit = {
    parser.nextToken() match {
      case JsonToken.START_ARRAY  => parser.skipChildren()
      case JsonToken.START_OBJECT => parser.skipChildren()
      case _                      =>
    }
  }

  def fail(parser: JsonParser, msg: String): Nothing = {
    val loc = parser.currentLocation
    val line = loc.getLineNr
    val col = loc.getColumnNr
    val fullMsg = s"$msg (line=$line, col=$col)"
    throw new IllegalArgumentException(fullMsg)
  }

  def requireNextToken(parser: JsonParser, expected: JsonToken): Unit = {
    val t = parser.nextToken()
    if (t != expected) fail(parser, s"expected $expected but received $t")
  }

  def nextString(parser: JsonParser): String = {
    requireNextToken(parser, JsonToken.VALUE_STRING)
    parser.getText
  }

  def nextStringList(parser: JsonParser): List[String] = {
    val vs = List.newBuilder[String]
    foreachItem(parser) {
      vs += parser.getText
    }
    vs.result()
  }

  def nextBoolean(parser: JsonParser): Boolean = {
    import com.fasterxml.jackson.core.JsonToken.*
    parser.nextToken() match {
      case VALUE_FALSE  => parser.getValueAsBoolean
      case VALUE_TRUE   => parser.getValueAsBoolean
      case VALUE_STRING => java.lang.Boolean.parseBoolean(parser.getText)
      case t            => fail(parser, s"expected VALUE_FALSE or VALUE_TRUE but received $t")
    }
  }

  def nextInt(parser: JsonParser): Int = {
    requireNextToken(parser, JsonToken.VALUE_NUMBER_INT)
    parser.getValueAsInt
  }

  def nextLong(parser: JsonParser): Long = {
    requireNextToken(parser, JsonToken.VALUE_NUMBER_INT)
    parser.getValueAsLong
  }

  def nextFloat(parser: JsonParser): Double = nextDouble(parser).toFloat

  def nextDouble(parser: JsonParser): Double = {
    import com.fasterxml.jackson.core.JsonToken.*
    parser.nextToken() match {
      case VALUE_NUMBER_INT   => parser.getValueAsDouble
      case VALUE_NUMBER_FLOAT => parser.getValueAsDouble
      case VALUE_STRING       => parseDouble(parser.getText)
      case t                  => fail(parser, s"expected VALUE_NUMBER_FLOAT but received $t")
    }
  }

  private def parseDouble(v: String): Double = {
    // Common case for string encoding is NaN
    if (v == "NaN") Double.NaN else java.lang.Double.parseDouble(v)
  }

  def foreachItem[T](parser: JsonParser)(f: => T): Unit = {
    requireNextToken(parser, JsonToken.START_ARRAY)
    var t = parser.nextToken()
    while (t != null && t != JsonToken.END_ARRAY) {
      f
      t = parser.nextToken()
    }

    if (t == null) {
      // If we get here, then `f` has a bug and consumed the END_ARRAY token
      throw new IllegalArgumentException(s"expected END_ARRAY but hit end of stream")
    }
  }

  def foreachField[T](parser: JsonParser)(f: PartialFunction[String, T]): Unit = {
    while (skipTo(parser, JsonToken.FIELD_NAME, JsonToken.END_OBJECT)) {
      val n = parser.getText
      f.applyOrElse[String, T](n, k => throw new IllegalArgumentException(s"invalid field: '$k'"))
    }
  }

  def firstField[T](parser: JsonParser)(f: PartialFunction[String, T]): Unit = {
    if (skipTo(parser, JsonToken.FIELD_NAME, JsonToken.END_OBJECT)) {
      val n = parser.getText
      f.applyOrElse[String, T](n, k => throw new IllegalArgumentException(s"invalid field: '$k'"))
    }
  }

  def skipToEndOfObject(parser: JsonParser): Unit = skip(parser, JsonToken.END_OBJECT)
}

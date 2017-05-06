/*
 * Copyright 2014-2017 Netflix, Inc.
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

import java.awt.Color
import java.time.Duration
import java.util.regex.Matcher
import java.util.regex.Pattern

import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.stacklang.StandardVocabulary
import com.netflix.atlas.core.util.Strings

case class StyleExpr(expr: TimeSeriesExpr, settings: Map[String, String]) extends Expr {
  override def toString: String = {
    val vs = settings.toList.sortWith(_._1 < _._1).map {
      case ("sed", v) => v
      case (k, v)     => s"$v,:$k"
    }
    if (vs.isEmpty) expr.toString else s"$expr,${vs.mkString(",")}"
  }

  def legend(t: TimeSeries): String = {
    val fmt = settings.getOrElse("legend", t.label)
    sed(Strings.substitute(fmt, t.tags))
  }

  private def sed(str: String): String = {
    settings.get("sed").fold(str) { v => sed(str, v.split(",").toList) }
  }

  @scala.annotation.tailrec
  private def sed(str: String, cmds: List[String]): String = {
    if (cmds.isEmpty) str else {
      cmds match {
        case mode :: ":decode" :: cs => sed(decode(str, mode), cs)
        case s :: r :: ":s" :: cs    => sed(searchAndReplace(str, s, r), cs)
        case _                       => sed(str, cmds.tail)
      }
    }
  }

  private def decode(str: String, mode: String): String = {
    mode match {
      case "hex"  => Strings.hexDecode(str, '_')
      case "none" => str
      case _      => throw new IllegalArgumentException(s"unknown encoding '$mode'")
    }
  }

  private def searchAndReplace(str: String, search: String, replace: String): String = {
    val m = Pattern.compile(search).matcher(str)
    if (!m.find()) str else {
      val sb = new StringBuffer()
      m.appendReplacement(sb, substitute(replace, m))
      while (m.find()) {
        m.appendReplacement(sb, substitute(replace, m))
      }
      m.appendTail(sb)
      sb.toString
    }
  }

  /**
    * The `appendReplacement` method on the matcher will do substitutions, but this makes
    * it consistent with the variable substitutions for legends to avoid confusion about
    * slightly different syntax for variables in legends verses the replacement field.
    */
  private def substitute(str: String, m: Matcher): String = {
    Strings.substitute(str, k => if (StyleExpr.isNumber(k)) m.group(k.toInt) else m.group(k))
  }

  def sortBy: Option[String] = settings.get("sort")

  def useDescending: Boolean = settings.get("order").contains("desc")

  /** Returns the maximum number of lines that should be shown for this expression. */
  def limit: Option[Int] = settings.get("limit").map(_.toInt)

  def axis: Option[Int] = settings.get("axis").map(_.toInt)

  def color: Option[Color] = settings.get("color").map(Strings.parseColor)

  def alpha: Option[Int] = settings.get("alpha").map { s =>
    require(s.length == 2, "value should be 2 digit hex string")
    Integer.parseInt(s, 16)
  }

  def lineStyle: Option[String] = settings.get("ls")

  def lineWidth: Float = settings.get("lw").fold(1.0f)(_.toFloat)

  def offset: Long = {
    if (expr.dataExprs.isEmpty) 0L else expr.dataExprs.map(_.offset.toMillis).min
  }

  private def offsets: List[Duration] = {
    settings.get("offset").fold(List.empty[Duration])(StyleExpr.parseDurationList)
  }

  /**
   * Returns a list of style expressions with one entry per offset specified. This is to support
   * a legacy form or :offset that takes a list and is applied on the style expression. Since
   * further style operations need to get applied to all results we cannot expand until the
   * full expression is evaluated. Should get removed after we have a better story around
   * deprecation.
   *
   * If no high level offset is used then a list with this expression will be returned.
   */
  def perOffset: List[StyleExpr] = {
    offsets match {
      case Nil => List(this)
      case vs  => vs.map(d => copy(expr = expr.withOffset(d), settings = settings - "offset"))
    }
  }
}

object StyleExpr {
  private val interpreter = new Interpreter(StandardVocabulary.allWords)

  private def parseDurationList(s: String): List[Duration] = {
    import ModelExtractors._
    val ctxt = interpreter.execute(s)
    ctxt.stack match {
      case StringListType(vs) :: Nil => vs.map { v => Strings.parseDuration(v) }
      case _                         => Nil
    }
  }

  private val numberPattern = Pattern.compile("""^(\d+)$""")

  private def isNumber(s: String): Boolean = numberPattern.matcher(s).matches()
}


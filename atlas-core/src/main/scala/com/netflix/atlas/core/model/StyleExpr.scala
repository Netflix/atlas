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

import java.awt.Color
import java.time.Duration

import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.stacklang.StandardVocabulary
import com.netflix.atlas.core.util.Strings

case class StyleExpr(expr: TimeSeriesExpr, settings: Map[String, String]) extends Expr {
  override def toString: String = {
    val vs = settings.toList.sortWith(_._1 < _._1).map(t => s"${t._2},:${t._1}")
    s"$expr,${vs.mkString(",")}"
  }

  def legend(t: TimeSeries): String = {
    val fmt = settings.getOrElse("legend", t.label)
    Strings.substitute(fmt, t.tags)
  }

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
}


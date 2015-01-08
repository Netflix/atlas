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
}


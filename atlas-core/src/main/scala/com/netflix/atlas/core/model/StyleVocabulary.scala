/*
 * Copyright 2014-2018 Netflix, Inc.
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

import com.netflix.atlas.core.stacklang.Context
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.stacklang.SimpleWord
import com.netflix.atlas.core.stacklang.StandardVocabulary.Macro
import com.netflix.atlas.core.stacklang.Vocabulary
import com.netflix.atlas.core.stacklang.Word
import com.netflix.atlas.core.util.Strings

object StyleVocabulary extends Vocabulary {

  import com.netflix.atlas.core.model.ModelExtractors._

  val name: String = "style"

  val dependsOn: List[Vocabulary] = List(FilterVocabulary)

  val words: List[Word] = List(
    Alpha, Color, LineStyle, LineWidth, Legend, Decode, Axis, Offset, Filter, Sort, Order,
    Macro("area", List("area", ":ls"), List("name,sps,:eq,:sum")),
    Macro("line", List("line", ":ls"), List("name,sps,:eq,:sum")),
    Macro("stack", List("stack", ":ls"), List("name,sps,:eq,(,nf.cluster,),:by")),
    Macro("vspan", List("vspan", ":ls"), List("name,sps,:eq,:sum,:dup,200e3,:gt")),

    Macro("des-epic-viz", desEpicViz, List("name,sps,:eq,:sum,10,0.1,0.5,0.2,0.2,4"))
  )

  sealed trait StyleWord extends SimpleWord {
    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case (_: String) :: PresentationType(_) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case (v: String) :: PresentationType(t) :: s =>
        t.copy(settings = t.settings + (name -> v)) :: s
    }

    override def signature: String = "TimeSeriesExpr String -- StyleExpr"
  }

  case object Alpha extends SimpleWord {
    override def name: String = "alpha"

    override def summary: String =
      """
        |Set the alpha value for the colors on the line. The value should be a two digit hex number
        |where `00` is transparent and `ff` is opague. This setting will be ignored if the
        |[color](style-color) setting is used for the same line.
      """.stripMargin.trim

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case (_: String) :: PresentationType(_) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case (v: String) :: PresentationType(t) :: s =>
        val settings = t.settings.get("color") match {
          case Some(c) => t.settings + ("color" -> withAlpha(c, v)) - "alpha"
          case None    => t.settings + ("alpha" -> v)
        }
        t.copy(settings = settings) :: s
    }

    private def withAlpha(color: String, alpha: String): String = {
      val a = Integer.parseInt(alpha, 16)
      val c = Strings.parseColor(color)
      val nc = new Color(c.getRed, c.getGreen, c.getBlue, a)
      "%08x".format(nc.getRGB)
    }

    override def signature: String = "TimeSeriesExpr String -- StyleExpr"

    override def examples: List[String] = List(
      "name,sps,:eq,:sum,:stack,40",
      "name,sps,:eq,:sum,:stack,f00,:color,40"
    )
  }

  case object Color extends SimpleWord {
    override def name: String = "color"

    override def summary: String =
      """
        |Set the color for the line. The value should be one of:
        |
        |* [Hex triplet]
        |  (http://en.wikipedia.org/wiki/Web_colors#Hex_triplet), e.g. f00 is red.
        |* 6 digit hex RBG, e.g. ff0000 is red.
        |* 8 digit hex ARGB, e.g. ffff0000 is red. The first byte is the [alpha](style-alpha)
        |  setting to use with the color.
      """.stripMargin.trim

    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case (_: String) :: PresentationType(_) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case (v: String) :: PresentationType(t) :: s =>
        val settings = t.settings + ("color" -> v) - "alpha"
        t.copy(settings = settings) :: s
    }

    override def signature: String = "TimeSeriesExpr String -- StyleExpr"

    override def examples: List[String] = List(
      "name,sps,:eq,:sum,ff0000",
      "name,sps,:eq,:sum,f00",
      "name,sps,:eq,:sum,40,:alpha,f00"
    )
  }

  case object LineStyle extends StyleWord {
    override def name: String = "ls"

    override def summary: String =
      """
        |Set the line style. The value should be one of:
        |
        |* `line`: this is the default, draws a normal line.
        |* `area`: fill in the space between the line value and 0 on the Y-axis.
        |* `stack`: stack the filled area on to the previous stacked lines on the same axis.
        |* `vspan`: non-zero datapoints will be drawn as a vertical span.
        |
        |See the [line style examples](Line-Styles) page for more information.
      """.stripMargin.trim

    override def examples: List[String] = List(
      "name,sps,:eq,:sum,(,name,),:by,line",
      "name,sps,:eq,:sum,(,name,),:by,area",
      "name,sps,:eq,:sum,(,name,),:by,stack",
      "name,sps,:eq,:sum,(,name,),:by,200e3,:gt,vspan")
  }

  case object LineWidth extends StyleWord {
    override def name: String = "lw"

    override def summary: String =
      """
        |The width of the stroke used when drawing the line.
      """.stripMargin.trim

    override def examples: List[String] = List("name,sps,:eq,:sum,(,name,),:by,2")
  }

  case object Legend extends StyleWord {
    override def name: String = "legend"

    override def summary: String =
      """
        |Set the legend text.
      """.stripMargin.trim

    override def examples: List[String] = List(s"name,sps,:eq,:sum,(,name,),:by,$$name")
  }

  case object Decode extends StyleWord {
    override def name: String = "decode"

    override def summary: String =
      """
        |> :warning: It is recommended to avoid using special symbols or trying to
        |> encode structural information into tag values. This feature should be used
        |> sparingly and with great care to ensure it will not result in a combinatorial
        |> explosion.
        |
        |Perform decoding of the legend strings. Generally data going into Atlas
        |is restricted to simple ascii characters that are easy to use as part of
        |a URI. Most commonly the clients will convert unsupported characters to
        |an `_`. In some case it is desirable to be able to reverse that for the
        |purposes of presentation.
        |
        |* `none`: this is the default. It will not modify the legend string.
        |* `hex`: perform a hex decoding of the legend string. This is similar to
        |  [url encoding](https://en.wikipedia.org/wiki/Percent-encoding) except
        |  that the `_` character is used instead of `%` to indicate the start of
        |  an encoded symbol. The decoding is lenient, if the characters following
        |  the `_` are not valid hexadecimal digits then it will just copy those
        |  characters without modification.
        |
        |Since: 1.5
      """.stripMargin.trim

    override def examples: List[String] = List(s"1,one_21_25_26_3F,:legend,hex")
  }

  case object Axis extends StyleWord {
    override def name: String = "axis"

    override def summary: String =
      """
        |Specify which Y-axis to use for the line.
      """.stripMargin.trim

    override def examples: List[String] = List("name,sps,:eq,:sum,1")
  }

  case object Offset extends SimpleWord {
    protected def matcher: PartialFunction[List[Any], Boolean] = {
      case StringListType(_) :: PresentationType(_) :: _ => true
    }

    protected def executor: PartialFunction[List[Any], List[Any]] = {
      case StringListType(vs) :: PresentationType(t) :: s =>
        val v = Interpreter.toString(List(vs))
        t.copy(settings = t.settings + (name -> v)) :: s
    }

    override def signature: String = "TimeSeriesExpr List -- StyleExpr"

    override def name: String = "offset"

    override def summary: String =
      """
        |> :warning: **Deprecated**. Use the [data variant](data-offset) with signature
        |> `TimeSeriesExpr Duration -- TimeSeriesExpr` instead.
        |
        |Shift the time frame to use when fetching the data. The expression will be copied for
        |each shift value in the list.
      """.stripMargin.trim

    override def examples: List[String] = List("name,sps,:eq,:sum,(,0h,1d,1w,)")
  }

  object Filter extends Word {
    override def name: String = "filter"

    override def matches(stack: List[Any]): Boolean = stack match {
      case TimeSeriesType(_) :: (_: StyleExpr) :: _ => true
      case _ => false
    }

    override def execute(context: Context): Context = {
      context.stack match {
        case TimeSeriesType(ts) :: (se: StyleExpr) :: s =>
          val rs = FilterVocabulary.Filter.execute(context.copy(stack = ts :: se.expr :: s))
          val newExpr = se.copy(expr = rs.stack.head.asInstanceOf[TimeSeriesExpr])
          rs.copy(stack = newExpr :: rs.stack.tail)
        case _ =>
          invalidStack
      }
    }

    override def summary: String =
      """
        |Filter the output based on another expression. This operation is an overload to allow
        |applying filters after presentation settings have been set. See the
        |[main filter page](filter-filter) for more details on general usage.
      """.stripMargin

    override def signature: String = "StyleExpr TimeSeriesExpr -- StyleExpr"

    override def examples: List[String] = List(
      "name,sps,:eq,:sum,(,nf.cluster,),:by,$nf.cluster,:legend,:stat-max,30e3,:gt")
  }

  case object Sort extends StyleWord {
    override def name: String = "sort"

    override def summary: String =
      """
        |Sort the results of an expression in the legend by one of the
        |[summary statistics](filter-stat) or by the legend text. The default
        |behavior is to sort by the legend text. This will sort in ascending
        |order by default, for descending order use [order](style-order).
        |
        |Since: 1.5
      """.stripMargin.trim

    override def examples: List[String] = List(
      "name,sps,:eq,:sum,(,nf.cluster,),:by,max",
      "name,sps,:eq,:sum,(,nf.cluster,),:by,legend")
  }

  case object Order extends StyleWord {
    override def name: String = "order"

    override def summary: String =
      """
        |Order to use for [sorting](style-sort) results. Supported values are `asc` and `desc`
        |for ascending and descending order respectively. Default is `asc`.
        |
        |Since: 1.5
      """.stripMargin.trim

    override def examples: List[String] = List(
      "name,sps,:eq,:sum,(,nf.cluster,),:by,max,:sort,asc",
      "name,sps,:eq,:sum,(,nf.cluster,),:by,desc")
  }

  private def desEpicViz = List(
    // Show signal line as a vertical span
    ":des-epic-signal",
    ":vspan", "40", ":alpha", "triggered", ":legend",

    // Raw input line
    "line", ":get", "line", ":legend",

    // Lower bounds
    "minPredNoiseBound", ":get", "minPredNoiseBound", ":legend",
    "minPredPercentBound", ":get", "minPredPercentBound", ":legend",

    // Upper bounds
    "maxPredNoiseBound", ":get", "maxPredNoiseBound", ":legend",
    "maxPredPercentBound", ":get", "maxPredPercentBound",":legend"
  )

}

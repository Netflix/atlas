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

import com.netflix.atlas.core.stacklang.SimpleWord
import com.netflix.atlas.core.stacklang.StandardVocabulary.Macro
import com.netflix.atlas.core.stacklang.Vocabulary
import com.netflix.atlas.core.stacklang.Word

object StyleVocabulary extends Vocabulary {

  import com.netflix.atlas.core.model.Extractors._

  val words: List[Word] = StatefulVocabulary.words ::: List(
    Alpha, Color, LineStyle, LineWidth, Legend, Axis,
    Macro("area", List("area", ":ls"), List("42")),
    Macro("line", List("line", ":ls"), List("42")),
    Macro("stack", List("stack", ":ls"), List("42")),
    Macro("vspan", List("vspan", ":ls"), List("42"))
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

  case object Alpha extends StyleWord {
    override def name: String = "alpha"

    override def summary: String =
      """
        |Set the alpha value for the colors on the line. The value should be a two digit hex number
        |where `00` is transparent and `ff` is opague.
      """.stripMargin.trim

    override def examples: List[String] = List("a,b,:eq,:sum,40")
  }

  case object Color extends StyleWord {
    override def name: String = "color"

    override def summary: String =
      """
        |Set the color for the line. The value should be one of:
        |
        |* [Hex triplet]
        |  (http://en.wikipedia.org/wiki/Web_colors#Hex_triplet), e.g. f00 is red.
        |* 6 digit hex RBG, e.g. ff0000 is red.
        |* 8 digit hex ARGB, e.g. ffff0000 is red. The first byte is the [alpha](#alpha) setting
        |  to use with the color.
      """.stripMargin.trim

    override def examples: List[String] = List("a,b,:eq,:sum,ff0000", "a,b,:eq,:sum,f00")
  }

  case object LineStyle extends StyleWord {
    override def name: String = "ls"

    override def summary: String =
      """
        |Set the line style. The value should be one of `line`, `area`, `stack`, or `vspan`.
      """.stripMargin.trim

    override def examples: List[String] = List("a,b,:eq,:sum,(,name,),:by,area")
  }

  case object LineWidth extends StyleWord {
    override def name: String = "lw"

    override def summary: String =
      """
        |The width of the stroke used when drawing the line.
      """.stripMargin.trim

    override def examples: List[String] = List("a,b,:eq,:sum,(,name,),:by,2")
  }

  case object Legend extends StyleWord {
    override def name: String = "legend"

    override def summary: String =
      """
        |Set the legend text.
      """.stripMargin.trim

    override def examples: List[String] = List("a,b,:eq,:sum,(,name,),:by,$name")
  }

  case object Axis extends StyleWord {
    override def name: String = "axis"

    override def summary: String =
      """
        |Specify which Y-axis to use for the line.
      """.stripMargin.trim

    override def examples: List[String] = List("a,b,:eq,:sum,1")
  }

}

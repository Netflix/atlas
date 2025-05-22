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
package com.netflix.atlas.core.model

import java.time.Duration
import java.util.regex.Matcher
import java.util.regex.Pattern

import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.stacklang.StandardVocabulary
import com.netflix.atlas.core.util.Strings

case class StyleExpr(expr: TimeSeriesExpr, settings: Map[String, String]) extends Expr {

  override def append(builder: java.lang.StringBuilder): Unit = {
    Interpreter.append(builder, expr)
    // Use descending order to ensure that an explicit alpha used with
    // a palette is not overwritten when reprocessing the expression string.
    // This works because palette will be sorted before alpha, though a better
    // future solution would be to use a map that preserves the order of
    // updates to the object.
    val vs = settings.toList.sortWith(_._1 > _._1).map {
      case ("sed", v) => v
      case ("ls", v)  => s":$v"
      case (k, v)     => s"$v,:$k"
    }
    vs.foreach { v =>
      builder.append(',').append(v)
    }
  }

  def legend(t: TimeSeries): String = {
    val fmt = settings.getOrElse("legend", t.label)
    sed(Strings.substitute(fmt, t.tags), t.tags)
  }

  def legend(label: String, tags: Map[String, String]): String = {
    val fmt = settings.getOrElse("legend", label)
    sed(Strings.substitute(fmt, tags), tags)
  }

  private def sed(str: String, tags: Map[String, String]): String = {
    settings.get("sed").fold(str) { v =>
      sed(str, v.split(",").toList, tags)
    }
  }

  @scala.annotation.tailrec
  private def sed(str: String, cmds: List[String], tags: Map[String, String]): String = {
    if (cmds.isEmpty) str
    else {
      cmds match {
        case mode :: ":decode" :: cs => sed(decode(str, mode), cs, tags)
        case s :: r :: ":s" :: cs    => sed(searchAndReplace(str, s, r, tags), cs, tags)
        case _                       => sed(str, cmds.tail, tags)
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

  private def searchAndReplace(
    str: String,
    search: String,
    replace: String,
    tags: Map[String, String]
  ): String = {
    val m = Pattern.compile(search).matcher(str)
    if (!m.find()) str
    else {
      val sb = new StringBuffer()
      m.appendReplacement(sb, escape(substitute(replace, m, tags)))
      while (m.find()) {
        m.appendReplacement(sb, escape(substitute(replace, m, tags)))
      }
      m.appendTail(sb)
      sb.toString
    }
  }

  /** Escape string so it is treated like a literal for append replacement. */
  private def escape(str: String): String = {
    str.replace("\\", "\\\\").replace("$", "\\$")
  }

  /**
    * The `appendReplacement` method on the matcher will do substitutions, but this makes
    * it consistent with the variable substitutions for legends to avoid confusion about
    * slightly different syntax for variables in legends verses the replacement field.
    *
    * If a given variable name is not present, then it will fall back to try and resolve
    * the variable based on the tags for the expression.
    */
  private def substitute(str: String, m: Matcher, tags: Map[String, String]): String = {
    Strings.substitute(
      str,
      k => {
        // Default to use if the value cannot be found in the pattern
        val tagValue = tags.getOrElse(k, k)

        // Try to extract from the matcher first, if the variable isn't present, then use
        // the tags as a fallback.
        if (StyleExpr.isNumber(k)) {
          val i = k.toInt
          if (i <= m.groupCount()) m.group(i) else tagValue
        } else {
          // Prior to jdk20 there isn't a good way to detect set of available groups. So
          // for now rely on the exception to detect if the group name doesn't exist.
          try {
            m.group(k)
          } catch {
            case _: IllegalArgumentException => tagValue
          }
        }
      }
    )
  }

  def sortBy: Option[String] = settings.get("sort")

  def useDescending: Boolean = settings.get("order").contains("desc")

  /** Returns the maximum number of lines that should be shown for this expression. */
  def limit: Option[Int] = settings.get("limit").map(_.toInt)

  def axis: Option[Int] = settings.get("axis").map(_.toInt)

  def color: Option[String] = settings.get("color")

  def alpha: Option[Int] = settings.get("alpha").map { s =>
    require(s.length == 2, "value should be 2 digit hex string")
    Integer.parseInt(s, 16)
  }

  def palette: Option[String] = settings.get("palette")

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
    import ModelExtractors.*
    val ctxt = interpreter.execute(s)
    ctxt.stack match {
      case StringListType(vs) :: Nil =>
        vs.map { v =>
          Strings.parseDuration(v)
        }
      case _ => Nil
    }
  }

  private val numberPattern = Pattern.compile("""^(\d+)$""")

  private def isNumber(s: String): Boolean = numberPattern.matcher(s).matches()
}

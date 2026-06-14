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
package com.netflix.atlas.core.model

import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
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
      case ("sed", v)    => v
      case ("ls", v)     => s":$v"
      case ("offset", v) => s"$v,:offset"
      case (k, v)        => s"${Interpreter.escape(v)},:$k"
    }
    vs.foreach { v =>
      builder.append(',').append(v)
    }
  }

  def legend(t: TimeSeries): String = legend(t.label, t.tags)

  def legend(label: => String, tags: Map[String, String]): String = {
    // `label` is by-name so a lazily tag-derived label is not materialized when an explicit
    // legend has been set. When there is no explicit legend, a shifted expression is annotated
    // with its offset so the line can be distinguished. This annotation is applied here, as
    // part of deriving the default label for presentation, rather than in the core evaluation
    // where the label is frequently not consumed. The literal offset is used (rather than the
    // atlas.offset tag) so it resolves even when that tag is not present, e.g. on the fetch path.
    val fmt = settings.get("legend") match {
      case Some(explicit)      => explicit
      case None if offset > 0L => s"$label (offset=${Strings.toString(Duration.ofMillis(offset))})"
      case None                => label
    }
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

  // Patterns are constant for a given expression, so compile each one once and reuse
  // it across the many legend entries (time series) handled by this instance. The
  // matcher itself is stateful and created per legend.
  private val compiledPatterns = new ConcurrentHashMap[String, Pattern]()

  private def searchAndReplace(
    str: String,
    search: String,
    replace: String,
    tags: Map[String, String]
  ): String = {
    val pattern = compiledPatterns.computeIfAbsent(search, s => Pattern.compile(s))
    // Match against a bounded view of the input so catastrophic backtracking (ReDoS)
    // on an attacker-influenced pattern fails fast with a 400 instead of pinning a
    // request thread.
    val m = pattern.matcher(new StyleExpr.BoundedCharSequence(str, search))
    if (!m.find()) str
    else {
      val sb = new java.lang.StringBuilder()
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

  def styleOffsets: List[Duration] = {
    settings.get("offset").fold(List.empty[Duration])(StyleExpr.parseDurationList)
  }

  /**
    * Returns a list of style expressions with one entry per offset specified. This is to support
    * a legacy form of :offset that takes a list and is applied on the style expression. Since
    * further style operations need to get applied to all results we cannot expand until the
    * full expression is evaluated. Should get removed after we have a better story around
    * deprecation.
    *
    * If no high level offset is used then a list with this expression will be returned.
    */
  def perOffset: List[StyleExpr] = {
    styleOffsets match {
      case Nil => List(this)
      case vs  => vs.map(d => copy(expr = expr.withOffset(d), settings = settings - "offset"))
    }
  }
}

object StyleExpr {

  private val interpreter = new Interpreter(StandardVocabulary.allWords)

  private def parseDurationList(s: String): List[Duration] = {
    import com.netflix.atlas.core.stacklang.ast.DataType.*
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

  /**
    * Maximum number of character reads allowed while matching a single legend entry.
    * Catastrophic backtracking reads input characters an exponential number of times,
    * so capping the reads bounds the work for a pathological `:s` pattern. The limit is
    * far above any legitimate legend rewrite (legends are short) while a ReDoS pattern
    * blows past it almost immediately.
    */
  private val MaxRegexCharReads = 1_000_000

  /**
    * View of a string that limits how many characters the regex engine may read while
    * matching. Once the budget is exhausted `charAt` fails the match with an
    * `IllegalArgumentException` (surfaced as a 400) rather than allowing it to run away.
    */
  private final class BoundedCharSequence(seq: CharSequence, pattern: String, reads: Array[Int])
      extends CharSequence {

    // Entry point: start a fresh read budget that is shared with any sub-sequences
    // derived from this instance (reads is a single-element mutable counter).
    def this(seq: CharSequence, pattern: String) = this(seq, pattern, new Array[Int](1))

    override def length(): Int = seq.length()

    override def charAt(index: Int): Char = {
      reads(0) += 1
      if (reads(0) > MaxRegexCharReads)
        throw new IllegalArgumentException(s"regex is too expensive to evaluate: '$pattern'")
      seq.charAt(index)
    }

    // Sub-sequences share the same budget so matching work cannot escape the limit by
    // reading through a sub-sequence view.
    override def subSequence(start: Int, end: Int): CharSequence =
      new BoundedCharSequence(seq.subSequence(start, end), pattern, reads)

    override def toString: String = seq.toString
  }
}

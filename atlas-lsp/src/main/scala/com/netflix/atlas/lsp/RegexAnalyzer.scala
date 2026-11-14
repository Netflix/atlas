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
package com.netflix.atlas.lsp

import scala.util.Try

import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.QueryVocabulary
import com.netflix.spectator.impl.PatternMatcher

/**
  * Analyzes patterns used with `:re` to find common mistakes or cases where a simpler
  * operator would be clearer and more precise.
  *
  *  - Patterns that are equivalent to a starts with, ends with, contains, or exact match
  *    check can use `:starts`, `:ends`, `:contains`, or `:eq`.
  *  - A `.` between two word characters, e.g. `a.b.c`, is likely meant to match a literal
  *    `.` rather than any character. Those operators escape the value so a `.` matches
  *    exactly.
  *  - A `*` following a single literal character, e.g. `foo*`, is likely a glob style wildcard.
  *    In a regex it matches zero or more of the preceding character.
  *
  * Note, the second two suggestions are based on the likely intent of the user. The
  * suggested replacement will match a different set of values than the original pattern.
  */
private[lsp] object RegexAnalyzer {

  /**
    * Suggested change for a regex pattern.
    *
    * @param message
    *     Description of the issue to show the user.
    * @param value
    *     Unescaped value to use for the replacement.
    * @param word
    *     Name of the word to use for the replacement, without the leading colon.
    */
  case class Suggestion(message: String, value: String, word: String)

  /** Characters with special meaning in a regex. */
  private val metaChars = "\\^$.|?*+()[]{}"

  private def isMeta(c: Char): Boolean = metaChars.indexOf(c) >= 0

  private def isWordChar(c: Char): Boolean = Character.isLetterOrDigit(c) || c == '_'

  def analyze(pattern: String): Option[Suggestion] = {
    val stars = findGlobStars(pattern)
    if (stars.nonEmpty) Some(globSuggestion(pattern, stars))
    else basicSuggestion(pattern)
  }

  private def basicSuggestion(pattern: String): Option[Suggestion] = {
    simpleForm(pattern) match {
      case Some((word, value)) =>
        Some(Suggestion(s"pattern is equivalent to :$word, which is simpler", value, word))
      case None =>
        // If the pattern has dots that are likely meant as literals, then the replacement
        // changes the meaning. It is a suggestion based on the likely intent rather than
        // an equivalent transform.
        val escaped = escapeLiteralDots(pattern)
        if (escaped == pattern) None
        else
          simpleForm(escaped).map {
            case (word, value) =>
              val msg =
                s"'.' in a regex matches any character, if a literal '.' is intended use :$word"
              Suggestion(msg, value, word)
          }
    }
  }

  private def globSuggestion(pattern: String, stars: List[Int]): Suggestion = {
    val atoms = stars.map(i => s"'${pattern.substring(atomStart(pattern, i), i + 1)}'")
    val fixed = new java.lang.StringBuilder(pattern.length + stars.length)
    var prev = 0
    stars.foreach { i =>
      fixed.append(pattern, prev, i).append('.')
      prev = i
    }
    fixed.append(pattern, prev, pattern.length)
    val str = fixed.toString

    val msg = s"${atoms.mkString(", ")} matches zero or more of the preceding character, " +
      "use '.*' for a glob style wildcard or '?' if the character is optional"
    simpleForm(str) match {
      case Some((word, value)) =>
        Suggestion(msg, value, word)
      case None =>
        val escaped = escapeLiteralDots(str)
        simpleForm(escaped) match {
          case Some((word, value)) =>
            // The replacement also changes the meaning of the '.' in the pattern, make
            // sure that is called out to the user.
            val note = s"; note :$word matches '.' literally, not any character"
            Suggestion(msg + note, value, word)
          case None =>
            // Since the regex is anchored to the start, a trailing '.*' is redundant
            val value =
              if (str.endsWith(".*") && !isEscaped(str, str.length - 2))
                str.substring(0, str.length - 2)
              else str
            Suggestion(msg, value, "re")
        }
    }
  }

  /**
    * If the pattern is equivalent to a simple operation, then return the word name and the
    * value to use with it. The check for `:starts`, `:ends`, and `:contains` is based on the
    * pattern matcher used for `:re` so it is consistent with how the query will be evaluated.
    */
  private def simpleForm(pattern: String): Option[(String, String)] = {
    Try(PatternMatcher.compile(s"^$pattern")).toOption.flatMap { m =>
      val candidate =
        // Prefix and suffix matchers are also contains matchers, so check them first
        if (m.isPrefixMatcher) Some("starts" -> m.prefix())
        else if (m.isSuffixMatcher) Some("ends" -> m.containedString())
        else if (m.isContainsMatcher) Some("contains" -> m.containedString())
        else exactForm(pattern).map("eq" -> _)
      candidate.filter(t => t._2 != null && t._2.nonEmpty)
    }
  }

  /**
    * If the pattern is a literal string anchored to the end, then return the literal. The
    * check is done by verifying the pattern without the end anchor is exactly the escaped
    * form of the prefix.
    */
  private def exactForm(pattern: String): Option[String] = {
    val p = pattern.stripPrefix("^")
    if (!p.endsWith("$") || isEscaped(p, p.length - 1)) None
    else {
      val body = p.substring(0, p.length - 1)
      Try(PatternMatcher.compile(s"^$body")).toOption
        .filter(_.isPrefixMatcher)
        .map(_.prefix())
        .filter { prefix =>
          QueryVocabulary.Starts.newInstance("_", prefix) match {
            case Query.Regex(_, v) => v == body
            case _                 => false
          }
        }
    }
  }

  /**
    * Escape any unescaped `.` that is between two word characters, e.g. `a.b`. Those are
    * likely meant to match a literal `.`. Dots in other positions, e.g. `5..` or `a.*`, are
    * likely meant as wildcards and are left as is.
    */
  private def escapeLiteralDots(pattern: String): String = {
    val builder = new java.lang.StringBuilder(pattern.length)
    var i = 0
    while (i < pattern.length) {
      val c = pattern.charAt(i)
      if (c == '\\' && i + 1 < pattern.length) {
        builder.append(c).append(pattern.charAt(i + 1))
        i += 2
      } else {
        val literalDot = c == '.' &&
          i > 0 &&
          i + 1 < pattern.length &&
          isWordChar(pattern.charAt(i - 1)) &&
          !isEscaped(pattern, i - 1) &&
          isWordChar(pattern.charAt(i + 1))
        if (literalDot) builder.append('\\')
        builder.append(c)
        i += 1
      }
    }
    builder.toString
  }

  /**
    * Start index of the single character atom ending just before position `i`. An escaped
    * character or a supplementary character (surrogate pair) is treated as a single atom.
    */
  private def atomStart(pattern: String, i: Int): Int = {
    if (i >= 2 && isEscaped(pattern, i - 1)) i - 2
    else if (i >= 2 && Character.isSurrogatePair(pattern.charAt(i - 2), pattern.charAt(i - 1)))
      i - 2
    else i - 1
  }

  /** Returns true if the character at position `i` is escaped with a backslash. */
  private def isEscaped(pattern: String, i: Int): Boolean = {
    var n = 0
    var j = i - 1
    while (j >= 0 && pattern.charAt(j) == '\\') {
      n += 1
      j -= 1
    }
    n % 2 == 1
  }

  /**
    * Find the positions of each `*` that follows a single literal character. That is a
    * common mistake when the user is thinking of glob patterns. Returns an empty list if
    * the pattern has other regex features beyond `.`, `.*`, a leading `^`, or a trailing
    * `$` that make the intent less clear.
    */
  private def findGlobStars(pattern: String): List[Int] = {
    var i = 0
    val result = List.newBuilder[Int]
    while (i < pattern.length) {
      val c = pattern.charAt(i)
      if (c == '\\') {
        if (i + 1 >= pattern.length || !isMeta(pattern.charAt(i + 1))) return Nil
        i += 2
      } else if (c == '*') {
        val prev = if (i > 0) pattern.charAt(i - 1) else '*'
        val literalPrev = isEscaped(pattern, i - 1) || !isMeta(prev)
        if (literalPrev) result += i
        else if (prev != '.') return Nil
        i += 1
      } else if (
        c == '.' || !isMeta(c) || (c == '^' && i == 0) || (c == '$' && i == pattern.length - 1)
      ) {
        i += 1
      } else {
        return Nil
      }
    }
    result.result()
  }
}

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
package com.netflix.atlas.core.util

import java.util.regex.Matcher
import java.util.regex.Pattern

sealed trait StringMatcher {

  def prefix: Option[String]

  def matches(v: String): Boolean
}

object StringMatcher {

  /** Or queries. */
  private val AnchoredOrPattern = """^\^+\(([-_a-zA-Z0-9.*+]+\|[-_a-zA-Z0-9.*+|]+)\)\$+$""".r
  private val StartOrPattern = """^\^+\(([-_a-zA-Z0-9.*+]+\|[-_a-zA-Z0-9.*+|]+)\)\$*$""".r
  private val FloatingOrPattern = """^\(?([-_a-zA-Z0-9.*+^$]+\|[-_a-zA-Z0-9.*+^$|]+)\)?$""".r

  /** Matches everything. */
  private val AllPattern = """^\^*(\.\*)*\$*$""".r

  /** A pattern that can be checked with the String.equals method. */
  private val EqualsPattern = """^\^+([\-_a-zA-Z0-9]+)\$+$""".r

  /** A pattern that can be checked with the String.startsWith method. */
  private val StartsWithPattern = """^\^+([\-_a-zA-Z0-9]+)(?:\.\*)*$""".r

  /** A pattern that can be checked with the String.indexOf method. */
  private val IndexOfPattern = """^(?:\^*(?:\.\*)+)*([\-_a-zA-Z0-9]+)(?:(?:\.\*)+\$*)*$""".r

  /** A regex that has a simple prefix along with some arbitrary pattern. */
  private val PrefixPattern = """^\^+([\-_a-zA-Z0-9]+).*$""".r

  /**
    * Convert the string pattern into a matcher object. If possible, it will use a simple
    * string matcher and avoid expensive regex operations.
    *
    * @param pattern
    *     Regular expression string to compile.
    * @param caseSensitive
    *     Whether or not the match should be case sensitive. The default is true. Case
    *     insensitive matching is more expensive and breaks the ability to reliably match
    *     the prefix of queries. Avoid if possible.
    * @return
    *     Matcher based on the provided pattern.
    */
  def compile(pattern: String, caseSensitive: Boolean = true): StringMatcher = {
    pattern match {
      case EqualsPattern(p) if caseSensitive      => Equals(p)
      case EqualsPattern(p) if !caseSensitive     => EqualsIgnoreCase(p)
      case StartsWithPattern(p) if caseSensitive  => StartsWith(p)
      case StartsWithPattern(p) if !caseSensitive => default(pattern, caseSensitive)
      case IndexOfPattern(s) if caseSensitive     => IndexOf(s)
      case IndexOfPattern(s) if !caseSensitive    => IndexOfIgnoreCase(s)
      case AllPattern(_)                          => All
      case AnchoredOrPattern(s) if caseSensitive  => compileOr(s, v => s"^$v$$")
      case StartOrPattern(s) if caseSensitive     => compileOr(s, v => s"^$v")
      case FloatingOrPattern(s) if caseSensitive  => compileOr(s, v => s"$v")
      case PrefixPattern(p) if caseSensitive      => PrefixedRegex(p, Pattern.compile(pattern))
      case _                                      => default(pattern, caseSensitive)
    }
  }

  private def compileOr(s: String, f: String => String): StringMatcher = {
    Or(s.split("\\|").toList.map(v => compile(f(v))))
  }

  private def default(re: String, caseSensitive: Boolean = true): StringMatcher = {
    val flags = if (caseSensitive) 0 else Pattern.CASE_INSENSITIVE
    Regex(Pattern.compile(re, flags))
  }

  /** Match all strings. */
  case object All extends StringMatcher {
    val prefix: Option[String] = None

    def matches(v: String): Boolean = true
  }

  case class PrefixedRegex(prefixStr: String, pattern: Pattern) extends StringMatcher {

    val prefix: Option[String] = Some(prefixStr)

    // Reuse matcher to reduce allocations, keep in thread local in-case this Regex instance is
    // shared across multiple threads.
    private val matcher = new ThreadLocal[Matcher] {

      override protected def initialValue: Matcher = pattern.matcher("")
    }

    private def reMatches(v: String): Boolean = {
      val m = matcher.get
      m.reset(v)
      m.find
    }

    def matches(v: String): Boolean = {
      // Check with startsWith first to avoid using regex as much as possible. When using
      // for streaming evaluation, this is a big win. When used with the index, we already
      // know the prefix matches so it is a bit of extra overhead, but not too bad.
      v.startsWith(prefixStr) && reMatches(v)
    }

    override def equals(obj: Any): Boolean = {
      obj match {
        case PrefixedRegex(p, re) =>
          prefixStr == p && pattern.pattern() == re.pattern() && pattern.flags() == re.flags()
        case _ => false
      }
    }
  }

  case class Regex(pattern: Pattern) extends StringMatcher {
    val prefix: Option[String] = None

    // Reuse matcher to reduce allocations, keep in thread local in-case this Regex instance is
    // shared across multiple threads.
    private val matcher = new ThreadLocal[Matcher] {

      override protected def initialValue: Matcher = pattern.matcher("")
    }

    def matches(v: String): Boolean = {
      val m = matcher.get
      m.reset(v)
      m.find
    }

    override def equals(obj: Any): Boolean = {
      obj match {
        case Regex(re) =>
          pattern.pattern() == re.pattern() && pattern.flags() == re.flags()
        case _ => false
      }
    }
  }

  case class StartsWith(p: String) extends StringMatcher {
    val prefix: Option[String] = Some(p)

    def matches(v: String): Boolean = v.startsWith(p)
  }

  case class Equals(p: String) extends StringMatcher {
    val prefix: Option[String] = Some(p)

    def matches(v: String): Boolean = p == v
  }

  case class EqualsIgnoreCase(p: String) extends StringMatcher {
    val prefix: Option[String] = None

    def matches(v: String): Boolean = p.equalsIgnoreCase(v)
  }

  case class IndexOf(substr: String) extends StringMatcher {
    val prefix: Option[String] = None

    def matches(v: String): Boolean = v.indexOf(substr) != -1
  }

  case class IndexOfIgnoreCase(substr: String) extends StringMatcher {
    val prefix: Option[String] = None

    def matches(v: String): Boolean = {
      val end = (v.length - substr.length) + 1
      var i = 0
      while (i < end) {
        if (v.regionMatches(true, i, substr, 0, substr.length)) return true
        i += 1
      }
      false
    }
  }

  case class And(matchers: List[StringMatcher]) extends StringMatcher {
    val prefix: Option[String] = matchers.head.prefix

    def matches(v: String): Boolean = matchers.forall(_.matches(v))
  }

  case class Or(matchers: List[StringMatcher]) extends StringMatcher {
    val prefix: Option[String] = None

    def matches(v: String): Boolean = matchers.exists(_.matches(v))
  }
}

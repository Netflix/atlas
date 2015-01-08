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
package com.netflix.atlas.core.util

import java.util.regex.Matcher
import java.util.regex.Pattern

sealed trait StringMatcher {
  def prefix: Option[String]
  def matches(v: String): Boolean
}

object StringMatcher {

  /** Matches everything. */
  private val AllPattern = """^\^*(\.\*)*\$*$""".r

  /** A pattern that can be checked with the String.startsWith method. */
  private val StartsWithPattern = """^\^+([\-_a-zA-Z0-9]+)(?:\.\*)*$""".r

  /** A pattern that can be checked with the String.indexOf method. */
  private val IndexOfPattern = """^\^*(?:\.\*)*([\-_a-zA-Z0-9]+)(?:\.\*)*\$*$""".r

  /** A regex that has a simple prefix along with some arbitrary pattern. */
  private val PrefixPattern = """^\^+([\-_a-zA-Z0-9]+).*$""".r

  def compile(pattern: String, caseSensitive: Boolean = true): StringMatcher = {
    pattern match {
      case StartsWithPattern(p) if caseSensitive  => StartsWith(p)
      case StartsWithPattern(p) if !caseSensitive => default(pattern, caseSensitive)
      case IndexOfPattern(s) if caseSensitive     => IndexOf(s)
      case IndexOfPattern(s) if !caseSensitive    => IndexOfIgnoreCase(s)
      case AllPattern(_)                          => All
      case PrefixPattern(p) if caseSensitive      => Regex(Some(p), Pattern.compile(pattern))
      case _                                      => default(pattern, caseSensitive)
    }
  }

  private def default(re: String, caseSensitive: Boolean): StringMatcher = {
    val flags = if (caseSensitive) 0 else Pattern.CASE_INSENSITIVE
    Regex(None, Pattern.compile(re, flags))
  }

  /** Match all strings. */
  case object All extends StringMatcher {
    val prefix: Option[String] = None
    def matches(v: String): Boolean = true
  }

  case class Regex(prefix: Option[String], pattern: Pattern) extends StringMatcher {
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
        case Regex(p, re) =>
          prefix == p && pattern.pattern() == re.pattern() && pattern.flags() == re.flags()
        case _ => false
      }
    }
  }

  case class StartsWith(p: String) extends StringMatcher {
    val prefix: Option[String] = Some(p)
    def matches(v: String): Boolean = v.startsWith(p)
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
}



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

import com.netflix.atlas.core.stacklang.Interpreter

/**
  * String format for expressions that is intended for display to users. Regex queries that
  * are equivalent to `:starts`, `:contains`, or `:ends` will use those words since they are
  * easier to read, for example `a.b.c,:starts` rather than `a\.b\.c,:re`.
  *
  * The result will parse to an equal expression, but the string may vary across versions.
  * Use `toString` where a stable string is needed, such as for ids or keys.
  */
object ReadableExprFormat {

  private val words = List(QueryVocabulary.Starts, QueryVocabulary.Ends, QueryVocabulary.Contains)

  /** Return the readable string form of the expression. */
  def apply(expr: Expr): String = rewrite(expr.toString)

  /**
    * Rewrite the output of `toString` for an expression. Values in that format are escaped
    * so a comma will always be a separator and a token starting with ':' will always be a
    * word. This is not safe for arbitrary user input that may have whitespace or comments.
    */
  private[model] def rewrite(str: String): String = {
    if (!str.contains(",:re")) str
    else {
      val tokens = str.split(",", -1)
      var i = 1
      while (i < tokens.length) {
        if (tokens(i) == ":re") {
          simpleForm(Interpreter.unescape(tokens(i - 1))).foreach {
            case (word, value) =>
              tokens(i - 1) = Interpreter.escape(value)
              tokens(i) = s":$word"
          }
        }
        i += 1
      }
      tokens.mkString(",")
    }
  }

  /**
    * If the regex is equivalent to using `:starts`, `:contains`, or `:ends`, then return
    * the word name and value. The check is done by creating the query using the simpler
    * word and verifying it is equal to the original, so the result will always parse back
    * to an equal query.
    */
  private def simpleForm(pattern: String): Option[(String, String)] = {
    val q = Query.Regex("_", pattern)
    val p = q.pattern
    // Prefix and suffix matchers are also contains matchers, so they must be checked first
    val candidate =
      if (p.isPrefixMatcher) p.prefix()
      else if (p.isSuffixMatcher || p.isContainsMatcher) p.containedString()
      else null
    if (candidate == null || candidate.isEmpty) None
    else words.find(_.newInstance("_", candidate) == q).map(w => w.name -> candidate)
  }
}

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

import org.eclipse.lsp4j.SemanticTokenTypes
import org.eclipse.lsp4j.SemanticTokensLegend

/**
  * Token type indices for Atlas semantic tokens. The index values must match the
  * position in the `tokenTypes` list registered in the legend.
  */
object AslTokenTypes {

  /** A known word/operator like :dup, :swap, :eq. */
  val Word: Int = 0

  /** A string literal value. */
  val String: Int = 1

  /** A numeric literal value. */
  val Number: Int = 2

  /** Parenthesis for list grouping. */
  val Parenthesis: Int = 3

  /** An unrecognized word (for error highlighting). */
  val UnknownWord: Int = 4

  /** A comment delimited by `/* ... */`. */
  val Comment: Int = 5

  /** A URI parameter name (q, s, e, w, h, etc.). */
  val Parameter: Int = 6

  /** A URI operator character (?, &, =). */
  val UriOperator: Int = 7

  /** A URI path (/api/v1/graph). */
  val Path: Int = 8

  private val tokenTypes = java.util.List.of(
    SemanticTokenTypes.Function, // Word
    SemanticTokenTypes.String, // String
    SemanticTokenTypes.Number, // Number
    SemanticTokenTypes.Operator, // Parenthesis
    SemanticTokenTypes.Variable, // UnknownWord (variable as fallback)
    SemanticTokenTypes.Comment, // Comment
    SemanticTokenTypes.Parameter, // Parameter
    SemanticTokenTypes.Operator, // UriOperator
    SemanticTokenTypes.Namespace // Path
  )

  private val tokenModifiers = java.util.List.of[String]()

  val legend: SemanticTokensLegend = new SemanticTokensLegend(tokenTypes, tokenModifiers)
}

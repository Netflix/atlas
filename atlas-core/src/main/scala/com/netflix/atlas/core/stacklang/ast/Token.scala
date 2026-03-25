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
package com.netflix.atlas.core.stacklang.ast

/** A token produced by the tokenizer with position information. */
sealed trait Token {

  /** The encompassing span of this token in the source string. */
  def span: Span
}

/**
  * A value token with its text and source position fragments. When a comment is embedded
  * in a token (e.g., {{{:d/*c*/up}}} producing value `:dup`), the spans list contains
  * the disjoint source fragments that make up the value.
  */
case class ValueToken(value: String, spans: List[Span]) extends Token {

  require(spans.nonEmpty, "spans must not be empty")
  def span: Span = Span(spans.head.start, spans.last.end)
}

/** A comment token delimited by {{{/* ... */}}}. */
case class CommentToken(text: String, span: Span) extends Token

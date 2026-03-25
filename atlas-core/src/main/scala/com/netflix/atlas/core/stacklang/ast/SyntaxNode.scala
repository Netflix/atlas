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

import com.netflix.atlas.core.stacklang.Word

/** A node in the syntax tree. */
sealed trait SyntaxNode {
  def span: Span
}

/** A literal value that gets pushed onto the stack. */
case class LiteralNode(token: ValueToken) extends SyntaxNode {
  def span: Span = token.span
}

/** A word/operator reference (e.g. :dup, :eq). */
case class WordNode(
  token: ValueToken,
  word: Option[Word],
  stack: List[Any],
  diagnostic: Option[Diagnostic]
) extends SyntaxNode {

  def span: Span = token.span
}

/** A parenthesized list group. */
case class ListNode(
  open: ValueToken,
  children: List[SyntaxNode],
  close: Option[ValueToken],
  diagnostic: Option[Diagnostic]
) extends SyntaxNode {

  def span: Span = {
    val endPos = close
      .map(_.span.end)
      .orElse(children.lastOption.map(_.span.end))
      .getOrElse(open.span.end)
    Span(open.span.start, endPos)
  }
}

/** A comment delimited by `/* ... */`, possibly nested. */
case class CommentNode(token: CommentToken) extends SyntaxNode {
  def span: Span = token.span
}

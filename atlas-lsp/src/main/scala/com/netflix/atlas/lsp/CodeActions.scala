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

import scala.jdk.CollectionConverters.*

import org.eclipse.lsp4j.CodeAction
import org.eclipse.lsp4j.Command
import org.eclipse.lsp4j.Position
import org.eclipse.lsp4j.Range
import org.eclipse.lsp4j.jsonrpc.messages.Either

/** Helper functions for working with code actions. */
object CodeActions {

  /**
    * Filter the code actions to those that are relevant for the requested range. Actions
    * that are associated with diagnostics, such as quick fixes, are kept if any of the
    * diagnostics overlap the range. Actions without diagnostics apply to the whole document,
    * such as formatting, and are always kept.
    *
    * @param actions
    *     Code actions computed for the document.
    * @param range
    *     Range from the code action request, typically the cursor position or selection.
    *     If null, then all actions will be returned.
    */
  def filterByRange(
    actions: List[Either[Command, CodeAction]],
    range: Range
  ): List[Either[Command, CodeAction]] = {
    if (range == null) actions
    else actions.filter(a => a.isLeft || isRelevant(a.getRight, range))
  }

  private def isRelevant(action: CodeAction, range: Range): Boolean = {
    val diagnostics = action.getDiagnostics
    diagnostics == null || diagnostics.isEmpty || diagnostics.asScala.exists { d =>
      d.getRange == null || overlaps(d.getRange, range)
    }
  }

  /** Check if the ranges overlap. Ranges that touch at the end points are considered to overlap. */
  private[lsp] def overlaps(a: Range, b: Range): Boolean = {
    compare(a.getStart, b.getEnd) <= 0 && compare(b.getStart, a.getEnd) <= 0
  }

  private def compare(a: Position, b: Position): Int = {
    if (a.getLine != b.getLine) Integer.compare(a.getLine, b.getLine)
    else Integer.compare(a.getCharacter, b.getCharacter)
  }
}

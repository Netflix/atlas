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

import org.eclipse.lsp4j.CodeAction
import org.eclipse.lsp4j.Command
import org.eclipse.lsp4j.Diagnostic
import org.eclipse.lsp4j.Position
import org.eclipse.lsp4j.Range
import org.eclipse.lsp4j.jsonrpc.messages.Either
import munit.FunSuite

class CodeActionsSuite extends FunSuite {

  private def range(l1: Int, c1: Int, l2: Int, c2: Int): Range = {
    new Range(new Position(l1, c1), new Position(l2, c2))
  }

  private def action(title: String, diagRanges: Range*): Either[Command, CodeAction] = {
    val a = new CodeAction(title)
    if (diagRanges.nonEmpty) {
      val diags = diagRanges.map(r => new Diagnostic(r, title))
      a.setDiagnostics(java.util.List.of(diags*))
    }
    Either.forRight(a)
  }

  private def titles(actions: List[Either[Command, CodeAction]]): List[String] = {
    actions.map(a => if (a.isLeft) a.getLeft.getTitle else a.getRight.getTitle)
  }

  test("overlaps") {
    assert(CodeActions.overlaps(range(0, 2, 0, 5), range(0, 3, 0, 3)))
    assert(CodeActions.overlaps(range(0, 2, 0, 5), range(0, 5, 0, 5)))
    assert(CodeActions.overlaps(range(0, 2, 0, 5), range(0, 2, 0, 2)))
    assert(CodeActions.overlaps(range(0, 2, 0, 5), range(0, 0, 1, 0)))
    assert(!CodeActions.overlaps(range(0, 2, 0, 5), range(0, 6, 0, 8)))
    assert(!CodeActions.overlaps(range(0, 2, 0, 5), range(0, 0, 0, 1)))
    assert(!CodeActions.overlaps(range(0, 2, 0, 5), range(1, 3, 1, 3)))
  }

  test("filterByRange") {
    val actions = List(
      action("a", range(0, 0, 0, 3)),
      action("b", range(0, 10, 0, 12)),
      action("doc"),
      Either.forLeft[Command, CodeAction](new Command("cmd", "cmd")),
      action("multi", range(0, 20, 0, 21), range(0, 1, 0, 2))
    )
    assertEquals(
      titles(CodeActions.filterByRange(actions, range(0, 1, 0, 1))),
      List("a", "doc", "cmd", "multi")
    )
    assertEquals(
      titles(CodeActions.filterByRange(actions, range(0, 11, 0, 11))),
      List("b", "doc", "cmd")
    )
    assertEquals(titles(CodeActions.filterByRange(actions, null)), titles(actions))
  }
}

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
package com.netflix.atlas.core.stacklang

import com.netflix.atlas.core.model.StyleVocabulary
import com.netflix.atlas.core.stacklang.ast.*
import munit.FunSuite

class SyntaxTreeSuite extends FunSuite {

  private val interpreter = Interpreter(StandardVocabulary.allWords)
  private val styleInterpreter = Interpreter(StyleVocabulary.allWords)

  private def valueTokens(str: String): List[ValueToken] = {
    Interpreter.tokenize(str).collect { case vt: ValueToken => vt }
  }

  private def commentTokens(str: String): List[CommentToken] = {
    Interpreter.tokenize(str).collect { case ct: CommentToken => ct }
  }

  //
  // tokenize: basic value tokens
  //

  test("tokenize: simple tokens") {
    val tokens = valueTokens("a,b,c")
    assertEquals(tokens.map(_.value), List("a", "b", "c"))
    assertEquals(tokens.map(_.span), List(Span(0, 1), Span(2, 3), Span(4, 5)))
  }

  test("tokenize: whitespace trimming preserves position") {
    val tokens = valueTokens("  a , b , c  ")
    assertEquals(tokens.map(_.value), List("a", "b", "c"))
    assertEquals(tokens(0).span, Span(2, 3))
    assertEquals(tokens(1).span, Span(6, 7))
    assertEquals(tokens(2).span, Span(10, 11))
  }

  test("tokenize: empty segments skipped") {
    val tokens = valueTokens("a,,b,  ,c")
    assertEquals(tokens.map(_.value), List("a", "b", "c"))
  }

  test("tokenize: leading and trailing commas") {
    val tokens = valueTokens(",a,b,")
    assertEquals(tokens.map(_.value), List("a", "b"))
  }

  test("tokenize: empty string") {
    assertEquals(Interpreter.tokenize(""), Nil)
  }

  test("tokenize: word tokens") {
    val tokens = valueTokens("a,:dup,:swap")
    assertEquals(tokens.map(_.value), List("a", ":dup", ":swap"))
    assertEquals(tokens(1).span, Span(2, 6))
    assertEquals(tokens(2).span, Span(7, 12))
  }

  test("tokenize: consistent with splitAndTrim values") {
    val exprs = List(
      "a,b,c",
      "  a , b ",
      ":dup,:swap",
      "(,a,b,)",
      ",,a,,b,,"
    )
    exprs.foreach { expr =>
      val tokenValues = valueTokens(expr).map(_.value)
      val splitValues = Interpreter.splitAndTrim(expr)
      assertEquals(tokenValues, splitValues, s"mismatch for: $expr")
    }
  }

  //
  // tokenize: comments
  //

  test("tokenize: standalone comment") {
    val tokens = Interpreter.tokenize("a,/* comment */,b")
    val values = tokens.collect { case vt: ValueToken => vt.value }
    val comments = tokens.collect { case ct: CommentToken => ct.text }
    assertEquals(values, List("a", "b"))
    assertEquals(comments, List("/* comment */"))
  }

  test("tokenize: comment in its own segment") {
    val tokens = Interpreter.tokenize("a,/* comment */b,:eq")
    val values = tokens.collect { case vt: ValueToken => vt.value }
    val comments = tokens.collect { case ct: CommentToken => ct.text }
    assertEquals(values, List("a", "b", ":eq"))
    assertEquals(comments, List("/* comment */"))
  }

  test("tokenize: comment embedded in value") {
    val tokens = Interpreter.tokenize("a,:d/*comment*/up")
    val values = tokens.collect { case vt: ValueToken => vt }
    assertEquals(values.size, 2)
    assertEquals(values(0).value, "a")
    assertEquals(values(1).value, ":dup")
    assertEquals(values(1).spans.size, 2) // :d and up are separate fragments
  }

  test("tokenize: comment spanning commas") {
    // a,b,:d/*,d,e,c,*/up is equivalent to a,b,:dup
    val tokens = Interpreter.tokenize("a,b,:d/*,d,e,c,*/up")
    val values = tokens.collect { case vt: ValueToken => vt.value }
    val comments = tokens.collect { case ct: CommentToken => ct.text }
    assertEquals(values, List("a", "b", ":dup"))
    assertEquals(comments, List("/*,d,e,c,*/"))
  }

  test("tokenize: comment at start of expression") {
    val tokens = Interpreter.tokenize("/* start */a,b")
    val values = tokens.collect { case vt: ValueToken => vt.value }
    assertEquals(values, List("a", "b"))
    assertEquals(commentTokens("/* start */a,b").size, 1)
  }

  test("tokenize: comment at end of expression") {
    val tokens = Interpreter.tokenize("a,b/* end */")
    val values = tokens.collect { case vt: ValueToken => vt.value }
    assertEquals(values, List("a", "b"))
  }

  test("tokenize: multiple comments in one segment") {
    val tokens = Interpreter.tokenize("/*c1*/:d/*c2*/up")
    val values = tokens.collect { case vt: ValueToken => vt }
    val comments = tokens.collect { case ct: CommentToken => ct }
    assertEquals(values.size, 1)
    assertEquals(values.head.value, ":dup")
    assertEquals(values.head.spans.size, 2)
    assertEquals(comments.size, 2)
  }

  test("tokenize: nested comments") {
    val tokens = Interpreter.tokenize("a,/* outer /* inner */ end */,b")
    val values = tokens.collect { case vt: ValueToken => vt.value }
    assertEquals(values, List("a", "b"))
  }

  test("tokenize: unclosed comment throws") {
    intercept[IllegalStateException] {
      Interpreter.tokenize("a,/* unclosed")
    }
  }

  test("tokenize: unmatched close throws") {
    intercept[IllegalStateException] {
      Interpreter.tokenize("a,*/b")
    }
  }

  test("tokenize: source order preserved") {
    val tokens = Interpreter.tokenize("/*c1*/a,/*c2*/b")
    val starts = tokens.map(_.span.start)
    assertEquals(starts, starts.sorted)
  }

  //
  // syntaxTree: literals
  //

  test("syntaxTree: single literal") {
    val tree = interpreter.syntaxTree("a")
    assertEquals(tree.nodes.size, 1)
    assert(tree.nodes.head.isInstanceOf[LiteralNode])
    assertEquals(tree.diagnostics, Nil)
    assertEquals(tree.stack, List("a"))
  }

  test("syntaxTree: multiple literals") {
    val tree = interpreter.syntaxTree("a,b,c")
    assertEquals(tree.nodes.size, 3)
    assert(tree.nodes.forall(_.isInstanceOf[LiteralNode]))
    assertEquals(tree.stack, List("c", "b", "a"))
  }

  //
  // syntaxTree: words
  //

  test("syntaxTree: known word") {
    val tree = interpreter.syntaxTree("a,b,:swap")
    assertEquals(tree.nodes.size, 3)
    val wordNode = tree.nodes(2).asInstanceOf[WordNode]
    assert(wordNode.word.isDefined)
    assertEquals(wordNode.word.get.name, "swap")
    assertEquals(wordNode.stack, List("b", "a")) // stack before execution
    assertEquals(wordNode.diagnostic, None)
    assertEquals(tree.stack, List("a", "b")) // stack after swap
  }

  test("syntaxTree: word node has correct span") {
    val tree = interpreter.syntaxTree("a,b,:swap")
    val wordNode = tree.nodes(2).asInstanceOf[WordNode]
    assertEquals(wordNode.span, Span(4, 9))
  }

  //
  // syntaxTree: error recovery
  //

  test("syntaxTree: unknown word produces diagnostic and continues") {
    val tree = interpreter.syntaxTree("a,:unknown,b")
    assertEquals(tree.nodes.size, 3)
    val wordNode = tree.nodes(1).asInstanceOf[WordNode]
    assertEquals(wordNode.word, None)
    assert(wordNode.diagnostic.isDefined)
    assert(wordNode.diagnostic.get.message.contains("unknown word"))
    assertEquals(wordNode.diagnostic.get.severity, Severity.Error)
    // Processing continued: "b" was still pushed
    assertEquals(tree.stack, List("b", "a"))
    assertEquals(tree.diagnostics.size, 1)
  }

  test("syntaxTree: stack mismatch produces diagnostic and continues") {
    // :swap requires 2 items, only 1 on stack
    val tree = interpreter.syntaxTree("a,:swap,b")
    assertEquals(tree.nodes.size, 3)
    val wordNode = tree.nodes(1).asInstanceOf[WordNode]
    assert(wordNode.diagnostic.isDefined)
    assertEquals(wordNode.diagnostic.get.severity, Severity.Error)
    // Processing continued
    assertEquals(tree.stack, List("b", "a"))
    assertEquals(tree.diagnostics.size, 1)
  }

  //
  // syntaxTree: lists
  //

  test("syntaxTree: simple list") {
    val tree = interpreter.syntaxTree("(,a,b,)")
    assertEquals(tree.nodes.size, 1)
    val listNode = tree.nodes.head.asInstanceOf[ListNode]
    assert(listNode.close.isDefined)
    assertEquals(listNode.children.size, 2)
    assertEquals(listNode.diagnostic, None)
    // List should be on the stack
    assertEquals(tree.stack, List(List("a", "b")))
  }

  test("syntaxTree: unmatched opening paren") {
    val tree = interpreter.syntaxTree("(,a,b")
    assertEquals(tree.diagnostics.size, 1)
    assert(tree.diagnostics.head.message.contains("unmatched opening parenthesis"))
  }

  test("syntaxTree: unmatched closing paren") {
    val tree = interpreter.syntaxTree("a,)")
    assertEquals(tree.diagnostics.size, 1)
    assert(tree.diagnostics.head.message.contains("unmatched closing parenthesis"))
  }

  //
  // syntaxTree: round-trip consistency with execute
  //

  test("syntaxTree: final stack matches execute for valid expressions") {
    val exprs = List(
      "a,b,c",
      "a,b,:swap",
      "a,:dup",
      "a,b,:swap,:dup",
      "(,a,b,)"
    )
    exprs.foreach { expr =>
      val tree = interpreter.syntaxTree(expr)
      val ctx = interpreter.execute(expr)
      assertEquals(tree.stack, ctx.stack, s"stack mismatch for: $expr")
    }
  }

  //
  // syntaxTree: unstable words
  //

  test("syntaxTree: unstable word produces warning diagnostic") {
    val unstableWord = new Word {
      def name: String = "experimental"
      def matches(stack: List[Any]): Boolean = true
      def execute(context: Context): Context =
        context.copy(stack = "exp" :: context.stack)
      def summary: String = ""
      def signature: String = "-- exp"
      def examples: List[String] = Nil
      override def isStable: Boolean = false
    }
    val interp = Interpreter(StandardVocabulary.allWords :+ unstableWord)
    val tree = interp.syntaxTree(":experimental")
    assertEquals(tree.nodes.size, 1)
    val wordNode = tree.nodes.head.asInstanceOf[WordNode]
    assert(wordNode.word.isDefined)
    assert(wordNode.diagnostic.isDefined)
    assertEquals(wordNode.diagnostic.get.severity, Severity.Warning)
    assert(wordNode.diagnostic.get.message.contains("unstable"))
    // Word still executed successfully
    assertEquals(tree.stack, List("exp"))
    // Warning appears in tree diagnostics
    assertEquals(tree.diagnostics.size, 1)
    assertEquals(tree.diagnostics.head.severity, Severity.Warning)
  }

  //
  // syntaxTree: span accuracy
  //

  test("syntaxTree: spans point to correct substrings") {
    val expr = "abc, :dup , xyz"
    val tree = interpreter.syntaxTree(expr)
    tree.nodes.foreach { node =>
      val span = node.span
      val substr = expr.substring(span.start, span.end)
      node match {
        case LiteralNode(token)       => assertEquals(substr, token.value)
        case WordNode(token, _, _, _) => assertEquals(substr, token.value)
        case _                        =>
      }
    }
  }

  //
  // syntaxTree: comments
  //

  test("syntaxTree: comment produces CommentNode") {
    val tree = interpreter.syntaxTree("a,/* comment */,b")
    val comments = tree.nodes.collect { case c: CommentNode => c }
    assertEquals(comments.size, 1)
    assertEquals(comments.head.token.text, "/* comment */")
  }

  test("syntaxTree: comment embedded in word") {
    val tree = interpreter.syntaxTree("a,:d/*comment*/up")
    val words = tree.nodes.collect { case w: WordNode => w }
    assertEquals(words.size, 1)
    assertEquals(words.head.token.value, ":dup")
    assert(words.head.word.isDefined)
    assertEquals(words.head.word.get.name, "dup")
    assertEquals(tree.stack, List("a", "a")) // a,:dup -> a,a
  }

  test("syntaxTree: comment spanning commas") {
    val tree = interpreter.syntaxTree("a,b,:d/*,d,e,c,*/up")
    val words = tree.nodes.collect { case w: WordNode => w }
    assertEquals(words.size, 1)
    assertEquals(words.head.token.value, ":dup")
    assert(words.head.word.isDefined)
    assertEquals(tree.stack, List("b", "b", "a")) // a,b,:dup -> b,b,a
  }

  test("syntaxTree: expression with comments matches execute result") {
    val exprs = List(
      "a,/* comment */b,:swap",
      "a,b,:d/* c */up",
      "/* start */a,b,:swap/* end */"
    )
    exprs.foreach { expr =>
      val tree = interpreter.syntaxTree(expr)
      val ctx = interpreter.execute(expr)
      assertEquals(tree.stack, ctx.stack, s"stack mismatch for: $expr")
    }
  }

  //
  // syntaxTree: semicolon typo detection
  //

  test("syntaxTree: semicolon typo for known word produces warning") {
    val tree = interpreter.syntaxTree("a,b,;swap")
    val warnings = tree.diagnostics.filter(_.severity == Severity.Warning)
    assertEquals(warnings.size, 1)
    assert(warnings.head.message.contains(":swap"))
    // Still pushed as literal
    assertEquals(tree.stack, List(";swap", "b", "a"))
  }

  test("syntaxTree: semicolon with unknown name produces no warning") {
    val tree = interpreter.syntaxTree(";unknown")
    val warnings = tree.diagnostics.filter(_.severity == Severity.Warning)
    assertEquals(warnings, Nil)
    assertEquals(tree.stack, List(";unknown"))
  }

  test("syntaxTree: deprecated word produces warning") {
    val tree = styleInterpreter.syntaxTree("name,sps,:eq,:sum,(,0h,1d,1w,),:offset")
    val warnings = tree.diagnostics.filter(_.severity == Severity.Warning)
    assert(warnings.exists(_.message.contains("deprecated")))
    // Word still executes and stack is correct
    assert(tree.stack.nonEmpty)
  }

  test("syntaxTree: empty expression") {
    val tree = interpreter.syntaxTree("")
    assertEquals(tree.nodes, Nil)
    assertEquals(tree.diagnostics, Nil)
    assertEquals(tree.stack, Nil)
  }

  test("syntaxTree: nested list") {
    val tree = interpreter.syntaxTree("(,(,a,b,),c,)")
    assertEquals(tree.diagnostics, Nil)
    val listNode = tree.nodes.head.asInstanceOf[ListNode]
    // Outer list has a nested ListNode and a LiteralNode
    val innerList = listNode.children.collectFirst { case l: ListNode => l }
    assert(innerList.isDefined, "expected nested ListNode")
    assertEquals(tree.stack.size, 1)
    assert(tree.stack.head.isInstanceOf[List[?]])
  }

  test("syntaxTree: list followed by word") {
    val tree = interpreter.syntaxTree("(,a,b,),:dup")
    assertEquals(tree.diagnostics, Nil)
    assertEquals(tree.stack.size, 2) // two copies of the list
    assertEquals(tree.stack.head, tree.stack(1))
  }

  test("syntaxTree: multiple errors collected") {
    val tree = interpreter.syntaxTree(":unknown1,:unknown2")
    assertEquals(tree.diagnostics.size, 2)
    assert(tree.diagnostics.forall(_.severity == Severity.Error))
  }

  test("syntaxTree: execution error recovery does not crash") {
    // :each matches but the body (:unknown) fails at runtime. The error
    // recovery path should catch the exception and continue processing.
    val tree = interpreter.syntaxTree("(,a,b,),(,:unknown,),:each")
    // The :each word should be recorded as matched despite the failure
    val eachNode = tree.nodes.collectFirst {
      case w: WordNode if w.token.value == ":each" => w
    }
    assert(eachNode.isDefined)
    assert(eachNode.get.word.isDefined)
  }

  test("syntaxTree: set and fcall propagates variables") {
    val tree = interpreter.syntaxTree("duplicate,(,:dup,),:set,4,duplicate,:fcall")
    assertEquals(tree.diagnostics, Nil)
    assertEquals(tree.stack, List("4", "4"))
  }
}

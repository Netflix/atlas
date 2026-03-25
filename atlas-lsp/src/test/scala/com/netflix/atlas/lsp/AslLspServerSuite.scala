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

import com.netflix.atlas.core.model.StyleVocabulary
import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.stacklang.StandardVocabulary
import org.eclipse.lsp4j.CodeAction
import org.eclipse.lsp4j.CodeActionContext
import org.eclipse.lsp4j.CodeActionParams
import org.eclipse.lsp4j.Command
import org.eclipse.lsp4j.DiagnosticTag
import org.eclipse.lsp4j.jsonrpc.messages.{Either => LspEither}
import org.eclipse.lsp4j.CompletionParams
import org.eclipse.lsp4j.DidOpenTextDocumentParams
import org.eclipse.lsp4j.InitializeParams
import org.eclipse.lsp4j.Position
import org.eclipse.lsp4j.PublishDiagnosticsParams
import org.eclipse.lsp4j.Range
import org.eclipse.lsp4j.SemanticTokensParams
import org.eclipse.lsp4j.TextDocumentIdentifier
import org.eclipse.lsp4j.TextDocumentItem
import org.eclipse.lsp4j.services.LanguageClient

import munit.FunSuite

class AslLspServerSuite extends FunSuite {

  private def newServer: AslLspServer = new AslLspServer(StandardVocabulary)

  private def openDocument(server: AslLspServer, uri: String, text: String): Unit = {
    val params = new DidOpenTextDocumentParams
    params.setTextDocument(new TextDocumentItem(uri, "atlas", 1, text))
    server.getTextDocumentService.didOpen(params)
  }

  private def requestCompletion(
    server: AslLspServer,
    uri: String,
    character: Int
  ): List[String] = {
    val params = new CompletionParams
    params.setTextDocument(new TextDocumentIdentifier(uri))
    params.setPosition(new Position(0, character))
    val result = server.getTextDocumentService.completion(params).get()
    result.getLeft.asScala.map(_.getLabel).toList
  }

  test("initialize enables completion") {
    val server = newServer
    val result = server.initialize(new InitializeParams).get()
    assertNotEquals(result.getCapabilities.getCompletionProvider, null)
  }

  test("shutdown completes successfully") {
    val server = newServer
    val result = server.shutdown().get()
    assertEquals(result, null)
  }

  test("completion: all words offered on empty expression") {
    val server = newServer
    val uri = "expr:1"
    openDocument(server, uri, "")
    val labels = requestCompletion(server, uri, 0)
    assert(labels.nonEmpty)
    assert(labels.contains(":depth"))
  }

  test("completion: words filtered by prefix") {
    val server = newServer
    val uri = "expr:2"
    openDocument(server, uri, "a,b,:sw")
    // cursor at end, on partial word ":sw"
    val labels = requestCompletion(server, uri, 7)
    assert(labels.contains(":swap"))
    assert(!labels.contains(":dup"))
  }

  test("completion: words filtered by stack") {
    val server = newServer
    val uri = "expr:3"
    // With two items on stack, :swap and :drop should match
    openDocument(server, uri, "a,b,")
    val labels = requestCompletion(server, uri, 4)
    assert(labels.contains(":swap"))
    assert(labels.contains(":drop"))
    assert(labels.contains(":dup"))
  }

  test("completion: items have detail and documentation") {
    val server = newServer
    val uri = "expr:4"
    openDocument(server, uri, "a,b,")
    val params = new CompletionParams
    params.setTextDocument(new TextDocumentIdentifier(uri))
    params.setPosition(new Position(0, 4))
    val items = server.getTextDocumentService.completion(params).get().getLeft.asScala
    val swap = items.find(_.getLabel == ":swap").get
    assertNotEquals(swap.getDetail, null)
    assertNotEquals(swap.getDocumentation, null)
  }

  //
  // semantic tokens
  //

  private def requestSemanticTokens(
    server: AslLspServer,
    uri: String
  ): List[Int] = {
    val params = new SemanticTokensParams
    params.setTextDocument(new TextDocumentIdentifier(uri))
    val result = server.getTextDocumentService.semanticTokensFull(params).get()
    result.getData.asScala.map(_.intValue()).toList
  }

  /** Decode the raw LSP integer array into (start, length, tokenType) tuples. */
  private def decodeTokens(data: List[Int]): List[(Int, Int, Int)] = {
    var pos = 0
    data
      .grouped(5)
      .map { group =>
        pos += group(1) // deltaStart (deltaLine is always 0)
        (pos, group(2), group(3)) // (absoluteStart, length, tokenType)
      }
      .toList
  }

  test("initialize enables semantic tokens") {
    val server = newServer
    val result = server.initialize(new InitializeParams).get()
    assertNotEquals(result.getCapabilities.getSemanticTokensProvider, null)
  }

  test("semantic tokens: literals classified as string") {
    val server = newServer
    val uri = "expr:st1"
    openDocument(server, uri, "abc")
    val tokens = decodeTokens(requestSemanticTokens(server, uri))
    assertEquals(tokens.size, 1)
    assertEquals(tokens.head, (0, 3, AslTokenTypes.String))
  }

  test("semantic tokens: numeric literal") {
    val server = newServer
    val uri = "expr:st2"
    openDocument(server, uri, "42")
    val tokens = decodeTokens(requestSemanticTokens(server, uri))
    assertEquals(tokens.size, 1)
    assertEquals(tokens.head, (0, 2, AslTokenTypes.Number))
  }

  test("semantic tokens: known word") {
    val server = newServer
    val uri = "expr:st3"
    openDocument(server, uri, "a,b,:swap")
    val tokens = decodeTokens(requestSemanticTokens(server, uri))
    assertEquals(tokens.size, 3)
    // "a" at 0, "b" at 2, ":swap" at 4
    assertEquals(tokens(0), (0, 1, AslTokenTypes.String))
    assertEquals(tokens(1), (2, 1, AslTokenTypes.String))
    assertEquals(tokens(2), (4, 5, AslTokenTypes.Word))
  }

  test("semantic tokens: unknown word") {
    val server = newServer
    val uri = "expr:st4"
    openDocument(server, uri, "a,:unknown")
    val tokens = decodeTokens(requestSemanticTokens(server, uri))
    assertEquals(tokens.size, 2)
    assertEquals(tokens(1)._3, AslTokenTypes.UnknownWord)
  }

  test("semantic tokens: list with parentheses") {
    val server = newServer
    val uri = "expr:st5"
    openDocument(server, uri, "(,a,b,)")
    val tokens = decodeTokens(requestSemanticTokens(server, uri))
    // ( at 0, a at 2, b at 4, ) at 6
    assertEquals(tokens.size, 4)
    assertEquals(tokens(0)._3, AslTokenTypes.Parenthesis)
    assertEquals(tokens(1)._3, AslTokenTypes.String)
    assertEquals(tokens(2)._3, AslTokenTypes.String)
    assertEquals(tokens(3)._3, AslTokenTypes.Parenthesis)
  }

  test("semantic tokens: mixed expression") {
    val server = newServer
    val uri = "expr:st6"
    openDocument(server, uri, "42,hello,:dup")
    val tokens = decodeTokens(requestSemanticTokens(server, uri))
    assertEquals(tokens.size, 3)
    assertEquals(tokens(0)._3, AslTokenTypes.Number)
    assertEquals(tokens(1)._3, AslTokenTypes.String)
    assertEquals(tokens(2)._3, AslTokenTypes.Word)
  }

  //
  // semantic tokens: comments
  //

  test("semantic tokens: standalone comment") {
    val server = newServer
    val uri = "expr:st7"
    openDocument(server, uri, "a,/* comment */,b")
    val tokens = decodeTokens(requestSemanticTokens(server, uri))
    val commentTokens = tokens.filter(_._3 == AslTokenTypes.Comment)
    assertEquals(commentTokens.size, 1)
    // "/* comment */" starts at 2, length 13
    assertEquals(commentTokens.head, (2, 13, AslTokenTypes.Comment))
  }

  test("semantic tokens: comment embedded in word") {
    val server = newServer
    val uri = "expr:st8"
    // a,:d/*c*/up -> word fragments :d at 2-4 and up at 9-11, comment /*c*/ at 4-9
    openDocument(server, uri, "a,:d/*c*/up")
    val tokens = decodeTokens(requestSemanticTokens(server, uri))
    // Expect: String "a" at 0, Word ":d" at 2, Comment "/*c*/" at 4, Word "up" at 9
    val wordTokens = tokens.filter(_._3 == AslTokenTypes.Word)
    val commentTokens = tokens.filter(_._3 == AslTokenTypes.Comment)
    assertEquals(wordTokens.size, 2) // two fragments of :dup
    assertEquals(commentTokens.size, 1)
    assertEquals(wordTokens(0), (2, 2, AslTokenTypes.Word)) // :d
    assertEquals(wordTokens(1), (9, 2, AslTokenTypes.Word)) // up
    assertEquals(commentTokens.head, (4, 5, AslTokenTypes.Comment)) // /*c*/
  }

  //
  // semantic tokens: multi-line
  //

  /** Decode raw LSP integer array into (line, col, length, tokenType) tuples. */
  private def decodeTokensMultiLine(data: List[Int]): List[(Int, Int, Int, Int)] = {
    var line = 0
    var col = 0
    data
      .grouped(5)
      .map { group =>
        line += group(0)
        col = if (group(0) > 0) group(1) else col + group(1)
        (line, col, group(2), group(3))
      }
      .toList
  }

  test("semantic tokens: multi-line expression") {
    val server = newServer
    val uri = "expr:st-ml1"
    // "a,\nb" — two lines with comma delimiter
    openDocument(server, uri, "a,\nb")
    val tokens = decodeTokensMultiLine(requestSemanticTokens(server, uri))
    assertEquals(tokens.size, 2)
    // "a" on line 0, col 0
    assertEquals(tokens(0), (0, 0, 1, AslTokenTypes.String))
    // "b" on line 1, col 0
    assertEquals(tokens(1), (1, 0, 1, AslTokenTypes.String))
  }

  test("semantic tokens: multi-line with word on second line") {
    val server = newServer
    val uri = "expr:st-ml2"
    // "a,b,\n:swap" — word on second line
    openDocument(server, uri, "a,b,\n:swap")
    val tokens = decodeTokensMultiLine(requestSemanticTokens(server, uri))
    val wordTokens = tokens.filter(_._4 == AslTokenTypes.Word)
    assertEquals(wordTokens.size, 1)
    // :swap on line 1, col 0
    assertEquals(wordTokens.head, (1, 0, 5, AslTokenTypes.Word))
  }

  //
  // code actions
  //

  private def requestCodeActions(
    server: AslLspServer,
    uri: String
  ): java.util.List[?] = {
    val text = server.analyzer().getText(uri)
    val params = new CodeActionParams
    params.setTextDocument(new TextDocumentIdentifier(uri))
    params.setRange(new Range(new Position(0, 0), new Position(0, text.length)))
    params.setContext(new CodeActionContext(java.util.List.of()))
    server.getTextDocumentService.codeAction(params).get()
  }

  test("initialize enables code actions") {
    val server = newServer
    val result = server.initialize(new InitializeParams).get()
    assertNotEquals(result.getCapabilities.getCodeActionProvider, null)
  }

  test("codeAction: no action for simple expression") {
    val server = newServer
    val uri = "expr:ca1"
    openDocument(server, uri, "a,b,:swap")
    val actions = requestCodeActions(server, uri)
    assertEquals(actions.size(), 0)
  }

  test("codeAction: no action on invalid expression") {
    val server = newServer
    val uri = "expr:ca2"
    openDocument(server, uri, ":unknown")
    val actions = requestCodeActions(server, uri)
    assertEquals(actions.size(), 0)
  }

  private def codeActionTitles(actions: java.util.List[?]): Seq[String] = {
    (0 until actions.size()).map { i =>
      val either = actions.get(i).asInstanceOf[LspEither[Command, CodeAction]]
      either.getRight.getTitle
    }
  }

  test("codeAction: normalize reorders query clauses") {
    val server = new AslLspServer(StyleVocabulary)
    val uri = "expr:ca-norm"
    openDocument(server, uri, "nf.cluster,foo,:eq,name,bar,:eq,:and,:sum")
    val actions = requestCodeActions(server, uri)
    val titles = codeActionTitles(actions)
    assert(titles.contains("Normalize expression"))
  }

  test("codeAction: normalize not offered when already normalized") {
    val server = new AslLspServer(StyleVocabulary)
    val uri = "expr:ca-norm2"
    openDocument(server, uri, "name,sps,:eq,:sum")
    val actions = requestCodeActions(server, uri)
    val titles = codeActionTitles(actions)
    assert(!titles.contains("Normalize expression"))
  }

  //
  // hover
  //

  private def requestHover(text: String, offset: Int): Option[org.eclipse.lsp4j.Hover] = {
    val server = newServer
    server.analyzer().computeHover(text, offset)
  }

  test("initialize enables hover") {
    val server = newServer
    val result = server.initialize(new InitializeParams).get()
    assertNotEquals(result.getCapabilities.getHoverProvider, null)
  }

  test("hover: word shows signature and summary") {
    val hover = requestHover("a,b,:swap", 5)
    assert(hover.isDefined)
    val content = hover.get.getContents.getRight.getValue
    assert(content.contains(":swap"), s"Expected word name in: $content")
    assert(content.contains("Swap"), s"Expected summary in: $content")
  }

  test("hover: literal returns None") {
    val hover = requestHover("a,b,:swap", 0)
    assert(hover.isEmpty)
  }

  test("hover: unknown word returns None") {
    val hover = requestHover(":unknown", 0)
    assert(hover.isEmpty)
  }

  private def requestModelHover(text: String, offset: Int): Option[org.eclipse.lsp4j.Hover] = {
    val server = new AslLspServer(StyleVocabulary)
    server.analyzer().computeHover(text, offset)
  }

  test("hover: :eq shows consumed args and result") {
    // name,sps,:eq
    // 0123456789012
    val hover = requestModelHover("name,sps,:eq", 9)
    assert(hover.isDefined)
    val content = hover.get.getContents.getRight.getValue
    assert(content.contains("Stack:"), s"Expected Stack section in: $content")
    assert(content.contains("\u2192"), s"Expected arrow in: $content")
    // Should show the consumed "name" and "sps" strings
    assert(content.contains("\"name\""), s"Expected consumed key in: $content")
    assert(content.contains("\"sps\""), s"Expected consumed value in: $content")
  }

  test("hover: :sum shows consumed Query and result") {
    // name,sps,:eq,:sum
    // 01234567890123456
    val hover = requestModelHover("name,sps,:eq,:sum", 13)
    assert(hover.isDefined)
    val content = hover.get.getContents.getRight.getValue
    assert(content.contains("Stack:"), s"Expected Stack section in: $content")
    assert(content.contains("\u2192"), s"Expected arrow in: $content")
  }

  test("hover: :swap shows stack items") {
    val hover = requestHover("a,b,:swap", 4)
    assert(hover.isDefined)
    val content = hover.get.getContents.getRight.getValue
    assert(content.contains("Stack:"), s"Expected Stack section in: $content")
    assert(content.contains("\"a\""), s"Expected stack item a in: $content")
    assert(content.contains("\"b\""), s"Expected stack item b in: $content")
  }

  test("hover: word with empty pre-stack shows produced items") {
    // :depth takes no parameters but produces an Int
    val hover = requestHover(":depth", 0)
    assert(hover.isDefined)
    val content = hover.get.getContents.getRight.getValue
    assert(content.contains("Stack:"), s"Expected Stack section in: $content")
    assert(content.contains("(empty) \u2192"), s"Expected empty input in: $content")
  }

  //
  // go to definition
  //

  private def requestDefinition(text: String, offset: Int): Option[org.eclipse.lsp4j.Location] = {
    val server = newServer
    server.analyzer().computeDefinition("test:uri", text, offset)
  }

  test("initialize enables definition") {
    val server = newServer
    val result = server.initialize(new InitializeParams).get()
    assertNotEquals(result.getCapabilities.getDefinitionProvider, null)
  }

  test("definition: :get jumps to :set") {
    // v,hello,:set,v,:get
    // 0123456789012345678
    val text = "v,hello,:set,v,:get"
    val defn = requestDefinition(text, 15) // cursor on :get
    assert(defn.isDefined)
    // :set span
    assertEquals(defn.get.getRange.getStart.getCharacter, 8)
    assertEquals(defn.get.getRange.getEnd.getCharacter, 12)
  }

  test("definition: variable name before :get jumps to :set") {
    val text = "v,hello,:set,v,:get"
    val defn = requestDefinition(text, 13) // cursor on "v" before :get
    assert(defn.isDefined)
    assertEquals(defn.get.getRange.getStart.getCharacter, 8)
  }

  test("definition: jumps to most recent :set") {
    // a,1,:set,a,2,:set,a,:get
    // 0123456789012345678901234
    val text = "a,1,:set,a,2,:set,a,:get"
    val defn = requestDefinition(text, 20) // cursor on :get
    assert(defn.isDefined)
    // Should jump to second :set at position 13, not first at position 4
    assertEquals(defn.get.getRange.getStart.getCharacter, 13)
  }

  test("definition: :sset support") {
    // 3,a,:sset,a,:get
    // 0123456789012345
    val text = "3,a,:sset,a,:get"
    val defn = requestDefinition(text, 12) // cursor on :get
    assert(defn.isDefined)
    assertEquals(defn.get.getRange.getStart.getCharacter, 4)
  }

  test("definition: no definition for literal without :get") {
    val text = "a,b,:swap"
    val defn = requestDefinition(text, 0) // cursor on "a"
    assert(defn.isEmpty)
  }

  //
  // compress expression
  //

  private def compress(text: String): String = {
    val server = newServer
    val tree = server.interpreter().syntaxTree(text)
    server.analyzer().compressExpression(text, tree.nodes)
  }

  test("compress: strips whitespace around tokens") {
    assertEquals(compress("a  ,  b  ,  :swap"), "a,b,:swap")
  }

  test("compress: removes empty tokens") {
    assertEquals(compress("a,b,,,:dup,"), "a,b,:dup")
  }

  test("compress: removes line breaks") {
    assertEquals(compress("a,\nb,\n:swap"), "a,b,:swap")
  }

  test("compress: preserves comments") {
    assertEquals(compress("a , /* hi */ , b"), "a,/* hi */,b")
  }

  test("compress: preserves lists") {
    assertEquals(compress("( , a , b , )"), "(,a,b,)")
  }

  test("compress: already compressed unchanged") {
    assertEquals(compress("a,b,:swap"), "a,b,:swap")
  }

  //
  // format expression (unit tests via analyzer)
  //

  private def format(text: String): String = {
    val server = newServer
    val tree = server.interpreter().syntaxTree(text)
    server.analyzer().formatExpression(text, tree.nodes)
  }

  /** Format using the full model vocabulary (query, data, math, style words). */
  private def formatModel(text: String): String = {
    val server = new AslLspServer(StyleVocabulary)
    val tree = server.interpreter().syntaxTree(text)
    server.analyzer().formatExpression(text, tree.nodes)
  }

  test("format: simple expression unchanged") {
    assertEquals(format("a,b,:swap"), "a,b,:swap")
  }

  test("format: single literal unchanged") {
    assertEquals(format("hello"), "hello")
  }

  test("format: nested operations get line breaks") {
    // :dup pops 1 pushes 2, :drop pops 1 pushes 0
    // tree: CommandNode([CommandNode([Simple("a")], ":dup", 2)], ":drop", 0)
    val input = "a,:dup,:drop"
    val expected = "a,:dup,\n:drop"
    assertEquals(format(input), expected)
  }

  test("format: multiple top-level items separated by blank line") {
    assertEquals(format("a,b"), "a,\n\nb")
  }

  test("format: model query with aggregation") {
    val input = "name,sps,:eq,:sum"
    val expected = "name,sps,:eq,\n:sum"
    assertEquals(formatModel(input), expected)
  }

  test("format: model group by with complex arg") {
    val input = "name,sps,:eq,:sum,(,name,),:by"
    val expected = "name,sps,:eq,\n:sum,\n(,name,),:by"
    assertEquals(formatModel(input), expected)
  }

  test("format: model multiple stack items") {
    val input = "name,sps,:eq,:sum,name,app,:eq,:sum"
    val expected = "name,sps,:eq,\n:sum,\n\nname,app,:eq,\n:sum"
    assertEquals(formatModel(input), expected)
  }

  test("format: model set indents value") {
    val input = "v,name,sps,:eq,:sum,:set"
    val result = formatModel(input)
    assert(result.contains("\n  "), s"Expected indented content in: $result")
  }

  test("format: model nested operations") {
    val input = "name,sps,:eq,:sum,(,name,),:by,4,:rolling-count,1,:gt,$name,:legend"
    val result = formatModel(input)
    assert(result.contains("\n"), s"Expected line breaks in: $result")
  }

  test("format: :list and :each grouped together") {
    val input = "name,a,:eq,:sum,:list,(,key,(,b,),:in,:cq,),:each"
    val result = formatModel(input)
    // :list and :each body should not have blank lines between them
    assert(
      !result.contains(":list,\n\n("),
      s"Unexpected blank line between :list and :each body in: $result"
    )
  }

  test("format: short list stays inline") {
    val input = "name,a,:eq,:sum,(,name,),:by"
    val result = formatModel(input)
    assert(result.contains("(,name,)"), s"Short list should be inline in: $result")
  }

  test("format: long list wraps to multiple lines") {
    val input =
      "name,a,:eq,:sum,(,us-west-2,us-east-1,us-east-2,eu-west-1,ap-southeast-1,ap-northeast-1,ap-southeast-2,),:by"
    val result = formatModel(input)
    assert(result.contains("\n  us-west-2"), s"Long list should be multi-line in: $result")
  }

  test("format: :list args separated by blank lines") {
    val input = "name,a,:eq,:sum,name,b,:eq,:sum,:list,(,key,(,c,),:in,:cq,),:each"
    val result = formatModel(input)
    // The two :sum expressions inside :list should be separated by blank lines
    assert(result.contains(":sum,\n\n"), s"Expected blank line between :list args in: $result")
    // :list and :each body should not have blank lines between them
    assert(
      !result.contains(":list,\n\n("),
      s"Unexpected blank line between :list and :each body in: $result"
    )
  }

  //
  // diagnostics: words inside lists
  //

  /** Get diagnostics from the syntax tree for the given expression using the model vocabulary. */
  private def modelDiagnostics(text: String): List[String] = {
    val server = new AslLspServer(StyleVocabulary)
    val tree = server.interpreter().syntaxTree(text)
    tree.diagnostics.map(d => s"[${d.span.start}-${d.span.end}] ${d.message}")
  }

  test("diagnostics: :each with :in in list body no false errors") {
    // When :each executes during syntax tree building, the list body may fail
    // because it runs against aggregate types rather than individual items.
    // The syntax tree should recognize the word matches and not emit a false
    // diagnostic.
    val input =
      "v,name,a,:eq,:sum,:set," +
        "v,:get,:list," +
        "(,key,(,b,),:in,:cq,),:each"
    val errors = modelDiagnostics(input).filter(_.contains("no matches"))
    assertEquals(errors, Nil, s"Unexpected errors: ${errors.mkString("; ")}")
  }

  //
  // parameter mismatch diagnostics
  //

  /** Get parameter mismatch diagnostics for the given expression using the model vocabulary. */
  private def paramDiagnostics(text: String): List[String] = {
    val server = new AslLspServer(StyleVocabulary)
    val tree = server.interpreter().syntaxTree(text)
    server.analyzer().computeParameterDiagnostics(tree).map { d =>
      s"[${d.span.start}-${d.span.end}] ${d.message}"
    }
  }

  test("diagnostics: :color with bad color value highlights the value") {
    // name,sps,:eq,:sum,12x,:color
    // 0123456789012345678901234567
    val diags = paramDiagnostics("name,sps,:eq,:sum,12x,:color")
    assert(diags.nonEmpty, "expected parameter diagnostics")
    assert(
      diags.exists(d => d.contains("expected Color") && d.contains("12x")),
      s"Expected color mismatch on '12x': $diags"
    )
    // Should point at "12x" which is at offset 18-21
    assert(
      diags.exists(_.startsWith("[18-21]")),
      s"Expected diagnostic on '12x' span: $diags"
    )
  }

  test("diagnostics: :color accepts named colors") {
    val diags = paramDiagnostics("name,sps,:eq,:sum,blue1,:color")
    assertEquals(diags, Nil)
  }

  test("diagnostics: valid expression has no parameter diagnostics") {
    val diags = paramDiagnostics("name,sps,:eq,:sum,f00,:color")
    assertEquals(diags, Nil)
  }

  test("diagnostics: multiple bad params each get their own diagnostic") {
    // :color expects PresentationType and ColorStringType
    // With "hello,12x,:color", neither matches correctly
    val diags = paramDiagnostics("hello,12x,:color")
    assert(diags.size >= 2, s"Expected at least 2 diagnostics: $diags")
  }

  //
  // positionToOffset
  //

  test("positionToOffset: clamps to text length for out-of-bounds position") {
    val text = "short"
    val offset = AslTextDocumentService.positionToOffset(text, 0, 100)
    assertEquals(offset, text.length)
  }

  test("positionToOffset: past last line clamps to end") {
    val text = "a\nb"
    val offset = AslTextDocumentService.positionToOffset(text, 5, 0)
    assertEquals(offset, text.length)
  }

  test("positionToOffset: normal case") {
    val text = "abc\ndef"
    assertEquals(AslTextDocumentService.positionToOffset(text, 0, 2), 2)
    assertEquals(AslTextDocumentService.positionToOffset(text, 1, 1), 5)
  }

  //
  // document symbols
  //

  private def requestDocumentSymbols(
    server: AslLspServer,
    text: String
  ): List[org.eclipse.lsp4j.DocumentSymbol] = {
    server
      .analyzer()
      .computeDocumentSymbols(text)
      .asInstanceOf[List[org.eclipse.lsp4j.DocumentSymbol]]
  }

  test("initialize enables document symbols") {
    val server = newServer
    val result = server.initialize(new InitializeParams).get()
    assertNotEquals(result.getCapabilities.getDocumentSymbolProvider, null)
  }

  test("documentSymbol: swap has two string children") {
    val server = newServer
    val symbols = requestDocumentSymbols(server, "a,b,:swap")
    assertEquals(symbols.size, 1)
    val swap = symbols.head
    assertEquals(swap.getName, ":swap")
    assertEquals(swap.getKind, org.eclipse.lsp4j.SymbolKind.Function)
    val children = swap.getChildren.asScala.toList
    assertEquals(children.size, 2)
    assertEquals(children(0).getName, "a")
    assertEquals(children(0).getKind, org.eclipse.lsp4j.SymbolKind.String)
    assertEquals(children(1).getName, "b")
    assertEquals(children(1).getKind, org.eclipse.lsp4j.SymbolKind.String)
  }

  test("documentSymbol: eq+sum nested with detail") {
    val server = new AslLspServer(StyleVocabulary)
    val symbols = requestDocumentSymbols(server, "name,sps,:eq,:sum")
    assertEquals(symbols.size, 1)
    val sum = symbols.head
    assertEquals(sum.getName, ":sum")
    assertEquals(sum.getKind, org.eclipse.lsp4j.SymbolKind.Function)
    assert(sum.getDetail.contains("--"), s"Expected signature in detail: ${sum.getDetail}")
    val sumChildren = sum.getChildren.asScala.toList
    assertEquals(sumChildren.size, 1)
    val eq = sumChildren.head
    assertEquals(eq.getName, ":eq")
    val eqChildren = eq.getChildren.asScala.toList
    assertEquals(eqChildren.size, 2)
    assertEquals(eqChildren(0).getName, "name")
    assertEquals(eqChildren(1).getName, "sps")
  }

  test("documentSymbol: list as array with children") {
    val server = newServer
    val symbols = requestDocumentSymbols(server, "(,a,b,)")
    assertEquals(symbols.size, 1)
    val list = symbols.head
    assertEquals(list.getName, "(...)")
    assertEquals(list.getKind, org.eclipse.lsp4j.SymbolKind.Array)
    val children = list.getChildren.asScala.toList
    assertEquals(children.size, 2)
    assertEquals(children(0).getName, "a")
    assertEquals(children(1).getName, "b")
  }

  test("documentSymbol: unknown word has unresolved detail") {
    val server = newServer
    val symbols = requestDocumentSymbols(server, ":unknown")
    assertEquals(symbols.size, 1)
    val sym = symbols.head
    assertEquals(sym.getName, ":unknown")
    assertEquals(sym.getKind, org.eclipse.lsp4j.SymbolKind.Function)
    assertEquals(sym.getDetail, "unresolved")
  }

  //
  // unicode completions
  //

  //
  // code actions: semicolon typo quick-fix
  //

  test("codeAction: semicolon typo offers quick-fix") {
    val server = newServer
    val uri = "expr:ca-typo1"
    openDocument(server, uri, "a,b,;swap")
    val actions = requestCodeActions(server, uri)
    val titles = codeActionTitles(actions)
    assert(titles.contains("Replace with ':swap'"), s"Expected typo fix in: $titles")
  }

  test("codeAction: semicolon typo for unknown word has no action") {
    val server = newServer
    val uri = "expr:ca-typo2"
    openDocument(server, uri, ";notaword")
    val actions = requestCodeActions(server, uri)
    assertEquals(actions.size(), 0)
  }

  test("codeAction: deprecated offset with single value offers quick-fix") {
    val server = new AslLspServer(StyleVocabulary)
    val uri = "expr:ca-offset1"
    openDocument(server, uri, "name,sps,:eq,:sum,(,1w,),:offset")
    val actions = requestCodeActions(server, uri)
    val titles = codeActionTitles(actions)
    assert(titles.exists(_.contains("data variant")), s"Expected offset fix in: $titles")
  }

  test("codeAction: deprecated offset with multiple values has no quick-fix") {
    val server = new AslLspServer(StyleVocabulary)
    val uri = "expr:ca-offset2"
    openDocument(server, uri, "name,sps,:eq,:sum,(,0h,1d,1w,),:offset")
    val actions = requestCodeActions(server, uri)
    val titles = codeActionTitles(actions)
    assert(!titles.exists(_.contains("data variant")), s"Unexpected offset fix in: $titles")
  }

  test("completion: \\ prefix shows curated list") {
    val server = newServer
    val uri = "expr:uc0"
    openDocument(server, uri, "\\")
    val labels = requestCompletion(server, uri, 1)
    assert(labels.exists(_.contains("002C")), "should include comma")
    assert(labels.exists(_.contains("005C")), "should include backslash")
  }

  test("completion: \\u prefix shows curated list") {
    val server = newServer
    val uri = "expr:uc1"
    openDocument(server, uri, "\\u")
    val labels = requestCompletion(server, uri, 2)
    assert(labels.exists(_.contains("002C")), "should include comma")
    assert(labels.exists(_.contains("003A")), "should include colon")
    assert(labels.exists(_.contains("0028")), "should include left paren")
  }

  test("completion: \\u with hex prefix filters") {
    val server = newServer
    val uri = "expr:uc2"
    openDocument(server, uri, "\\u002")
    val labels = requestCompletion(server, uri, 5)
    assert(labels.exists(_.contains("002C")), "should include comma")
    assert(!labels.exists(_.contains("003A")), "003A doesn't start with 002")
  }

  test("completion: \\u with exact 4-digit hex") {
    val server = newServer
    val uri = "expr:uc3"
    openDocument(server, uri, "\\u0041")
    val labels = requestCompletion(server, uri, 6)
    // 0041 is 'A' — LATIN CAPITAL LETTER A
    assert(labels.exists(_.contains("0041")))
  }

  test("completion: \\u with name search") {
    val server = newServer
    val uri = "expr:uc4"
    openDocument(server, uri, "\\uarrow")
    val labels = requestCompletion(server, uri, 7)
    assert(labels.nonEmpty, "should find arrow characters")
    assert(labels.forall(_.toLowerCase.contains("arrow")))
  }

  test("completion: \\u in middle of expression") {
    val server = newServer
    val uri = "expr:uc5"
    openDocument(server, uri, "a,\\u")
    val labels = requestCompletion(server, uri, 4)
    assert(labels.exists(_.contains("002C")), "should show curated list after comma")
  }

  //
  // deprecation diagnostics
  //

  //
  // glossary wiring
  //

  test("server with glossary wires through to analyzer") {
    val glossary = Glossary.load("sample-glossary.json")
    val server = new AslLspServer(StandardVocabulary, glossary)
    assertEquals(server.analyzer().glossary.id, "nflx.sample")
    assert(server.analyzer().glossary.metrics.nonEmpty)
  }

  //
  // glossary hover
  //

  private def glossaryServer: AslLspServer = {
    val glossary = Glossary.load("sample-glossary.json")
    new AslLspServer(StandardVocabulary, glossary)
  }

  private def requestGlossaryHover(
    text: String,
    offset: Int
  ): Option[org.eclipse.lsp4j.Hover] = {
    val server = glossaryServer
    server.analyzer().computeHover(text, offset)
  }

  test("hover: metric name shows glossary description") {
    // name,sys.cpu.utilization
    // 0123456789012345678901234
    val hover = requestGlossaryHover("name,sys.cpu.utilization", 5)
    assert(hover.isDefined)
    val content = hover.get.getContents.getRight.getValue
    assert(content.contains("sys.cpu.utilization"), s"Expected metric name in: $content")
    assert(content.contains("CPU utilization"), s"Expected description in: $content")
    assert(content.contains("percent"), s"Expected unit in: $content")
    assert(content.contains("gauge"), s"Expected type in: $content")
  }

  test("hover: tag key shows glossary description") {
    val hover = requestGlossaryHover("nf.app", 0)
    assert(hover.isDefined)
    val content = hover.get.getContents.getRight.getValue
    assert(content.contains("nf.app"), s"Expected tag key in: $content")
    assert(content.contains("Application name"), s"Expected description in: $content")
  }

  test("hover: tag value shows glossary description") {
    // statistic,count — "count" is at value position
    // 0123456789012345
    val hover = requestGlossaryHover("statistic,count", 10)
    assert(hover.isDefined)
    val content = hover.get.getContents.getRight.getValue
    assert(content.contains("count"), s"Expected tag value in: $content")
    assert(content.contains("Rate of events"), s"Expected description in: $content")
  }

  test("hover: tag key with values shows values") {
    val hover = requestGlossaryHover("nf.region", 0)
    assert(hover.isDefined)
    val content = hover.get.getContents.getRight.getValue
    assert(content.contains("us-east-1"), s"Expected values in: $content")
  }

  test("hover: unknown literal returns None") {
    val hover = requestGlossaryHover("unknownmetric", 0)
    assert(hover.isEmpty)
  }

  //
  // glossary completions
  //

  private def requestGlossaryCompletion(
    text: String,
    character: Int
  ): List[String] = {
    val server = glossaryServer
    val uri = "expr:gc"
    openDocument(server, uri, text)
    requestCompletion(server, uri, character)
  }

  test("completion: tag keys offered at key position") {
    // Empty expression, typing nothing — should include glossary tag keys
    val labels = requestGlossaryCompletion("", 0)
    assert(labels.contains("nf.app"), s"Expected nf.app in: $labels")
    assert(labels.contains("nf.region"), s"Expected nf.region in: $labels")
  }

  test("completion: name offered as built-in key with prefix filter") {
    val labels = requestGlossaryCompletion("nam", 3)
    assert(labels.contains("name"), s"Expected name in: $labels")
  }

  test("completion: metric names offered after name,") {
    // After "name," the next literal is a metric name
    val labels = requestGlossaryCompletion("name,", 5)
    assert(labels.exists(_.contains("sys.cpu.utilization")), s"Expected metric in: $labels")
    assert(labels.exists(_.contains("jvm.gc.pause")), s"Expected jvm metric in: $labels")
  }

  test("completion: tag values offered at value position") {
    // "nf.region," — value position for nf.region, should offer enum values
    val labels = requestGlossaryCompletion("nf.region,", 10)
    assert(labels.contains("us-east-1"), s"Expected us-east-1 in: $labels")
    assert(labels.contains("us-west-2"), s"Expected us-west-2 in: $labels")
  }

  test("completion: glossary completions filtered by prefix") {
    val labels = requestGlossaryCompletion("name,sys.", 9)
    assert(labels.exists(_.startsWith("sys.")), s"Expected sys.* metrics in: $labels")
    assert(!labels.exists(_.startsWith("jvm.")), s"Unexpected jvm.* metrics in: $labels")
  }

  test("completion: glossary completions use case-insensitive substring") {
    val labels = requestGlossaryCompletion("name,cpu", 8)
    assert(
      labels.contains("sys.cpu.utilization"),
      s"Expected sys.cpu.utilization in: $labels"
    )
    assert(
      labels.contains("aws.ec2.cpuUtilization"),
      s"Expected aws.ec2.cpuUtilization in: $labels"
    )
  }

  test("completion: glossary completion replaces typed prefix") {
    val server = glossaryServer
    val uri = "expr:gc-replace"
    val text = "name,sys"
    openDocument(server, uri, text)
    val params = new CompletionParams
    params.setTextDocument(new TextDocumentIdentifier(uri))
    params.setPosition(new Position(0, 8))
    val items = server.getTextDocumentService.completion(params).get().getLeft.asScala
    val cpu = items.find(_.getLabel == "sys.cpu.utilization")
    assert(cpu.isDefined, s"Expected sys.cpu.utilization in: ${items.map(_.getLabel)}")
    val edit = cpu.get.getTextEdit.getLeft
    // Should replace from position 5 (after "name,") to 8 (end of "sys")
    assertEquals(edit.getRange.getStart.getCharacter, 5)
    assertEquals(edit.getRange.getEnd.getCharacter, 8)
    assertEquals(edit.getNewText, "sys.cpu.utilization,")
  }

  //
  // deprecation diagnostics
  //

  test("diagnostics: deprecated word gets DiagnosticTag.Deprecated") {
    val captured = new java.util.concurrent.atomic.AtomicReference[PublishDiagnosticsParams]()
    val client = new LanguageClient {
      override def telemetryEvent(obj: Any): Unit = ()
      override def publishDiagnostics(params: PublishDiagnosticsParams): Unit = {
        captured.set(params)
      }
      override def showMessage(params: org.eclipse.lsp4j.MessageParams): Unit = ()
      override def showMessageRequest(
        params: org.eclipse.lsp4j.ShowMessageRequestParams
      ): java.util.concurrent.CompletableFuture[org.eclipse.lsp4j.MessageActionItem] = null
      override def logMessage(params: org.eclipse.lsp4j.MessageParams): Unit = ()
    }
    val interpreter = Interpreter(StyleVocabulary.allWords)
    val analyzer = new AslDocumentAnalyzer(interpreter, clientSupplier = () => client)
    analyzer.updateDocument("test:depr", "name,sps,:eq,:sum,(,0h,1d,1w,),:offset")
    val params = captured.get()
    assert(params != null, "expected diagnostics to be published")
    val diags = params.getDiagnostics.asScala.toList
    val deprecated = diags.filter { d =>
      d.getTags != null && d.getTags.asScala.contains(DiagnosticTag.Deprecated)
    }
    assert(deprecated.nonEmpty, s"expected DiagnosticTag.Deprecated in: $diags")
    val msg = deprecated.head.getMessage.getLeft
    assert(msg.contains("deprecated"), s"expected 'deprecated' in message: $msg")
  }
}

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

import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.stacklang.StandardVocabulary
import com.netflix.atlas.core.uri.UriParser
import org.eclipse.lsp4j.PublishDiagnosticsParams
import org.eclipse.lsp4j.services.LanguageClient

import munit.FunSuite

class UriDocumentAnalyzerSuite extends FunSuite {

  private def diagMessage(d: org.eclipse.lsp4j.Diagnostic): String = {
    val msg = d.getMessage
    if (msg.isLeft) msg.getLeft else msg.getRight.getValue
  }

  private def newAnalyzer(): UriDocumentAnalyzer = {
    val interpreter = Interpreter(StandardVocabulary.allWords)
    val asl = new AslDocumentAnalyzer(interpreter)
    new UriDocumentAnalyzer(asl)
  }

  private def newAnalyzerWithClient(): (
    UriDocumentAnalyzer,
    java.util.concurrent.atomic.AtomicReference[PublishDiagnosticsParams]
  ) = {
    val captured =
      new java.util.concurrent.atomic.AtomicReference[PublishDiagnosticsParams]()
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
    val interpreter = Interpreter(StandardVocabulary.allWords)
    val asl = new AslDocumentAnalyzer(interpreter)
    val analyzer = new UriDocumentAnalyzer(asl, () => client)
    (analyzer, captured)
  }

  //
  // URI parsing (via UriParser)
  //

  test("parseUri: path only") {
    val parsed = UriParser.parse("/api/v1/graph")
    assertEquals(parsed.path.start, 0)
    assertEquals(parsed.path.end, 13)
    assertEquals(parsed.query, Nil)
  }

  test("parseUri: path with query params") {
    val text = "/api/v1/graph?q=name,sps,:eq,:sum&s=e-3h"
    val parsed = UriParser.parse(text)
    assertEquals(parsed.path.end, 13)
    assertEquals(parsed.query.size, 2)

    val q = parsed.query(0)
    assertEquals(q.key.text, "q")
    assertEquals(q.key.start, 14)
    assertEquals(q.key.end, 15)
    assertEquals(q.value.start, 16)
    assertEquals(q.value.end, 33)

    val s = parsed.query(1)
    assertEquals(s.key.text, "s")
    assertEquals(s.key.start, 34)
    assertEquals(s.key.end, 35)
    assertEquals(s.value.start, 36)
    assertEquals(s.value.end, 40)
  }

  test("parseUri: cq recognized as expression param") {
    val text = "/api/v1/graph?q=:true&cq=nf.app,foo,:eq"
    val parsed = UriParser.parse(text)
    val cq = parsed.query.find(_.key.text == "cq").get
    assertEquals(cq.key.text, "cq")
  }

  test("parseUri: percent-encoded & not treated as separator") {
    val text = "/api/v1/graph?q=a%26b&s=e-3h"
    val parsed = UriParser.parse(text)
    assertEquals(parsed.query.size, 2)
    assertEquals(parsed.query(0).decodedValue, "a&b")
    assertEquals(parsed.query(1).key.text, "s")
  }

  //
  // Hover
  //

  test("hover: path shows path description") {
    val analyzer = newAnalyzer()
    val text = "/api/v1/graph?q=name,sps,:eq,:sum"
    val hover = analyzer.computeHover(text, 5)
    assert(hover.isDefined)
    val content = hover.get.getContents.getRight.getValue
    assert(content.contains("/api/v1/graph"), s"Expected path in: $content")
  }

  test("hover: param name shows description") {
    val analyzer = newAnalyzer()
    val text = "/api/v1/graph?q=name,sps,:eq,:sum&s=e-3h"
    // 's' param name is at position 34
    val hover = analyzer.computeHover(text, 34)
    assert(hover.isDefined)
    val content = hover.get.getContents.getRight.getValue
    assert(content.contains("Start time"), s"Expected 's' description in: $content")
  }

  test("hover: expression value delegates to ASL") {
    val analyzer = newAnalyzer()
    // /api/v1/graph?q=a,b,:swap
    // 0123456789012345678901234
    val text = "/api/v1/graph?q=a,b,:swap"
    // :swap starts at offset 20 in the URI, which is offset 4 in the expression
    val hover = analyzer.computeHover(text, 20)
    assert(hover.isDefined)
    val content = hover.get.getContents.getRight.getValue
    assert(content.contains(":swap"), s"Expected :swap hover in: $content")
  }

  test("hover: non-expression value shows param description") {
    val analyzer = newAnalyzer()
    val text = "/api/v1/graph?q=:true&s=e-3h"
    // value of s starts at 24
    val hover = analyzer.computeHover(text, 24)
    assert(hover.isDefined)
    val content = hover.get.getContents.getRight.getValue
    assert(content.contains("Start time"), s"Expected param description in: $content")
  }

  //
  // Completions
  //

  test("completions: after & offers param names") {
    val analyzer = newAnalyzer()
    val text = "/api/v1/graph?q=:true&"
    val items = analyzer.computeCompletions(text, text.length)
    val labels = items.map(_.getLabel)
    assert(labels.contains("s"), s"Expected 's' in: $labels")
    assert(labels.contains("e"), s"Expected 'e' in: $labels")
    assert(labels.contains("w"), s"Expected 'w' in: $labels")
  }

  test("completions: in expression value delegates to ASL") {
    val analyzer = newAnalyzer()
    // /api/v1/graph?q=a,b,:
    // 0123456789012345678901
    val text = "/api/v1/graph?q=a,b,:"
    val items = analyzer.computeCompletions(text, text.length)
    val labels = items.map(_.getLabel)
    assert(labels.contains(":swap"), s"Expected :swap in: $labels")
    assert(labels.contains(":dup"), s"Expected :dup in: $labels")
  }

  test("completions: ASL completions have shifted text edit ranges") {
    val analyzer = newAnalyzer()
    // /api/v1/graph?q=a,b,:sw
    val text = "/api/v1/graph?q=a,b,:sw"
    val items = analyzer.computeCompletions(text, text.length)
    val swap = items.find(_.getLabel == ":swap")
    assert(swap.isDefined, s"Expected :swap in: ${items.map(_.getLabel)}")
    val edit = swap.get.getTextEdit.getLeft
    // The edit range should be in URI coordinates (offset 20 for :sw)
    assert(edit.getRange.getStart.getCharacter >= 16, s"Expected shifted range: ${edit.getRange}")
  }

  //
  // Semantic tokens
  //

  /** Decode raw LSP integer array into (absolutePos, length, tokenType) tuples. */
  private def decodeTokens(data: List[Integer]): List[(Int, Int, Int)] = {
    var col = 0
    data
      .map(_.intValue())
      .grouped(5)
      .map { group =>
        col = if (group(0) > 0) group(1) else col + group(1)
        (col, group(2), group(3))
      }
      .toList
  }

  test("semantic tokens: path token") {
    val analyzer = newAnalyzer()
    val text = "/api/v1/graph?q=:true"
    val tokens = decodeTokens(analyzer.computeSemanticTokens(text))
    val pathTokens = tokens.filter(_._3 == AslTokenTypes.Path)
    assertEquals(pathTokens.size, 1)
    assertEquals(pathTokens.head._1, 0) // starts at 0
    assertEquals(pathTokens.head._2, 13) // "/api/v1/graph" length
  }

  test("semantic tokens: param name and operators") {
    val analyzer = newAnalyzer()
    val text = "/api/v1/graph?q=:true"
    val tokens = decodeTokens(analyzer.computeSemanticTokens(text))
    val paramTokens = tokens.filter(_._3 == AslTokenTypes.Parameter)
    assert(paramTokens.nonEmpty, s"Expected parameter tokens in: $tokens")
    val opTokens = tokens.filter(_._3 == AslTokenTypes.UriOperator)
    assert(opTokens.nonEmpty, s"Expected URI operator tokens in: $tokens")
  }

  test("semantic tokens: expression tokens are shifted") {
    val analyzer = newAnalyzer()
    // /api/v1/graph?q=42
    // 0123456789012345678
    val text = "/api/v1/graph?q=42"
    val tokens = decodeTokens(analyzer.computeSemanticTokens(text))
    val numTokens = tokens.filter(_._3 == AslTokenTypes.Number)
    assertEquals(numTokens.size, 1)
    // "42" starts at position 16 in the URI
    assertEquals(numTokens.head._1, 16)
    assertEquals(numTokens.head._2, 2)
  }

  test("semantic tokens: non-expression value is string") {
    val analyzer = newAnalyzer()
    val text = "/api/v1/graph?q=:true&s=e-3h"
    val tokens = decodeTokens(analyzer.computeSemanticTokens(text))
    val stringTokens = tokens.filter(_._3 == AslTokenTypes.String)
    // "e-3h" should be a string token
    assert(stringTokens.exists(t => t._1 == 24 && t._2 == 4), s"Expected string at 24: $tokens")
  }

  //
  // Diagnostics
  //

  test("diagnostics: missing q parameter") {
    val (analyzer, captured) = newAnalyzerWithClient()
    analyzer.updateDocument("test:uri1", "/api/v1/graph?s=e-3h")
    val params = captured.get()
    assert(params != null, "expected diagnostics")
    val diags = params.getDiagnostics.asScala.toList
    assert(
      diags.exists(d => diagMessage(d).contains("Missing required 'q'")),
      s"Expected missing q: $diags"
    )
  }

  test("diagnostics: unknown parameter warning") {
    val (analyzer, captured) = newAnalyzerWithClient()
    analyzer.updateDocument("test:uri2", "/api/v1/graph?q=:true&foo=bar")
    val params = captured.get()
    assert(params != null, "expected diagnostics")
    val diags = params.getDiagnostics.asScala.toList
    assert(
      diags.exists(d => diagMessage(d).contains("Unknown parameter 'foo'")),
      s"Expected unknown param: $diags"
    )
  }

  test("diagnostics: axis-suffixed params are known") {
    val (analyzer, captured) = newAnalyzerWithClient()
    analyzer.updateDocument("test:uri-axis", "/api/v1/graph?q=:true&ylabel.0=Requests")
    val params = captured.get()
    assert(params != null, "expected diagnostics")
    val diags = params.getDiagnostics.asScala.toList
    val unknowns = diags.filter(d => diagMessage(d).contains("Unknown parameter"))
    assertEquals(unknowns.size, 0, s"ylabel.0 should be known: $diags")
  }

  test("diagnostics: ASL errors shifted to URI coordinates") {
    val (analyzer, captured) = newAnalyzerWithClient()
    // :unknown is an unknown word in ASL
    // /api/v1/graph?q=:unknown
    // 0123456789012345678901234
    analyzer.updateDocument("test:uri3", "/api/v1/graph?q=:unknown")
    val params = captured.get()
    assert(params != null, "expected diagnostics")
    val diags = params.getDiagnostics.asScala.toList
    val aslDiags = diags.filter(_.getSource == "atlas")
    assert(aslDiags.nonEmpty, s"Expected ASL diagnostics: $diags")
    // All on line 0, character offset >= 16 (start of q value)
    val unknownDiag = aslDiags.head
    assertEquals(
      unknownDiag.getRange.getStart.getLine,
      0,
      s"Expected line 0: ${unknownDiag.getRange}"
    )
    assert(
      unknownDiag.getRange.getStart.getCharacter >= 16,
      s"Expected character >= 16: ${unknownDiag.getRange}"
    )
  }

  test("diagnostics: valid URI has no errors") {
    val (analyzer, captured) = newAnalyzerWithClient()
    analyzer.updateDocument("test:uri4", "/api/v1/graph?q=a,b,:swap&s=e-3h")
    val params = captured.get()
    assert(params != null, "expected diagnostics")
    val diags = params.getDiagnostics.asScala.toList
    assertEquals(diags.size, 0, s"Expected no diagnostics: $diags")
  }

  test("diagnostics: & separator not merged into expression value") {
    val (analyzer, captured) = newAnalyzerWithClient()
    // Use a simple expression so :swap is known in StandardVocabulary.
    // The key test: the '&' between q and s is a real separator, not part of q's value.
    val text = "/api/v1/graph?q=a,b,:swap&s=e-3h"
    analyzer.updateDocument("test:amp", text)
    val params = captured.get()
    assert(params != null, "expected diagnostics")
    val diags = params.getDiagnostics.asScala.toList
    // Should NOT have any unknown word containing '&' (e.g., ':swap&s=e-3h')
    val mergedWords =
      diags.filter(d => diagMessage(d).contains("&"))
    assertEquals(
      mergedWords.size,
      0,
      s"Expected no diagnostics with '&' in the word: $diags"
    )
  }

  test("diagnostics: percent-encoded & keeps expression intact") {
    val (analyzer, captured) = newAnalyzerWithClient()
    // q=a%26b should decode to "a&b" — a single expression value, not split
    val text = "/api/v1/graph?q=a%26b&s=e-3h"
    analyzer.updateDocument("test:pctamp", text)
    val params = captured.get()
    assert(params != null, "expected diagnostics")
    val diags = params.getDiagnostics.asScala.toList
    // 's' should be parsed as a separate param, not merged into q
    val unknownParams = diags.filter(d => diagMessage(d).contains("Unknown parameter"))
    assertEquals(unknownParams.size, 0, s"s should be known: $diags")
  }

  //
  // Document symbols
  //

  test("documentSymbol: path and params") {
    val analyzer = newAnalyzer()
    val text = "/api/v1/graph?q=a,b,:swap&s=e-3h"
    val symbols = analyzer.computeDocumentSymbols(text)
    // Path + q + s = 3 symbols
    assertEquals(symbols.size, 3)
    assertEquals(symbols(0).getName, "/api/v1/graph")
    assertEquals(symbols(1).getName, "q")
    assertEquals(symbols(2).getName, "s=e-3h")
  }

  test("documentSymbol: expression param nests ASL symbols") {
    val analyzer = newAnalyzer()
    val text = "/api/v1/graph?q=a,b,:swap"
    val symbols = analyzer.computeDocumentSymbols(text)
    val qSymbol = symbols.find(_.getName == "q").get
    val children = qSymbol.getChildren.asScala.toList
    assert(children.nonEmpty, "Expected ASL symbols nested under q")
  }

  //
  // Definition
  //

  test("definition: delegates to ASL within expression") {
    val analyzer = newAnalyzer()
    // /api/v1/graph?q=v,hello,:set,v,:get
    // 0123456789012345678901234567890123456
    val text = "/api/v1/graph?q=v,hello,:set,v,:get"
    val uri = "test:def"
    analyzer.updateDocument(uri, text)
    // :get is at offset 31 in the URI, offset 15 in the expression
    val defn = analyzer.computeDefinition(uri, text, 31)
    assert(defn.isDefined, "Expected definition result")
    // The definition range should be shifted to URI coordinates
    assert(
      defn.get.getRange.getStart.getCharacter >= 16,
      s"Expected shifted range: ${defn.get.getRange}"
    )
  }

  test("computeDefinition: returns None for non-expression position") {
    val (analyzer, _) = newAnalyzerWithClient()
    val text = "/api/v1/graph?q=a,b,:swap"
    val uri = "test:def2"
    analyzer.updateDocument(uri, text)
    // offset 5 is in the path, not an expression param
    val defn = analyzer.computeDefinition(uri, text, 5)
    assert(defn.isEmpty, "Expected no definition for non-expression position")
  }

  test("computeCodeActions: returns actions for expression params") {
    val (analyzer, _) = newAnalyzerWithClient()
    val text = "/api/v1/graph?q=a,b,;swap"
    val uri = "test:actions"
    analyzer.updateDocument(uri, text)
    val actions = analyzer.computeCodeActions(uri)
    // The semicolon typo in ;swap should produce a quick-fix action
    assert(actions.nonEmpty, s"Expected code actions for typo: $actions")
  }

  test("computeSemanticTokens: percent-encoded expression") {
    val (analyzer, _) = newAnalyzerWithClient()
    // a%2Cb%2C%3Aswap = a,b,:swap percent-encoded
    val text = "/api/v1/graph?q=a%2Cb%2C%3Aswap"
    val uri = "test:pctenc"
    analyzer.updateDocument(uri, text)
    val tokens = analyzer.computeSemanticTokens(text)
    // Should produce tokens — the percent-encoded expression should be decoded
    // and tokenized, with positions shifted back to raw URI coordinates
    assert(tokens.nonEmpty, s"Expected semantic tokens for percent-encoded expression")
  }
}

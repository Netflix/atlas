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

import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters.*

import com.netflix.atlas.core.uri.ParsedUri
import com.netflix.atlas.core.uri.QueryParam
import com.netflix.atlas.core.uri.UriParser
import com.netflix.atlas.core.util.Strings

import org.eclipse.lsp4j.CodeAction
import org.eclipse.lsp4j.Command
import org.eclipse.lsp4j.CompletionItem
import org.eclipse.lsp4j.CompletionItemKind
import org.eclipse.lsp4j.Diagnostic
import org.eclipse.lsp4j.DiagnosticSeverity
import org.eclipse.lsp4j.DocumentSymbol
import org.eclipse.lsp4j.Hover
import org.eclipse.lsp4j.Location
import org.eclipse.lsp4j.MarkupContent
import org.eclipse.lsp4j.MarkupKind
import org.eclipse.lsp4j.Position
import org.eclipse.lsp4j.PublishDiagnosticsParams
import org.eclipse.lsp4j.Range
import org.eclipse.lsp4j.SymbolKind
import org.eclipse.lsp4j.TextEdit
import org.eclipse.lsp4j.jsonrpc.messages.Either
import org.eclipse.lsp4j.services.LanguageClient

/**
  * URI analysis that wraps an [[AslDocumentAnalyzer]] via composition. Parses
  * Atlas URIs (e.g. `/api/v1/graph?q=name,sps,:eq,:sum&s=e-3h`) and delegates
  * expression analysis within `q=` and `cq=` parameters to the ASL analyzer.
  *
  * The document model is a single-line compact URI. Visual word wrap is handled
  * by the editor (Monaco `wordWrap: 'on'`), so all LSP positions use line 0.
  */
class UriDocumentAnalyzer(
  private val asl: AslDocumentAnalyzer,
  private val clientSupplier: () => LanguageClient = () => null
) extends DocumentAnalyzer {

  private val documents = new ConcurrentHashMap[String, String]

  override def updateDocument(uri: String, text: String): Unit = {
    documents.put(uri, text)
    publishDiagnostics(uri, text)
  }

  override def removeDocument(uri: String): Unit = {
    documents.remove(uri)
  }

  override def getText(uri: String): String = {
    documents.getOrDefault(uri, "")
  }

  // -------------------------------------------------------------------
  // URI parsing
  // -------------------------------------------------------------------

  private val expressionParams = Set("q", "cq")

  private def isExpression(param: QueryParam): Boolean = {
    expressionParams.contains(param.key.text)
  }

  private def findParam(parsed: ParsedUri, offset: Int): Option[QueryParam] = {
    parsed.query.find(p => offset >= p.key.start && offset <= p.value.end)
  }

  private def inParamName(param: QueryParam, offset: Int): Boolean = {
    offset >= param.key.start && offset < param.key.end
  }

  private def inParamValue(param: QueryParam, offset: Int): Boolean = {
    offset >= param.value.start && offset <= param.value.end
  }

  /** Build and cache the offset map for a param to avoid rebuilding per call. */
  private def offsetMap(param: QueryParam): Array[Int] = {
    offsetMapCache.computeIfAbsent(
      param.value.text,
      _ => UriParser.buildOffsetMap(param.value.text)
    )
  }

  private val offsetMapCache =
    new java.util.concurrent.ConcurrentHashMap[String, Array[Int]]()

  private def rawOffsetToDecoded(param: QueryParam, uriOffset: Int): Int = {
    val rawRelative = uriOffset - param.value.start
    if (param.value.text == param.decodedValue) return rawRelative
    val map = offsetMap(param)
    var di = 0
    while (di + 1 < map.length && map(di + 1) <= rawRelative) {
      di += 1
    }
    di
  }

  /**
    * Translate a decoded-expression offset back to a raw URI offset using the
    * offset map.
    */
  private def decodedOffsetToRaw(param: QueryParam, decodedOffset: Int): Int = {
    if (param.value.text == param.decodedValue)
      return param.value.start + decodedOffset
    val map = offsetMap(param)
    val rawRelative = if (decodedOffset < map.length) map(decodedOffset) else map.last
    param.value.start + rawRelative
  }

  // -------------------------------------------------------------------
  // Known parameters
  // -------------------------------------------------------------------

  private val knownParams: Map[String, String] = Map(
    "q"               -> "Atlas Stack Language query expression",
    "cq"              -> "Common query applied to all expressions",
    "s"               -> "Start time (e.g. e-3h, 2024-01-01T00:00)",
    "e"               -> "End time (e.g. now, 2024-01-01T06:00)",
    "tz"              -> "Time zone (e.g. US/Pacific, UTC)",
    "step"            -> "Step size (e.g. 1m, 60s)",
    "format"          -> "Output format (png, json, csv, etc.)",
    "w"               -> "Image width in pixels",
    "h"               -> "Image height in pixels",
    "zoom"            -> "Zoom factor for the rendered image",
    "layout"          -> "Graph layout (canvas, image)",
    "title"           -> "Title displayed above the graph",
    "palette"         -> "Color palette name",
    "theme"           -> "Graph theme (light, dark)",
    "no_legend"       -> "Disable the legend (1 to enable)",
    "no_legend_stats" -> "Disable legend statistics (1 to enable)",
    "only_graph"      -> "Show only the graph area (1 to enable)",
    "no_border"       -> "Disable the border (1 to enable)",
    "axis_per_line"   -> "Use a separate axis for each line (1 to enable)",
    "u"               -> "Upper bound for the Y axis",
    "l"               -> "Lower bound for the Y axis",
    "scale"           -> "Y-axis scale (linear, log, pow2, sqrt)",
    "o"               -> "Legacy log scale (1 to enable)",
    "stack"           -> "Stack lines on the graph (1 to enable)",
    "ylabel"          -> "Label for the Y axis",
    "tick_labels"     -> "Tick label mode (decimal, binary, off)",
    "sort"            -> "Legend sort mode (name, min, max, avg, last, count)",
    "order"           -> "Legend sort order (asc, desc)",
    "features"        -> "Feature flags for the graph rendering",
    "id"              -> "Identifier for the graph",
    "vision"          -> "Color vision simulation (protanopia, deuteranopia, tritanopia)",
    "hints"           -> "Rendering hints",
    "heatmap_palette" -> "Color palette for heatmap rendering",
    "heatmap_scale"   -> "Scale for heatmap rendering",
    "heatmap_u"       -> "Upper bound for the heatmap color axis",
    "heatmap_l"       -> "Lower bound for the heatmap color axis",
    "heatmap_label"   -> "Label for the heatmap color axis"
  )

  /** Axis-per-line parameters that can have a `.N` numeric suffix. */
  private val axisSuffixParams: Set[String] = Set(
    "u",
    "l",
    "scale",
    "o",
    "stack",
    "ylabel",
    "tick_labels",
    "sort",
    "order",
    "palette",
    "heatmap_palette",
    "heatmap_scale",
    "heatmap_u",
    "heatmap_l",
    "heatmap_label"
  )

  private def isKnownParam(name: String): Boolean = {
    if (knownParams.contains(name)) return true
    // Check for .N suffix (e.g., ylabel.0, u.1)
    val dotIdx = name.indexOf('.')
    if (dotIdx > 0 && dotIdx < name.length - 1) {
      val base = name.substring(0, dotIdx)
      val suffix = name.substring(dotIdx + 1)
      axisSuffixParams.contains(base) && suffix.forall(_.isDigit)
    } else {
      false
    }
  }

  private def paramDescription(name: String): Option[String] = {
    knownParams.get(name).orElse {
      val dotIdx = name.indexOf('.')
      if (dotIdx > 0) {
        val base = name.substring(0, dotIdx)
        knownParams.get(base).map(d => s"$d (axis ${name.substring(dotIdx + 1)})")
      } else {
        None
      }
    }
  }

  // -------------------------------------------------------------------
  // Hover
  // -------------------------------------------------------------------

  override def computeHover(text: String, offset: Int): Option[Hover] = {
    val parsed = UriParser.parse(text)

    // Hovering on path
    if (offset >= parsed.path.start && offset < parsed.path.end) {
      return Some(markdownHover(s"**Path:** `${parsed.path.text}`"))
    }

    findParam(parsed, offset) match {
      case Some(param) if inParamName(param, offset) =>
        paramDescriptionHover(param.key.text)
      case Some(param) if inParamValue(param, offset) && isExpression(param) =>
        val exprOffset = rawOffsetToDecoded(param, offset)
        asl
          .computeHover(param.decodedValue, exprOffset)
          .map(shiftHoverDecoded(_, param))
      case Some(param) if inParamValue(param, offset) =>
        paramDescriptionHover(param.key.text)
      case _ =>
        None
    }
  }

  private def paramDescriptionHover(name: String): Option[Hover] = {
    paramDescription(name).map { desc =>
      markdownHover(s"**$name:** $desc")
    }
  }

  private def markdownHover(md: String): Hover = {
    val content = new MarkupContent(MarkupKind.MARKDOWN, md)
    new Hover(content)
  }

  // -------------------------------------------------------------------
  // Completions
  // -------------------------------------------------------------------

  override def computeCompletions(text: String, offset: Int): List[CompletionItem] = {
    val parsed = UriParser.parse(text)

    findParam(parsed, offset) match {
      case Some(param) if inParamValue(param, offset) && isExpression(param) =>
        val exprOffset = rawOffsetToDecoded(param, offset)
        asl
          .computeCompletions(param.decodedValue, exprOffset)
          .map(shiftCompletionDecoded(_, param))
      case _ =>
        // Offer known parameter names
        val prefix =
          if (offset > 0) {
            var start = offset
            while (
              start > 0 && text.charAt(start - 1) != '?' &&
              text.charAt(start - 1) != '&'
            ) {
              start -= 1
            }
            text.substring(start, offset)
          } else ""

        knownParams.toList
          .filter(_._1.startsWith(prefix))
          .map {
            case (name, desc) =>
              val item = new CompletionItem(name)
              item.setKind(CompletionItemKind.Property)
              item.setDetail(desc)
              val startPos = new Position(0, offset - prefix.length)
              val endPos = new Position(0, offset)
              val replaceRange = new Range(startPos, endPos)
              item.setTextEdit(Either.forLeft(new TextEdit(replaceRange, s"$name=")))
              item
          }
    }
  }

  // -------------------------------------------------------------------
  // Semantic tokens
  // -------------------------------------------------------------------

  override def computeSemanticTokens(text: String): List[Integer] = {
    val parsed = UriParser.parse(text)
    val tokens = scala.collection.mutable.ListBuffer.empty[(Int, Int, Int)]

    // Path token
    if (parsed.path.end > parsed.path.start) {
      tokens += ((parsed.path.start, parsed.path.end - parsed.path.start, AslTokenTypes.Path))
    }

    // ? operator
    val qIdx = text.indexOf('?')
    if (qIdx >= 0) {
      tokens += ((qIdx, 1, AslTokenTypes.UriOperator))
    }

    for (param <- parsed.query) {
      // & before this param (except the first)
      if (param.key.start > 0 && param.key.start > parsed.path.end + 1) {
        val ampPos = param.key.start - 1
        if (ampPos >= 0 && ampPos < text.length && text.charAt(ampPos) == '&') {
          tokens += ((ampPos, 1, AslTokenTypes.UriOperator))
        }
      }

      // Parameter name
      tokens += ((
        param.key.start,
        param.key.end - param.key.start,
        AslTokenTypes.Parameter
      ))

      // = operator (only if there's a value span)
      if (param.key.end < param.value.start) {
        tokens += ((param.key.end, 1, AslTokenTypes.UriOperator))
      }

      // Value
      if (isExpression(param)) {
        val aslTokens = asl.computeSemanticTokens(param.decodedValue)
        tokens ++= shiftSemanticTokensDecoded(aslTokens, param)
      } else if (param.value.end > param.value.start) {
        tokens += ((
          param.value.start,
          param.value.end - param.value.start,
          AslTokenTypes.String
        ))
      }
    }

    // Sort by position and encode as LSP delta format (all on line 0)
    encodeSemanticTokens(tokens.sortBy(_._1).toList)
  }

  /** Decode ASL semantic token integers and shift to raw URI positions. */
  private def shiftSemanticTokensDecoded(
    data: List[Integer],
    param: QueryParam
  ): List[(Int, Int, Int)] = {
    // ASL expressions are single-line, so delta line is always 0.
    // We only need to accumulate the column delta.
    var col = 0
    data
      .map(_.intValue())
      .grouped(5)
      .flatMap { group =>
        if (group.size < 5) None
        else {
          col = if (group(0) > 0) group(1) else col + group(1)
          val absPos = decodedOffsetToRaw(param, col)
          val endPos = decodedOffsetToRaw(param, col + group(2))
          Some((absPos, endPos - absPos, group(3)))
        }
      }
      .toList
  }

  /**
    * Encode (offset, length, tokenType) triples as LSP delta-encoded
    * integers. All tokens are on line 0, so deltaLine is always 0.
    */
  private def encodeSemanticTokens(
    tokens: List[(Int, Int, Int)]
  ): List[Integer] = {
    var prevCol = 0
    tokens.flatMap {
      case (absPos, length, tokenType) =>
        val deltaCol = absPos - prevCol
        prevCol = absPos
        List[Integer](0, deltaCol, length, tokenType, 0)
    }
  }

  // -------------------------------------------------------------------
  // Code actions
  // -------------------------------------------------------------------

  override def computeCodeActions(uri: String): List[Either[Command, CodeAction]] = {
    val text = getText(uri)
    val parsed = UriParser.parse(text)
    parsed.query
      .filter(isExpression)
      .zipWithIndex
      .flatMap {
        case (param, idx) =>
          val tempUri = s"$uri#${param.key.text}.$idx"
          asl.updateDocument(tempUri, param.decodedValue)
          val actions = asl.computeCodeActions(tempUri)
          asl.removeDocument(tempUri)
          actions.map(shiftCodeActionDecoded(_, param, uri, text))
      }
  }

  // -------------------------------------------------------------------
  // Definition
  // -------------------------------------------------------------------

  override def computeDefinition(
    uri: String,
    text: String,
    offset: Int
  ): Option[Location] = {
    val parsed = UriParser.parse(text)
    findParam(parsed, offset) match {
      case Some(param) if inParamValue(param, offset) && isExpression(param) =>
        val exprOffset = rawOffsetToDecoded(param, offset)
        asl
          .computeDefinition(uri, param.decodedValue, exprOffset)
          .map(shiftLocationDecoded(_, param, uri))
      case _ =>
        None
    }
  }

  // -------------------------------------------------------------------
  // Document symbols
  // -------------------------------------------------------------------

  override def computeDocumentSymbols(text: String): List[DocumentSymbol] = {
    val parsed = UriParser.parse(text)
    val symbols = scala.collection.mutable.ListBuffer.empty[DocumentSymbol]

    // Path symbol
    if (parsed.path.end > parsed.path.start) {
      val pathStart = new Position(0, parsed.path.start)
      val pathEnd = new Position(0, parsed.path.end)
      val pathRange = new Range(pathStart, pathEnd)
      val pathSymbol = new DocumentSymbol(
        parsed.path.text,
        SymbolKind.Namespace,
        pathRange,
        pathRange
      )
      symbols += pathSymbol
    }

    for (param <- parsed.query) {
      val range = new Range(
        new Position(0, param.key.start),
        new Position(0, param.value.end)
      )
      val selRange = new Range(
        new Position(0, param.key.start),
        new Position(0, param.key.end)
      )

      if (isExpression(param)) {
        val aslSymbols = asl.computeDocumentSymbols(param.decodedValue)
        val shifted = aslSymbols.map(shiftDocumentSymbolDecoded(_, param))
        val paramSymbol = new DocumentSymbol(
          param.key.text,
          SymbolKind.Function,
          range,
          selRange
        )
        paramSymbol.setChildren(shifted.asJava)
        symbols += paramSymbol
      } else {
        val paramSymbol = new DocumentSymbol(
          s"${param.key.text}=${param.value.text}",
          SymbolKind.Property,
          range,
          selRange
        )
        symbols += paramSymbol
      }
    }

    symbols.toList
  }

  // -------------------------------------------------------------------
  // Diagnostics
  // -------------------------------------------------------------------

  private[lsp] def publishDiagnostics(uri: String, text: String): Unit = {
    val client = clientSupplier()
    if (client == null) return
    val parsed = UriParser.parse(text)
    val diags = scala.collection.mutable.ListBuffer.empty[Diagnostic]

    // Check for missing q parameter
    val hasQuery = parsed.query.exists(_.key.text == "q")
    if (parsed.query.nonEmpty && !hasQuery) {
      val range = new Range(new Position(0, 0), new Position(0, text.length))
      diags += new Diagnostic(
        range,
        "Missing required 'q' parameter",
        DiagnosticSeverity.Error,
        "atlas-uri"
      )
    }

    // Check for unknown parameters
    for (param <- parsed.query) {
      if (!isKnownParam(param.key.text)) {
        val range = new Range(
          new Position(0, param.key.start),
          new Position(0, param.key.end)
        )
        diags += new Diagnostic(
          range,
          s"Unknown parameter '${param.key.text}'",
          DiagnosticSeverity.Warning,
          "atlas-uri"
        )
      }
    }

    // ASL diagnostics for expression parameters
    for (param <- parsed.query if isExpression(param)) {
      val aslDiags = asl.computeLspDiagnostics(param.decodedValue)
      for (d <- aslDiags.asScala) {
        val shiftedRange = shiftRangeDecoded(d.getRange, param)
        val msg =
          if (d.getMessage.isLeft) d.getMessage.getLeft
          else d.getMessage.getRight.getValue
        diags += new Diagnostic(shiftedRange, msg, d.getSeverity, "atlas")
      }
    }

    val params = new PublishDiagnosticsParams(uri, diags.asJava)
    client.publishDiagnostics(params)
  }

  // -------------------------------------------------------------------
  // Offset shifting helpers (decoded → raw URI, all on line 0)
  // -------------------------------------------------------------------

  private def shiftPositionDecoded(
    pos: Position,
    param: QueryParam
  ): Position = {
    if (pos.getLine == 0) {
      new Position(0, decodedOffsetToRaw(param, pos.getCharacter))
    } else {
      pos
    }
  }

  private def shiftRangeDecoded(
    range: Range,
    param: QueryParam
  ): Range = {
    new Range(
      shiftPositionDecoded(range.getStart, param),
      shiftPositionDecoded(range.getEnd, param)
    )
  }

  private def shiftHoverDecoded(
    hover: Hover,
    param: QueryParam
  ): Hover = {
    if (hover.getRange != null) {
      val shifted = new Hover()
      shifted.setContents(hover.getContents)
      shifted.setRange(shiftRangeDecoded(hover.getRange, param))
      shifted
    } else {
      hover
    }
  }

  private def shiftCompletionDecoded(
    item: CompletionItem,
    param: QueryParam
  ): CompletionItem = {
    if (item.getTextEdit != null && item.getTextEdit.isLeft) {
      val edit = item.getTextEdit.getLeft
      val range = shiftRangeDecoded(edit.getRange, param)
      item.setTextEdit(Either.forLeft(new TextEdit(range, edit.getNewText)))
    }
    item
  }

  private def shiftCodeActionDecoded(
    either: Either[Command, CodeAction],
    param: QueryParam,
    docUri: String,
    text: String
  ): Either[Command, CodeAction] = {
    if (either.isRight) {
      val action = either.getRight
      if (action.getEdit != null && action.getEdit.getChanges != null) {
        val newChanges = new java.util.HashMap[String, java.util.List[TextEdit]]()
        for ((_, edits) <- action.getEdit.getChanges.asScala) {
          val shifted = edits.asScala.map { edit =>
            // Compact the new expression (strip whitespace/newlines from formatted ASL)
            val newExpr = edit.getNewText
              .split("\n")
              .map(_.trim)
              .filter(_.nonEmpty)
              .mkString("")
            // Splice into the raw (percent-encoded) param value so that existing
            // encoding (e.g. %20 for spaces in legend text) is preserved. Translate
            // the decoded edit range to raw offsets, then encode only the new text.
            val rawStart = decodedOffsetToRaw(param, edit.getRange.getStart.getCharacter)
            val rawEnd = decodedOffsetToRaw(param, edit.getRange.getEnd.getCharacter)
            val encoded = Strings.urlEncode(newExpr)
            val newText = text.substring(0, rawStart) +
              encoded +
              text.substring(rawEnd)
            new TextEdit(new Range(new Position(0, 0), new Position(0, text.length)), newText)
          }
          newChanges.put(docUri, shifted.asJava)
        }
        action.getEdit.setChanges(newChanges)
      }
      Either.forRight(action)
    } else {
      either
    }
  }

  private def shiftLocationDecoded(
    loc: Location,
    param: QueryParam,
    docUri: String
  ): Location = {
    new Location(docUri, shiftRangeDecoded(loc.getRange, param))
  }

  private def shiftDocumentSymbolDecoded(
    sym: DocumentSymbol,
    param: QueryParam
  ): DocumentSymbol = {
    val shifted = new DocumentSymbol(
      sym.getName,
      sym.getKind,
      shiftRangeDecoded(sym.getRange, param),
      shiftRangeDecoded(sym.getSelectionRange, param)
    )
    shifted.setDetail(sym.getDetail)
    if (sym.getChildren != null) {
      shifted.setChildren(
        sym.getChildren.asScala.map(shiftDocumentSymbolDecoded(_, param)).asJava
      )
    }
    shifted
  }
}

/**
  * Minimal LanguageClient that captures the last publishDiagnostics call.
  */
private class CapturingLanguageClient(
  captured: java.util.concurrent.atomic.AtomicReference[PublishDiagnosticsParams]
) extends LanguageClient {

  override def telemetryEvent(obj: Any): Unit = ()

  override def publishDiagnostics(params: PublishDiagnosticsParams): Unit = {
    captured.set(params)
  }
  override def showMessage(params: org.eclipse.lsp4j.MessageParams): Unit = ()

  override def showMessageRequest(
    params: org.eclipse.lsp4j.ShowMessageRequestParams
  ): java.util.concurrent.CompletableFuture[org.eclipse.lsp4j.MessageActionItem] =
    java.util.concurrent.CompletableFuture.completedFuture(null)
  override def logMessage(params: org.eclipse.lsp4j.MessageParams): Unit = ()
}

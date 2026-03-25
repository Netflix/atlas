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
import org.eclipse.lsp4j.CompletionItem
import org.eclipse.lsp4j.DocumentSymbol
import org.eclipse.lsp4j.Hover
import org.eclipse.lsp4j.Location
import org.eclipse.lsp4j.jsonrpc.messages.Either

/**
  * Shared trait for document analysis. [[AslDocumentAnalyzer]] handles raw ASL
  * expressions; [[UriDocumentAnalyzer]] handles Atlas URIs and delegates expression
  * analysis within `q=` and `cq=` parameters to an underlying ASL analyzer.
  */
trait DocumentAnalyzer {

  def updateDocument(uri: String, text: String): Unit

  def removeDocument(uri: String): Unit

  def getText(uri: String): String

  def computeHover(text: String, offset: Int): Option[Hover]

  def computeCompletions(text: String, offset: Int): List[CompletionItem]

  def computeSemanticTokens(text: String): List[Integer]

  def computeCodeActions(uri: String): List[Either[Command, CodeAction]]

  def computeDefinition(uri: String, text: String, offset: Int): Option[Location]

  def computeDocumentSymbols(text: String): List[DocumentSymbol]
}

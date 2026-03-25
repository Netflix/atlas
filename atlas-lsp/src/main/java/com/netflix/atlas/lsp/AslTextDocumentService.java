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
package com.netflix.atlas.lsp;

import java.util.concurrent.CompletableFuture;

import org.eclipse.lsp4j.CodeAction;
import org.eclipse.lsp4j.CodeActionParams;
import org.eclipse.lsp4j.Command;
import org.eclipse.lsp4j.CompletionList;
import org.eclipse.lsp4j.CompletionItem;
import org.eclipse.lsp4j.CompletionParams;
import org.eclipse.lsp4j.DidChangeTextDocumentParams;
import org.eclipse.lsp4j.DidCloseTextDocumentParams;
import org.eclipse.lsp4j.DidOpenTextDocumentParams;
import org.eclipse.lsp4j.DidSaveTextDocumentParams;
import org.eclipse.lsp4j.DefinitionParams;
import org.eclipse.lsp4j.DocumentSymbol;
import org.eclipse.lsp4j.DocumentSymbolParams;
import org.eclipse.lsp4j.Hover;
import org.eclipse.lsp4j.HoverParams;
import org.eclipse.lsp4j.Location;
import org.eclipse.lsp4j.LocationLink;
import org.eclipse.lsp4j.SemanticTokens;
import org.eclipse.lsp4j.SemanticTokensParams;
import org.eclipse.lsp4j.SymbolInformation;
import org.eclipse.lsp4j.jsonrpc.messages.Either;
import org.eclipse.lsp4j.services.TextDocumentService;

import scala.jdk.javaapi.CollectionConverters;

/**
 * Java adapter for TextDocumentService that delegates to a DocumentAnalyzer.
 * Written in Java to avoid Scala/JDK annotation interop issues with LSP4j.
 */
public class AslTextDocumentService implements TextDocumentService {

    private final DocumentAnalyzer analyzer;

    public AslTextDocumentService(DocumentAnalyzer analyzer) {
        this.analyzer = analyzer;
    }

    DocumentAnalyzer analyzer() {
        return analyzer;
    }

    @Override
    public void didOpen(DidOpenTextDocumentParams params) {
        var doc = params.getTextDocument();
        analyzer.updateDocument(doc.getUri(), doc.getText());
    }

    @Override
    public void didChange(DidChangeTextDocumentParams params) {
        var uri = params.getTextDocument().getUri();
        var text = analyzer.getText(uri);
        for (var change : params.getContentChanges()) {
            if (change.getRange() == null) {
                // Full sync: no range means entire document replacement
                text = change.getText();
            } else {
                // Incremental sync: apply range-based edit
                var start = positionToOffset(text, change.getRange().getStart().getLine(),
                        change.getRange().getStart().getCharacter());
                var end = positionToOffset(text, change.getRange().getEnd().getLine(),
                        change.getRange().getEnd().getCharacter());
                text = text.substring(0, start) + change.getText() + text.substring(end);
            }
        }
        analyzer.updateDocument(uri, text);
    }

    @Override
    public void didClose(DidCloseTextDocumentParams params) {
        analyzer.removeDocument(params.getTextDocument().getUri());
    }

    @Override
    public void didSave(DidSaveTextDocumentParams params) {}

    @Override
    public CompletableFuture<Either<java.util.List<CompletionItem>, CompletionList>> completion(
            CompletionParams params) {
        var uri = params.getTextDocument().getUri();
        var text = analyzer.getText(uri);
        var pos = params.getPosition();
        var offset = positionToOffset(text, pos.getLine(), pos.getCharacter());
        var items = analyzer.computeCompletions(text, offset);
        var javaItems = CollectionConverters.asJava(items);
        var result = Either.<java.util.List<CompletionItem>, CompletionList>forLeft(
                new java.util.ArrayList<>(javaItems));
        return CompletableFuture.completedFuture(result);
    }

    @Override
    public CompletableFuture<java.util.List<Either<Command, CodeAction>>> codeAction(
            CodeActionParams params) {
        var uri = params.getTextDocument().getUri();
        var actions = analyzer.computeCodeActions(uri);
        var javaActions = CollectionConverters.asJava(actions);
        return CompletableFuture.completedFuture(new java.util.ArrayList<>(javaActions));
    }

    @Override
    public CompletableFuture<Either<java.util.List<? extends Location>, java.util.List<? extends LocationLink>>> definition(
            DefinitionParams params) {
        var uri = params.getTextDocument().getUri();
        var text = analyzer.getText(uri);
        var pos = params.getPosition();
        var offset = positionToOffset(text, pos.getLine(), pos.getCharacter());
        var location = analyzer.computeDefinition(uri, text, offset);
        java.util.List<Location> locations = location.isDefined()
                ? java.util.List.of(location.get())
                : java.util.List.of();
        return CompletableFuture.completedFuture(Either.forLeft(locations));
    }

    @Override
    public CompletableFuture<Hover> hover(HoverParams params) {
        var uri = params.getTextDocument().getUri();
        var text = analyzer.getText(uri);
        var pos = params.getPosition();
        var offset = positionToOffset(text, pos.getLine(), pos.getCharacter());
        var hover = analyzer.computeHover(text, offset);
        return CompletableFuture.completedFuture(hover.getOrElse(() -> null));
    }

    @Override
    public CompletableFuture<SemanticTokens> semanticTokensFull(SemanticTokensParams params) {
        var uri = params.getTextDocument().getUri();
        var text = analyzer.getText(uri);
        var data = analyzer.computeSemanticTokens(text);
        var javaData = CollectionConverters.asJava(data);
        return CompletableFuture.completedFuture(new SemanticTokens(new java.util.ArrayList<>(javaData)));
    }

    @Override
    public CompletableFuture<java.util.List<Either<SymbolInformation, DocumentSymbol>>> documentSymbol(
            DocumentSymbolParams params) {
        var uri = params.getTextDocument().getUri();
        var text = analyzer.getText(uri);
        var symbols = analyzer.computeDocumentSymbols(text);
        var javaSymbols = CollectionConverters.asJava(symbols);
        var result = new java.util.ArrayList<Either<SymbolInformation, DocumentSymbol>>();
        for (var sym : javaSymbols) {
            result.add(Either.forRight(sym));
        }
        return CompletableFuture.completedFuture(result);
    }

    /** Convert an LSP line/character position to an absolute character offset. */
    static int positionToOffset(String text, int line, int character) {
        int offset = 0;
        int currentLine = 0;
        while (currentLine < line && offset < text.length()) {
            if (text.charAt(offset) == '\n') {
                currentLine++;
            }
            offset++;
        }
        return Math.min(offset + character, text.length());
    }
}
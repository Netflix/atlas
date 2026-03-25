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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.atlas.core.stacklang.Interpreter;
import com.netflix.atlas.core.stacklang.Vocabulary;
import org.eclipse.lsp4j.CodeActionKind;
import org.eclipse.lsp4j.CodeActionOptions;
import org.eclipse.lsp4j.CompletionOptions;
import org.eclipse.lsp4j.InitializeParams;
import org.eclipse.lsp4j.InitializeResult;
import org.eclipse.lsp4j.SemanticTokensWithRegistrationOptions;
import org.eclipse.lsp4j.ServerCapabilities;
import org.eclipse.lsp4j.TextDocumentSyncKind;
import org.eclipse.lsp4j.TextDocumentSyncOptions;
import org.eclipse.lsp4j.services.LanguageClient;
import org.eclipse.lsp4j.services.LanguageClientAware;
import org.eclipse.lsp4j.services.LanguageServer;
import org.eclipse.lsp4j.services.TextDocumentService;
import org.eclipse.lsp4j.services.WorkspaceService;

/**
 * URI flavor LSP server for Atlas URIs. Written in Java to avoid Scala compiler
 * issues with LSP4j annotation propagation on bridge methods.
 */
public class UriLspServer implements LanguageServer, LanguageClientAware {

    private final AtomicReference<LanguageClient> clientRef = new AtomicReference<>();

    private final AslTextDocumentService textDocService;
    private final AslWorkspaceService workspaceService;
    private final UriDocumentAnalyzer analyzer;

    public UriLspServer(Vocabulary vocabulary) {
        this(vocabulary, Glossary.empty());
    }

    public UriLspServer(Vocabulary vocabulary, Glossary glossary) {
        var interpreter = Interpreter.apply(vocabulary.allWords());
        var aslAnalyzer = new AslDocumentAnalyzer(interpreter, glossary, this::client);
        this.analyzer = new UriDocumentAnalyzer(aslAnalyzer, this::client);
        this.textDocService = new AslTextDocumentService(analyzer);
        this.workspaceService = new AslWorkspaceService();
    }

    public UriDocumentAnalyzer analyzer() {
        return analyzer;
    }

    public LanguageClient client() {
        return clientRef.get();
    }

    @Override
    public CompletableFuture<InitializeResult> initialize(InitializeParams params) {
        var capabilities = new ServerCapabilities();
        var syncOptions = new TextDocumentSyncOptions();
        syncOptions.setOpenClose(true);
        syncOptions.setChange(TextDocumentSyncKind.Incremental);
        capabilities.setTextDocumentSync(syncOptions);
        var completionOptions = new CompletionOptions();
        completionOptions.setTriggerCharacters(List.of(":", "&", "="));
        capabilities.setCompletionProvider(completionOptions);
        var semanticTokensOptions = new SemanticTokensWithRegistrationOptions();
        semanticTokensOptions.setLegend(AslTokenTypes.legend());
        semanticTokensOptions.setFull(true);
        capabilities.setSemanticTokensProvider(semanticTokensOptions);
        var codeActionOptions = new CodeActionOptions(List.of(CodeActionKind.QuickFix, CodeActionKind.RefactorRewrite));
        capabilities.setCodeActionProvider(codeActionOptions);
        capabilities.setHoverProvider(true);
        capabilities.setDefinitionProvider(true);
        capabilities.setDocumentSymbolProvider(true);
        return CompletableFuture.completedFuture(new InitializeResult(capabilities));
    }

    @Override
    public CompletableFuture<Object> shutdown() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void exit() {}

    @Override
    public TextDocumentService getTextDocumentService() {
        return textDocService;
    }

    @Override
    public WorkspaceService getWorkspaceService() {
        return workspaceService;
    }

    @Override
    public void connect(LanguageClient client) {
        clientRef.set(client);
    }
}

# Atlas LSP

Language Server Protocol implementation for the Atlas Stack Language. Designed
to be used by both editors and AI tools that need deterministic context about
Atlas expressions and graph URIs.

## Usage

### Test Client

A browser-based editor for testing the LSP server lives in [test-client/](test-client/).
It uses [Monaco Editor](https://microsoft.github.io/monaco-editor/) 0.55+ with its
built-in LSP client — no custom protocol handling required.

1. Start the LSP server:

   ```
   project/sbt 'atlas-lsp/test:runMain com.netflix.atlas.lsp.AslLspRunner'
   ```

2. In another terminal, start the test client:

   ```
   cd atlas-lsp/test-client
   npm install
   npm run dev
   ```

3. Open the URL printed by Vite (typically http://localhost:5173).

The connection uses Monaco's built-in LSP client:

```js
const ws = new WebSocket('ws://localhost:7102');
const transport = monaco.lsp.WebSocketTransport.fromWebSocket(ws);
new monaco.lsp.MonacoLspClient(transport);
```

### Editor Integration

The LSP server communicates over standard JSON-RPC. To use with an editor:

1. Build the LSP server jar
2. Launch it with stdin/stdout transport
3. Configure your editor's LSP client to connect

Currently supported capabilities:
- **Completions** — context-aware word suggestions filtered by stack state
- **Semantic tokens** — syntax highlighting for words, strings, numbers, parentheses, and comments
- **Hover** — word summary, stack signature, examples, and stack state introspection on hover
- **Go to definition** — jump from `:get` to corresponding `:set` definition
- **Diagnostics** — error and warning reporting for invalid expressions
- **Code actions** — expression formatting, compression, and normalization via refactor/rewrite

## TODO

### Code Actions
- [ ] Improve format action — macros (`:stack`, `:area`, etc.) need TypedMacro with declared pop/push counts so the formatter can group them correctly with their arguments
- [x] Add compress action
- [x] Add normalize action
- [ ] Add rewrite action

### Completions
- [ ] Tag value completion
- [ ] Tag key completion
- [x] Unicode character completion
- [ ] Improved operator completion

### Editor Features
- [x] Hover documentation — show word summary and stack signature on hover
- [x] Go to definition — jump to variable definitions for `:set`/`:get`

### Expression Introspection (for AI tooling)
- [x] Stack state inspection — hover shows consumed/produced stack items for each word
- [ ] Operator documentation — given an expression, return docs for each operator used
- [ ] Glossary lookup — connect metrics used in an expression to glossary docs when available
- [ ] Expression decomposition — break multi-expression queries into labeled constituent parts
- [ ] URI ↔ expression conversion — round-trip between graph URI form and raw expression form
- [ ] Structured validation — return errors with kind, expected/actual types, and fix suggestions
- [ ] Batch operations — accept multiple expressions per request for validation/formatting

### Other
- [ ] Glossary support
- [ ] Improve operation docs
- [ ] ASL vs URI LSP support

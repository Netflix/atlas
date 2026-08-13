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

## Glossary

`Glossary` (see [Glossary.scala](src/main/scala/com/netflix/atlas/lsp/Glossary.scala) and
[GLOSSARY_SPEC.md](GLOSSARY_SPEC.md)) supplies metric/tag descriptions, units, and enumerated
values that power hover text and completions in `AslDocumentAnalyzer`. This module ships only the
mechanism, not any glossary data — `Glossary.loadAll(config)` auto-discovers every
`META-INF/atlas/glossary/*.json` fragment on the classpath and merges in any additional files
listed under `atlas.lsp.glossary.files` in config, so a fresh `AslLspServer`/`UriLspServer` gets
the merged result for free as soon as a fragment-bearing jar or config entry is added.

To add a fragment as a library author, ship a JSON file at that same classpath path in your jar —
no code or config changes needed downstream. Deployment-specific or internal fragments (e.g.
Netflix-owned metric catalogs) belong in the consuming application's own resources, not in this
repo. To add one as an application owner without publishing a jar, point `atlas.lsp.glossary.files`
at a classpath resource instead:

```hocon
atlas.lsp.glossary.files = [ "my-app-glossary.json" ]
```

See `GlossarySuite` for the merge/discovery semantics, and `GLOSSARY_SPEC.md` for the full JSON
schema (`metrics`, `tagKeys`, `tagValues`, value types, and merge rules).


import * as monaco from 'monaco-editor';
import editorWorker from 'monaco-editor/esm/vs/editor/editor.worker?worker';

// Monaco worker setup
self.MonacoEnvironment = {
  getWorker: () => new editorWorker(),
};

// Register the Atlas language
monaco.languages.register({ id: 'atlas' });
monaco.languages.setLanguageConfiguration('atlas', {
  wordPattern: /[^\s,()]+/,
  comments: { blockComment: ['/*', '*/'] },
});

// Theme with semantic token colors
monaco.editor.defineTheme('atlas-dark', {
  base: 'vs-dark',
  inherit: true,
  rules: [
    { token: 'keyword',    foreground: 'DCDCAA', fontStyle: 'bold' },
    { token: 'operator',   foreground: 'DCDCAA', fontStyle: 'bold' },
    { token: 'function',   foreground: 'DCDCAA' },
    { token: 'string',     foreground: 'CE9178' },
    { token: 'number',     foreground: 'B5CEA8' },
    { token: 'comment',    foreground: '6A9955', fontStyle: 'italic' },
    { token: 'variable',   foreground: '9CDCFE' },
    { token: 'type',       foreground: '4EC5D4' },
    { token: 'parameter',  foreground: '9CDCFE' },
    { token: 'namespace',  foreground: '4EC5D4' },
  ],
  colors: {
    'editor.background': '#1e1e1e',
  },
  semanticHighlighting: true,
});

const ASL_SAMPLE = `name,sps,:eq,:sum,
name,sps,:eq,errors,:sum,
:div,
100,:mul`;

const URI_SAMPLE = `/api/v1/graph?q=name,sps,:eq,:sum&s=e-3h&w=800&h=300`;

// Mode from URL query parameter
const params = new URLSearchParams(window.location.search);
const currentMode = params.get('mode') === 'uri' ? 'uri' : 'asl';

// Editor options — URI mode uses word wrap for visual line breaking
const uriOptions = currentMode === 'uri' ? {
  wordWrap: 'on',
  wordWrapBreakBeforeCharacters: '?&',
  wrappingIndent: 'indent',
} : {};

// Create editor
const editor = monaco.editor.create(document.getElementById('editor'), {
  value: currentMode === 'uri' ? URI_SAMPLE : ASL_SAMPLE,
  language: 'atlas',
  theme: 'atlas-dark',
  'semanticHighlighting.enabled': true,
  fontSize: 14,
  minimap: { enabled: false },
  scrollBeyondLastLine: false,
  renderLineHighlight: 'none',
  padding: { top: 8, bottom: 8 },
  automaticLayout: true,
  fixedOverflowWidgets: true,
  wordBasedSuggestions: 'off',
  ...uriOptions,
});

// Connect to LSP server
const dot = document.getElementById('status-dot');
const statusText = document.getElementById('status-text');

function connect() {
  const wsPath = currentMode === 'uri' ? '/uri' : '/asl';
  const ws = new WebSocket(`ws://localhost:7102${wsPath}`);
  ws.onopen = () => {
    const transport = monaco.lsp.WebSocketTransport.fromWebSocket(ws);
    new monaco.lsp.MonacoLspClient(transport);
    dot.className = 'dot connected';
    statusText.textContent = `LSP (${currentMode.toUpperCase()}): connected`;
  };
  ws.onerror = () => {
    dot.className = 'dot disconnected';
    statusText.textContent = 'LSP: connection failed';
  };
  ws.onclose = () => {
    dot.className = 'dot disconnected';
    statusText.textContent = 'LSP: disconnected \u2014 reconnecting...';
    setTimeout(connect, 3000);
  };
}

// Mode switch reloads the page
function switchMode(mode) {
  if (mode === currentMode) return;
  window.location.search = `?mode=${mode}`;
}
window.switchMode = switchMode;

// Copy compact URI to clipboard with percent-encoding for URI safety
function copyCompactUri() {
  const text = editor.getValue();
  // Split into path and query, then encode each param value
  const qIdx = text.indexOf('?');
  if (qIdx < 0) {
    navigator.clipboard.writeText(text);
    return;
  }
  const path = text.substring(0, qIdx);
  const query = text.substring(qIdx + 1);
  const encoded = query.split('&').map(param => {
    const eqIdx = param.indexOf('=');
    if (eqIdx < 0) return param;
    const key = param.substring(0, eqIdx);
    const val = param.substring(eqIdx + 1);
    // Decode first to avoid double-encoding (e.g. %20 → %2520), then re-encode.
    // Keep commas and colons (ASL syntax) unencoded for readability.
    const decoded = decodeURIComponent(val);
    const safe = encodeURIComponent(decoded).replace(/%2C/gi, ',').replace(/%3A/gi, ':');
    return key + '=' + safe;
  }).join('&');
  navigator.clipboard.writeText(path + '?' + encoded);
}
window.copyCompactUri = copyCompactUri;

// Set active button
document.getElementById('btn-asl').classList.toggle('active', currentMode === 'asl');
document.getElementById('btn-uri').classList.toggle('active', currentMode === 'uri');

// Show Copy URI button in URI mode
if (currentMode === 'uri') {
  document.getElementById('btn-copy').style.display = '';
}

connect();

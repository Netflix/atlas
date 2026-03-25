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
package com.netflix.atlas.core.uri

/** A span in the original text: [start, end) with the text content. */
case class UriSpan(start: Int, end: Int, text: String)

/** A single query parameter with positional spans. */
case class QueryParam(
  key: UriSpan,
  value: UriSpan,
  decodedValue: String
)

/**
  * A parsed URI with span information for LSP features.
  *
  * For a full URI like `https://atlas.example.com:7101/api/v1/graph?q=...`:
  *   - `scheme` = `https`
  *   - `host`   = `atlas.example.com`
  *   - `port`   = `7101`
  *   - `path`   = `/api/v1/graph`
  *
  * For a path-only URI like `/api/v1/graph?q=...`, scheme/host/port
  * are `None` and `path` covers everything before `?`.
  */
case class ParsedUri(
  scheme: Option[UriSpan],
  host: Option[UriSpan],
  port: Option[UriSpan],
  path: UriSpan,
  query: List[QueryParam]
)

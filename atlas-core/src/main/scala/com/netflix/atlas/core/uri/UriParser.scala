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

/**
  * Hand-written single-pass URI parser that preserves position spans. Unlike
  * `split("&")`, this correctly handles percent-encoded `&` (`%26`) inside
  * query values so that operators like `:each` are not broken up.
  */
object UriParser {

  /** Parse a URI string into a [[ParsedUri]] with position-preserving spans. */
  def parse(text: String): ParsedUri = {
    val qIdx = text.indexOf('?')
    val prefixEnd = if (qIdx < 0) text.length else qIdx

    val (scheme, authorityStart) = parseScheme(text, prefixEnd)
    val (authority, port, pathStart) = parseAuthority(text, authorityStart, prefixEnd)
    val path = UriSpan(pathStart, prefixEnd, text.substring(pathStart, prefixEnd))

    val params = if (qIdx < 0) Nil else parseQuery(text, qIdx + 1)
    ParsedUri(scheme, authority, port, path, params)
  }

  /**
    * Extract scheme (e.g. `http`, `https`) if the prefix starts with `scheme://`.
    * Returns the scheme span and the position after `://`.
    */
  private def parseScheme(text: String, prefixEnd: Int): (Option[UriSpan], Int) = {
    val colonSlashSlash = text.indexOf("://")
    if (colonSlashSlash < 0 || colonSlashSlash >= prefixEnd) {
      return (None, 0)
    }
    val s = text.substring(0, colonSlashSlash)
    (Some(UriSpan(0, colonSlashSlash, s)), colonSlashSlash + 3)
  }

  /**
    * Extract host and optional port from the text starting at `start` up to
    * `prefixEnd`. The authority section ends at the first `/` or at
    * `prefixEnd` if there is no path.
    */
  private def parseAuthority(
    text: String,
    start: Int,
    prefixEnd: Int
  ): (Option[UriSpan], Option[UriSpan], Int) = {
    if (start == 0) {
      // No scheme, so no authority
      return (None, None, 0)
    }
    val slashIdx = text.indexOf('/', start)
    val authEnd = if (slashIdx < 0 || slashIdx >= prefixEnd) prefixEnd else slashIdx
    val authText = text.substring(start, authEnd)

    // Split host and port on last colon (e.g. "host:7101")
    val colonIdx = authText.lastIndexOf(':')
    if (colonIdx >= 0) {
      val portStr = authText.substring(colonIdx + 1)
      if (portStr.nonEmpty && portStr.forall(_.isDigit)) {
        val hostText = authText.substring(0, colonIdx)
        val host = Some(UriSpan(start, start + colonIdx, hostText))
        val portStart = start + colonIdx + 1
        val port = Some(UriSpan(portStart, authEnd, portStr))
        return (host, port, authEnd)
      }
    }

    // No port — the entire authority is the host
    val host = Some(UriSpan(start, authEnd, authText))
    (host, None, authEnd)
  }

  private def parseQuery(text: String, start: Int): List[QueryParam] = {
    val params = List.newBuilder[QueryParam]
    val len = text.length
    var pos = start
    var trailingAmp = false

    while (pos < len) {
      // Find the end of this parameter (next unencoded &)
      val paramStart = pos
      var paramEnd = paramStart
      while (paramEnd < len && !isUnencAmpersand(text, paramEnd)) {
        paramEnd = advance(text, paramEnd)
      }

      params += parseParam(text, paramStart, paramEnd)

      // Skip the '&'
      trailingAmp = paramEnd < len
      pos = if (trailingAmp) paramEnd + 1 else paramEnd
    }

    // Trailing '&' at end of string produces an empty param
    if (trailingAmp) {
      params += parseParam(text, len, len)
    }

    params.result()
  }

  /** Check if position is an unencoded `&` (not part of `%26`). */
  private def isUnencAmpersand(text: String, pos: Int): Boolean = {
    text.charAt(pos) == '&'
  }

  /** Advance past current character, skipping `%XX` as a unit. */
  private def advance(text: String, pos: Int): Int = {
    if (text.charAt(pos) == '%' && pos + 2 < text.length) pos + 3
    else pos + 1
  }

  private def parseParam(text: String, start: Int, end: Int): QueryParam = {
    // Find first unencoded '=' in [start, end)
    var eqPos = -1
    var i = start
    while (i < end && eqPos < 0) {
      val ch = text.charAt(i)
      if (ch == '=') eqPos = i
      else i = advance(text, i)
    }

    if (eqPos < 0) {
      // Bare key, no value
      val keyText = text.substring(start, end)
      QueryParam(
        UriSpan(start, end, keyText),
        UriSpan(end, end, ""),
        ""
      )
    } else {
      val keyText = text.substring(start, eqPos)
      val valStart = eqPos + 1
      val rawValue = text.substring(valStart, end)
      QueryParam(
        UriSpan(start, eqPos, keyText),
        UriSpan(valStart, end, rawValue),
        percentDecode(rawValue)
      )
    }
  }

  /** Percent-decode a string, also converting `+` to space. Handles multi-byte UTF-8. */
  def percentDecode(s: String): String = {
    val len = s.length
    var i = 0
    var needsDecode = false
    while (i < len && !needsDecode) {
      val ch = s.charAt(i)
      if (ch == '%' || ch == '+') needsDecode = true
      i += 1
    }
    if (!needsDecode) return s

    val sb = new StringBuilder(len)
    val buf = new Array[Byte](4) // max 4 bytes per UTF-8 codepoint
    var bufLen = 0
    i = 0
    while (i < len) {
      val ch = s.charAt(i)
      if (ch == '%' && i + 2 < len) {
        val hi = hexVal(s.charAt(i + 1))
        val lo = hexVal(s.charAt(i + 2))
        if (hi >= 0 && lo >= 0) {
          buf(bufLen) = ((hi << 4) | lo).toByte
          bufLen += 1
          i += 3
          // Flush when we have a complete UTF-8 sequence
          if (bufLen >= utf8SeqLen(buf(0))) {
            sb.append(new String(buf, 0, bufLen, java.nio.charset.StandardCharsets.UTF_8))
            bufLen = 0
          }
        } else {
          bufLen = flushBytes(buf, bufLen, sb)
          sb.append(ch)
          i += 1
        }
      } else {
        bufLen = flushBytes(buf, bufLen, sb)
        if (ch == '+') {
          sb.append(' ')
        } else {
          sb.append(ch)
        }
        i += 1
      }
    }
    flushBytes(buf, bufLen, sb)
    sb.toString
  }

  private def flushBytes(buf: Array[Byte], bufLen: Int, sb: StringBuilder): Int = {
    if (bufLen > 0) {
      sb.append(new String(buf, 0, bufLen, java.nio.charset.StandardCharsets.UTF_8))
    }
    0
  }

  /** Expected byte count for a UTF-8 sequence based on the leading byte. */
  private def utf8SeqLen(b: Byte): Int = {
    if ((b & 0x80) == 0) 1
    else if ((b & 0xE0) == 0xC0) 2
    else if ((b & 0xF0) == 0xE0) 3
    else if ((b & 0xF8) == 0xF0) 4
    else 1 // invalid leading byte, flush as single byte
  }

  private def hexVal(c: Char): Int = {
    if (c >= '0' && c <= '9') c - '0'
    else if (c >= 'a' && c <= 'f') c - 'a' + 10
    else if (c >= 'A' && c <= 'F') c - 'A' + 10
    else -1
  }

  /**
    * Build a mapping from decoded-string offset to raw-string offset.
    * For percent-encoded sequences (`%XX`), one decoded char maps to the
    * position of the `%` in the raw string. For `+` (space), one decoded
    * char maps to the `+` position. Returns an array where
    * `result(decodedOffset)` gives the corresponding raw offset.
    */
  def buildOffsetMap(raw: String): Array[Int] = {
    val len = raw.length
    // Upper bound: decoded length <= raw length
    val map = new Array[Int](len + 1)
    var ri = 0 // raw index
    var di = 0 // decoded index
    while (ri < len) {
      map(di) = ri
      val ch = raw.charAt(ri)
      if (
        ch == '%' && ri + 2 < len && hexVal(raw.charAt(ri + 1)) >= 0 && hexVal(
          raw.charAt(ri + 2)
        ) >= 0
      ) {
        ri += 3
      } else {
        ri += 1
      }
      di += 1
    }
    // Sentinel: end position
    map(di) = ri
    java.util.Arrays.copyOf(map, di + 1)
  }
}

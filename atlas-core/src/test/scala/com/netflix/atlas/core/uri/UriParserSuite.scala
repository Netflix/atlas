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

import munit.FunSuite

class UriParserSuite extends FunSuite {

  test("basic: path and two params") {
    val text = "/api/v1/graph?q=name,sps,:eq,:sum&s=e-3h"
    val parsed = UriParser.parse(text)
    assertEquals(parsed.path, UriSpan(0, 13, "/api/v1/graph"))
    assertEquals(parsed.query.size, 2)

    val q = parsed.query(0)
    assertEquals(q.key, UriSpan(14, 15, "q"))
    assertEquals(q.value, UriSpan(16, 33, "name,sps,:eq,:sum"))
    assertEquals(q.decodedValue, "name,sps,:eq,:sum")

    val s = parsed.query(1)
    assertEquals(s.key, UriSpan(34, 35, "s"))
    assertEquals(s.value, UriSpan(36, 40, "e-3h"))
    assertEquals(s.decodedValue, "e-3h")
  }

  test("percent-encoded & in value") {
    val text = "/api?q=a%26b"
    val parsed = UriParser.parse(text)
    assertEquals(parsed.query.size, 1)
    val q = parsed.query(0)
    assertEquals(q.value, UriSpan(7, 12, "a%26b"))
    assertEquals(q.decodedValue, "a&b")
  }

  test("percent-encoded = and ,") {
    val text = "/api?q=%3D%2C"
    val parsed = UriParser.parse(text)
    val q = parsed.query(0)
    assertEquals(q.decodedValue, "=,")
    assertEquals(q.value.text, "%3D%2C")
  }

  test("pipe in regex") {
    val text = "/api?q=name,a|b,:re"
    val parsed = UriParser.parse(text)
    assertEquals(parsed.query.size, 1)
    assertEquals(parsed.query(0).decodedValue, "name,a|b,:re")
  }

  test("path only, no query string") {
    val parsed = UriParser.parse("/api/v1/graph")
    assertEquals(parsed.path, UriSpan(0, 13, "/api/v1/graph"))
    assertEquals(parsed.query, Nil)
  }

  test("empty value") {
    val text = "/api?q=&s=e-3h"
    val parsed = UriParser.parse(text)
    assertEquals(parsed.query.size, 2)
    assertEquals(parsed.query(0).key.text, "q")
    assertEquals(parsed.query(0).value.text, "")
    assertEquals(parsed.query(0).decodedValue, "")
    assertEquals(parsed.query(1).key.text, "s")
    assertEquals(parsed.query(1).decodedValue, "e-3h")
  }

  test("bare key with no =") {
    val text = "/api?format"
    val parsed = UriParser.parse(text)
    assertEquals(parsed.query.size, 1)
    val p = parsed.query(0)
    assertEquals(p.key.text, "format")
    assertEquals(p.value.text, "")
    assertEquals(p.decodedValue, "")
  }

  test("multiple same-key params") {
    val text = "/api?tz=UTC&tz=US/Pacific"
    val parsed = UriParser.parse(text)
    assertEquals(parsed.query.size, 2)
    assertEquals(parsed.query(0).decodedValue, "UTC")
    assertEquals(parsed.query(1).decodedValue, "US/Pacific")
  }

  test("+ as space") {
    val text = "/api?title=Starts+per+second"
    val parsed = UriParser.parse(text)
    assertEquals(parsed.query(0).decodedValue, "Starts per second")
  }

  test("each expression with real & separator") {
    // The key scenario: :each body uses encoded & for real separators
    val text =
      "/api/v1/graph?q=name,a,:eq,:sum,:list,(,nf.app,foo,:eq,:cq,),:each&s=e-3h"
    val parsed = UriParser.parse(text)
    assertEquals(parsed.query.size, 2)
    assertEquals(parsed.query(0).key.text, "q")
    // The value should NOT include &s=e-3h
    assert(parsed.query(0).decodedValue.endsWith(",:each"))
    assertEquals(parsed.query(1).key.text, "s")
    assertEquals(parsed.query(1).decodedValue, "e-3h")
  }

  //
  // Scheme, authority, port
  //

  test("full URI: scheme, host, port, path") {
    val text = "https://atlas.example.com:7101/api/v1/graph?q=:true"
    val parsed = UriParser.parse(text)

    val scheme = parsed.scheme.get
    assertEquals(scheme.text, "https")
    assertEquals(scheme.start, 0)
    assertEquals(scheme.end, 5)

    val host = parsed.host.get
    assertEquals(host.text, "atlas.example.com")
    assertEquals(host.start, 8)
    assertEquals(host.end, 25)

    val port = parsed.port.get
    assertEquals(port.text, "7101")
    assertEquals(port.start, 26)
    assertEquals(port.end, 30)

    assertEquals(parsed.path.text, "/api/v1/graph")
    assertEquals(parsed.path.start, 30)
    assertEquals(parsed.path.end, 43)

    assertEquals(parsed.query.size, 1)
    assertEquals(parsed.query(0).key.text, "q")
  }

  test("full URI: no port") {
    val text = "https://atlas.example.com/api/v1/graph?q=:true"
    val parsed = UriParser.parse(text)
    assertEquals(parsed.scheme.get.text, "https")
    assertEquals(parsed.host.get.text, "atlas.example.com")
    assertEquals(parsed.port, None)
    assertEquals(parsed.path.text, "/api/v1/graph")
  }

  test("full URI: http scheme") {
    val text = "http://localhost:7101/api/v1/graph?q=:true"
    val parsed = UriParser.parse(text)
    assertEquals(parsed.scheme.get.text, "http")
    assertEquals(parsed.host.get.text, "localhost")
    assertEquals(parsed.port.get.text, "7101")
    assertEquals(parsed.path.text, "/api/v1/graph")
  }

  test("full URI: no path after authority") {
    val text = "https://atlas.example.com:7101?q=:true"
    val parsed = UriParser.parse(text)
    assertEquals(parsed.scheme.get.text, "https")
    assertEquals(parsed.host.get.text, "atlas.example.com")
    assertEquals(parsed.port.get.text, "7101")
    assertEquals(parsed.path.text, "")
    assertEquals(parsed.query.size, 1)
  }

  test("path-only URI: no scheme, host, or port") {
    val parsed = UriParser.parse("/api/v1/graph?q=:true")
    assertEquals(parsed.scheme, None)
    assertEquals(parsed.host, None)
    assertEquals(parsed.port, None)
    assertEquals(parsed.path.text, "/api/v1/graph")
  }

  //
  // percentDecode
  //

  test("percentDecode: no encoding") {
    assertEquals(UriParser.percentDecode("hello"), "hello")
  }

  test("percentDecode: %26 -> &") {
    assertEquals(UriParser.percentDecode("a%26b"), "a&b")
  }

  test("percentDecode: invalid hex left as-is") {
    assertEquals(UriParser.percentDecode("a%ZZb"), "a%ZZb")
  }

  test("percentDecode: trailing % left as-is") {
    assertEquals(UriParser.percentDecode("a%"), "a%")
  }

  test("percentDecode: multi-byte UTF-8 em-dash") {
    // %E2%80%94 is the UTF-8 encoding of U+2014 EM DASH (—)
    assertEquals(UriParser.percentDecode("a%E2%80%94b"), "a\u2014b")
  }

  test("percentDecode: 2-byte UTF-8") {
    // %C3%A9 is the UTF-8 encoding of U+00E9 (é)
    assertEquals(UriParser.percentDecode("%C3%A9"), "\u00e9")
  }

  //
  // buildOffsetMap
  //

  test("buildOffsetMap: no encoding") {
    val map = UriParser.buildOffsetMap("abc")
    assertEquals(map.toList, List(0, 1, 2, 3))
  }

  test("buildOffsetMap: %26 maps one decoded char to three raw chars") {
    val map = UriParser.buildOffsetMap("a%26b")
    // decoded: "a&b" (3 chars)
    // map(0)=0 (a), map(1)=1 (%26), map(2)=4 (b), map(3)=5 (sentinel)
    assertEquals(map.toList, List(0, 1, 4, 5))
  }

  test("buildOffsetMap: + maps 1:1") {
    val map = UriParser.buildOffsetMap("a+b")
    assertEquals(map.toList, List(0, 1, 2, 3))
  }

  test("buildOffsetMap: multiple encodings") {
    // raw: "%3D%2C" -> decoded: "=,"
    val map = UriParser.buildOffsetMap("%3D%2C")
    // map(0)=0 (%3D), map(1)=3 (%2C), map(2)=6 (sentinel)
    assertEquals(map.toList, List(0, 3, 6))
  }
}

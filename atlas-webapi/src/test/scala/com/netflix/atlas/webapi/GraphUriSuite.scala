/*
 * Copyright 2014-2017 Netflix, Inc.
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
package com.netflix.atlas.webapi

import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.StyleExpr
import org.scalatest.FunSuite
import spray.http.HttpMethods
import spray.http.HttpRequest

class GraphUriSuite extends FunSuite {

  private def httpReq(uri: String): GraphApi.Request = {
    GraphApi.toRequest(HttpRequest(HttpMethods.GET, uri))
  }

  test("simple expr") {
    val req = httpReq("/api/v1/graph?q=name,foo,:eq,:sum")
    assert(req.exprs === List(StyleExpr(DataExpr.Sum(Query.Equal("name", "foo")), Map.empty)))
  }

  test("empty title") {
    val req = httpReq("/api/v1/graph?q=name,foo,:eq,:sum&title=")
    assert(req.flags.title === None)
  }

  test("with title") {
    val req = httpReq("/api/v1/graph?q=name,foo,:eq,:sum&title=foo")
    assert(req.flags.title === Some("foo"))
  }

  test("empty ylabel") {
    val req = httpReq("/api/v1/graph?q=name,foo,:eq,:sum&ylabel=")
    assert(req.flags.axes(0).ylabel === None)
  }

  test("with ylabel") {
    val req = httpReq("/api/v1/graph?q=name,foo,:eq,:sum&ylabel=foo")
    assert(req.flags.axes(0).ylabel === Some("foo"))
  }

  test("empty ylabel.1") {
    val req = httpReq("/api/v1/graph?q=name,foo,:eq,:sum&ylabel.1=")
    assert(req.flags.axes(1).ylabel === None)
  }

  test("empty ylabel.1 with ylabel") {
    val req = httpReq("/api/v1/graph?q=name,foo,:eq,:sum&ylabel.1=&ylabel=foo")
    assert(req.flags.axes(1).ylabel === None)
  }
}


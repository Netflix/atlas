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
package com.netflix.atlas.lwc.events

import com.netflix.atlas.core.model.Query
import munit.FunSuite

class TypedLwcEventFilterSuite extends FunSuite {

  private def parseQuery(str: String): Query = {
    ExprUtils.parseDataExpr(str).query
  }

  test("splitQuery: simple string query") {
    val filter = TypedLwcEventFilter(Map("duration" -> TypedLwcEventFilter.DurationMatcher))
    val query = parseQuery("name,foo,:eq")
    val queries = filter.splitQuery(query)
    assertEquals(queries.indexQuery, query)
    assertEquals(queries.postFilterQuery, Query.True)
  }

  test("splitQuery: simple duration query") {
    val filter = TypedLwcEventFilter(Map("duration" -> TypedLwcEventFilter.DurationMatcher))
    val query = parseQuery("duration,42ms,:eq")
    val queries = filter.splitQuery(query)
    assertEquals(queries.indexQuery, Query.True)
    assertEquals(queries.postFilterQuery, query)
  }

  test("splitQuery: AND both") {
    val filter = TypedLwcEventFilter(Map("duration" -> TypedLwcEventFilter.DurationMatcher))
    val query = parseQuery("name,foo,:eq,duration,42ms,:gt,:and")
    val queries = filter.splitQuery(query)
    assertEquals(queries.indexQuery, parseQuery("name,foo,:eq"))
    assertEquals(queries.postFilterQuery, parseQuery("duration,42ms,:gt"))
  }

  test("splitQuery: OR both") {
    val filter = TypedLwcEventFilter(Map("duration" -> TypedLwcEventFilter.DurationMatcher))
    val query = parseQuery("name,foo,:eq,duration,42ms,:gt,:or")
    val queries = filter.splitQuery(query)
    assertEquals(queries.indexQuery, Query.True)
    assertEquals(queries.postFilterQuery, query)
  }

  test("splitQuery: AND/OR both") {
    val filter = TypedLwcEventFilter(Map("duration" -> TypedLwcEventFilter.DurationMatcher))
    val query = parseQuery("app,www,:eq,name,foo,:eq,duration,42ms,:gt,:or,:and")
    val queries = filter.splitQuery(query)
    assertEquals(queries.indexQuery, parseQuery("app,www,:eq"))
    assertEquals(queries.postFilterQuery, parseQuery("name,foo,:eq,duration,42ms,:gt,:or"))
  }

  test("splitQuery: NOT both") {
    val filter = TypedLwcEventFilter(Map("duration" -> TypedLwcEventFilter.DurationMatcher))
    val query = parseQuery("name,foo,:eq,duration,42ms,:gt,:and,:not")
    val queries = filter.splitQuery(query)
    assertEquals(queries.indexQuery, parseQuery("name,foo,:eq,:not"))
    assertEquals(queries.postFilterQuery, query)
  }

  test("duration: equal") {
    val filter = TypedLwcEventFilter(Map("duration" -> TypedLwcEventFilter.DurationMatcher))
    val event = LwcEvent(Map("duration" -> 42_000_000L))
    assert(filter.matches(event, parseQuery("duration,42ms,:eq")))
    assert(filter.matches(event, parseQuery("duration,PT0.042S,:eq")))
    assert(!filter.matches(event, parseQuery("duration,41ms,:eq")))
  }

  test("duration: less than") {
    val filter = TypedLwcEventFilter(Map("duration" -> TypedLwcEventFilter.DurationMatcher))
    val event = LwcEvent(Map("duration" -> 42_000_000L))
    assert(filter.matches(event, parseQuery("duration,50ms,:lt")))
    assert(!filter.matches(event, parseQuery("duration,42ms,:lt")))
    assert(!filter.matches(event, parseQuery("duration,30ms,:lt")))
  }

  test("duration: less than or equal") {
    val filter = TypedLwcEventFilter(Map("duration" -> TypedLwcEventFilter.DurationMatcher))
    val event = LwcEvent(Map("duration" -> 42_000_000L))
    assert(filter.matches(event, parseQuery("duration,50ms,:le")))
    assert(filter.matches(event, parseQuery("duration,42ms,:le")))
    assert(!filter.matches(event, parseQuery("duration,30ms,:le")))
  }

  test("duration: greater than") {
    val filter = TypedLwcEventFilter(Map("duration" -> TypedLwcEventFilter.DurationMatcher))
    val event = LwcEvent(Map("duration" -> 42_000_000L))
    assert(filter.matches(event, parseQuery("duration,30ms,:gt")))
    assert(!filter.matches(event, parseQuery("duration,42ms,:gt")))
    assert(!filter.matches(event, parseQuery("duration,50ms,:gt")))
  }

  test("duration: greater than or equal") {
    val filter = TypedLwcEventFilter(Map("duration" -> TypedLwcEventFilter.DurationMatcher))
    val event = LwcEvent(Map("duration" -> 42_000_000L))
    assert(filter.matches(event, parseQuery("duration,30ms,:ge")))
    assert(filter.matches(event, parseQuery("duration,42ms,:ge")))
    assert(!filter.matches(event, parseQuery("duration,50ms,:ge")))
  }

  test("duration: in") {
    val filter = TypedLwcEventFilter(Map("duration" -> TypedLwcEventFilter.DurationMatcher))
    val event = LwcEvent(Map("duration" -> 42_000_000L))
    assert(filter.matches(event, parseQuery("duration,(,10ms,42ms,100ms,),:in")))
    assert(filter.matches(event, parseQuery("duration,(,PT0.042S,),:in")))
    assert(!filter.matches(event, parseQuery("duration,(,10ms,100ms,),:in")))
  }

  test("duration: in string") {
    val filter = TypedLwcEventFilter(Map("duration" -> TypedLwcEventFilter.DurationMatcher))
    val event = LwcEvent(Map("app" -> "www", "duration" -> 42_000_000L))
    assert(filter.matches(event, parseQuery("app,(,www,foo,),:in")))
    assert(!filter.matches(event, parseQuery("app,(,www2,foo,),:in")))
  }

  test("duration: regex") {
    val filter = TypedLwcEventFilter(Map("duration" -> TypedLwcEventFilter.DurationMatcher))
    val event = LwcEvent(Map("duration" -> 42_000_000L))
    assert(!filter.matches(event, parseQuery("duration,42ms,:re")))
    assert(!filter.matches(event, parseQuery("duration,42ms,:reic")))
  }

  test("duration: or") {
    val filter = TypedLwcEventFilter(Map("duration" -> TypedLwcEventFilter.DurationMatcher))
    val event = LwcEvent(Map("app" -> "www", "duration" -> 42_000_000L))
    assert(filter.matches(event, parseQuery("app,www,:eq,duration,42ms,:eq,:or")))
    assert(filter.matches(event, parseQuery("app,www,:eq,duration,43ms,:eq,:or")))
    assert(filter.matches(event, parseQuery("app,foo,:eq,duration,42ms,:eq,:or")))
    assert(!filter.matches(event, parseQuery("app,foo,:eq,duration,43ms,:eq,:or")))
  }

  test("duration: and, or, not") {
    val filter = TypedLwcEventFilter(Map("duration" -> TypedLwcEventFilter.DurationMatcher))
    val event = LwcEvent(Map("app" -> "www", "duration" -> 42_000_000L))
    assert(
      filter.matches(event, parseQuery("app,www,:eq,app,foo,:eq,:not,:and,duration,42ms,:eq,:or"))
    )
    assert(
      !filter.matches(event, parseQuery("app,www,:re,app,www,:eq,:not,:and,duration,43ms,:eq,:or"))
    )
  }

  test("duration: cast from Int") {
    val filter = TypedLwcEventFilter(Map("duration" -> TypedLwcEventFilter.DurationMatcher))
    val event = LwcEvent(Map("duration" -> 42_000_000))
    assert(filter.matches(event, parseQuery("duration,42ms,:eq")))
  }

  test("duration: cast from String") {
    val filter = TypedLwcEventFilter(Map("duration" -> TypedLwcEventFilter.DurationMatcher))
    val event = LwcEvent(Map("duration" -> "42ms"))
    assert(filter.matches(event, parseQuery("duration,42ms,:eq")))
  }

  test("long: equal") {
    val filter = TypedLwcEventFilter(Map("count" -> TypedLwcEventFilter.LongMatcher))
    val event = LwcEvent(Map("count" -> 42L))
    assert(filter.matches(event, parseQuery("count,42,:eq")))
    assert(!filter.matches(event, parseQuery("count,43,:eq")))
  }

  test("long: less than") {
    val filter = TypedLwcEventFilter(Map("count" -> TypedLwcEventFilter.LongMatcher))
    val event = LwcEvent(Map("count" -> 42L))
    assert(filter.matches(event, parseQuery("count,50,:lt")))
    assert(!filter.matches(event, parseQuery("count,42,:lt")))
    assert(!filter.matches(event, parseQuery("count,30,:lt")))
  }

  test("long: less than or equal") {
    val filter = TypedLwcEventFilter(Map("count" -> TypedLwcEventFilter.LongMatcher))
    val event = LwcEvent(Map("count" -> 42L))
    assert(filter.matches(event, parseQuery("count,50,:le")))
    assert(filter.matches(event, parseQuery("count,42,:le")))
    assert(!filter.matches(event, parseQuery("count,30,:le")))
  }

  test("long: greater than") {
    val filter = TypedLwcEventFilter(Map("count" -> TypedLwcEventFilter.LongMatcher))
    val event = LwcEvent(Map("count" -> 42L))
    assert(filter.matches(event, parseQuery("count,30,:gt")))
    assert(!filter.matches(event, parseQuery("count,42,:gt")))
    assert(!filter.matches(event, parseQuery("count,50,:gt")))
  }

  test("long: greater than or equal") {
    val filter = TypedLwcEventFilter(Map("count" -> TypedLwcEventFilter.LongMatcher))
    val event = LwcEvent(Map("count" -> 42L))
    assert(filter.matches(event, parseQuery("count,30,:ge")))
    assert(filter.matches(event, parseQuery("count,42,:ge")))
    assert(!filter.matches(event, parseQuery("count,50,:ge")))
  }

  test("long: in") {
    val filter = TypedLwcEventFilter(Map("count" -> TypedLwcEventFilter.LongMatcher))
    val event = LwcEvent(Map("count" -> 42L))
    assert(filter.matches(event, parseQuery("count,(,10,42,100,),:in")))
    assert(!filter.matches(event, parseQuery("count,(,10,100,),:in")))
  }

  test("long: cast from Int") {
    val filter = TypedLwcEventFilter(Map("count" -> TypedLwcEventFilter.LongMatcher))
    val event = LwcEvent(Map("count" -> 42))
    assert(filter.matches(event, parseQuery("count,42,:eq")))
  }

  test("long: cast from String") {
    val filter = TypedLwcEventFilter(Map("count" -> TypedLwcEventFilter.LongMatcher))
    val event = LwcEvent(Map("count" -> "42"))
    assert(filter.matches(event, parseQuery("count,42,:eq")))
  }

  test("long: cast from Short") {
    val filter = TypedLwcEventFilter(Map("count" -> TypedLwcEventFilter.LongMatcher))
    val event = LwcEvent(Map("count" -> 42.toShort))
    assert(filter.matches(event, parseQuery("count,42,:eq")))
  }

  test("long: cast from Byte") {
    val filter = TypedLwcEventFilter(Map("count" -> TypedLwcEventFilter.LongMatcher))
    val event = LwcEvent(Map("count" -> 42.toByte))
    assert(filter.matches(event, parseQuery("count,42,:eq")))
  }

  test("double: equal") {
    val filter = TypedLwcEventFilter(Map("value" -> TypedLwcEventFilter.DoubleMatcher))
    val event = LwcEvent(Map("value" -> 42.5))
    assert(filter.matches(event, parseQuery("value,42.5,:eq")))
    assert(!filter.matches(event, parseQuery("value,42.6,:eq")))
  }

  test("double: less than") {
    val filter = TypedLwcEventFilter(Map("value" -> TypedLwcEventFilter.DoubleMatcher))
    val event = LwcEvent(Map("value" -> 42.5))
    assert(filter.matches(event, parseQuery("value,50.0,:lt")))
    assert(!filter.matches(event, parseQuery("value,42.5,:lt")))
    assert(!filter.matches(event, parseQuery("value,30.0,:lt")))
  }

  test("double: less than or equal") {
    val filter = TypedLwcEventFilter(Map("value" -> TypedLwcEventFilter.DoubleMatcher))
    val event = LwcEvent(Map("value" -> 42.5))
    assert(filter.matches(event, parseQuery("value,50.0,:le")))
    assert(filter.matches(event, parseQuery("value,42.5,:le")))
    assert(!filter.matches(event, parseQuery("value,30.0,:le")))
  }

  test("double: greater than") {
    val filter = TypedLwcEventFilter(Map("value" -> TypedLwcEventFilter.DoubleMatcher))
    val event = LwcEvent(Map("value" -> 42.5))
    assert(filter.matches(event, parseQuery("value,30.0,:gt")))
    assert(!filter.matches(event, parseQuery("value,42.5,:gt")))
    assert(!filter.matches(event, parseQuery("value,50.0,:gt")))
  }

  test("double: greater than or equal") {
    val filter = TypedLwcEventFilter(Map("value" -> TypedLwcEventFilter.DoubleMatcher))
    val event = LwcEvent(Map("value" -> 42.5))
    assert(filter.matches(event, parseQuery("value,30.0,:ge")))
    assert(filter.matches(event, parseQuery("value,42.5,:ge")))
    assert(!filter.matches(event, parseQuery("value,50.0,:ge")))
  }

  test("double: in") {
    val filter = TypedLwcEventFilter(Map("value" -> TypedLwcEventFilter.DoubleMatcher))
    val event = LwcEvent(Map("value" -> 42.5))
    assert(filter.matches(event, parseQuery("value,(,10.0,42.5,100.0,),:in")))
    assert(!filter.matches(event, parseQuery("value,(,10.0,100.0,),:in")))
  }

  test("double: cast from Int") {
    val filter = TypedLwcEventFilter(Map("value" -> TypedLwcEventFilter.DoubleMatcher))
    val event = LwcEvent(Map("value" -> 42))
    assert(filter.matches(event, parseQuery("value,42.0,:eq")))
  }

  test("double: cast from Long") {
    val filter = TypedLwcEventFilter(Map("value" -> TypedLwcEventFilter.DoubleMatcher))
    val event = LwcEvent(Map("value" -> 42L))
    assert(filter.matches(event, parseQuery("value,42.0,:eq")))
  }

  test("double: cast from Float") {
    val filter = TypedLwcEventFilter(Map("value" -> TypedLwcEventFilter.DoubleMatcher))
    val event = LwcEvent(Map("value" -> 42.5f))
    assert(filter.matches(event, parseQuery("value,42.5,:eq")))
  }

  test("double: cast from String") {
    val filter = TypedLwcEventFilter(Map("value" -> TypedLwcEventFilter.DoubleMatcher))
    val event = LwcEvent(Map("value" -> "42.5"))
    assert(filter.matches(event, parseQuery("value,42.5,:eq")))
  }

  test("instant: equal") {
    val filter = TypedLwcEventFilter(Map("timestamp" -> TypedLwcEventFilter.InstantMatcher))
    val event = LwcEvent(Map("timestamp" -> 1609459200000L)) // 2021-01-01T00:00:00Z
    assert(filter.matches(event, parseQuery("timestamp,2021-01-01T00:00:00Z,:eq")))
    assert(!filter.matches(event, parseQuery("timestamp,2021-01-02T00:00:00Z,:eq")))
  }

  test("instant: less than") {
    val filter = TypedLwcEventFilter(Map("timestamp" -> TypedLwcEventFilter.InstantMatcher))
    val event = LwcEvent(Map("timestamp" -> 1609459200000L))
    assert(filter.matches(event, parseQuery("timestamp,2021-01-02T00:00:00Z,:lt")))
    assert(!filter.matches(event, parseQuery("timestamp,2021-01-01T00:00:00Z,:lt")))
    assert(!filter.matches(event, parseQuery("timestamp,2020-12-31T00:00:00Z,:lt")))
  }

  test("instant: less than or equal") {
    val filter = TypedLwcEventFilter(Map("timestamp" -> TypedLwcEventFilter.InstantMatcher))
    val event = LwcEvent(Map("timestamp" -> 1609459200000L))
    assert(filter.matches(event, parseQuery("timestamp,2021-01-02T00:00:00Z,:le")))
    assert(filter.matches(event, parseQuery("timestamp,2021-01-01T00:00:00Z,:le")))
    assert(!filter.matches(event, parseQuery("timestamp,2020-12-31T00:00:00Z,:le")))
  }

  test("instant: greater than") {
    val filter = TypedLwcEventFilter(Map("timestamp" -> TypedLwcEventFilter.InstantMatcher))
    val event = LwcEvent(Map("timestamp" -> 1609459200000L))
    assert(filter.matches(event, parseQuery("timestamp,2020-12-31T00:00:00Z,:gt")))
    assert(!filter.matches(event, parseQuery("timestamp,2021-01-01T00:00:00Z,:gt")))
    assert(!filter.matches(event, parseQuery("timestamp,2021-01-02T00:00:00Z,:gt")))
  }

  test("instant: greater than or equal") {
    val filter = TypedLwcEventFilter(Map("timestamp" -> TypedLwcEventFilter.InstantMatcher))
    val event = LwcEvent(Map("timestamp" -> 1609459200000L))
    assert(filter.matches(event, parseQuery("timestamp,2020-12-31T00:00:00Z,:ge")))
    assert(filter.matches(event, parseQuery("timestamp,2021-01-01T00:00:00Z,:ge")))
    assert(!filter.matches(event, parseQuery("timestamp,2021-01-02T00:00:00Z,:ge")))
  }

  test("instant: in") {
    val filter = TypedLwcEventFilter(Map("timestamp" -> TypedLwcEventFilter.InstantMatcher))
    val event = LwcEvent(Map("timestamp" -> 1609459200000L))
    assert(
      filter.matches(
        event,
        parseQuery(
          "timestamp,(,2020-12-31T00:00:00Z,2021-01-01T00:00:00Z,2021-01-02T00:00:00Z,),:in"
        )
      )
    )
    assert(
      !filter.matches(
        event,
        parseQuery("timestamp,(,2020-12-31T00:00:00Z,2021-01-02T00:00:00Z,),:in")
      )
    )
  }

  test("instant: cast from Int") {
    val filter = TypedLwcEventFilter(Map("timestamp" -> TypedLwcEventFilter.InstantMatcher))
    val event = LwcEvent(Map("timestamp" -> 1609459200))
    assert(filter.matches(event, parseQuery("timestamp,2021-01-01T00:00:00Z,:eq")))
  }

  test("instant: cast from String") {
    val filter = TypedLwcEventFilter(Map("timestamp" -> TypedLwcEventFilter.InstantMatcher))
    val event = LwcEvent(Map("timestamp" -> "2021-01-01T00:00:00Z"))
    assert(filter.matches(event, parseQuery("timestamp,2021-01-01T00:00:00Z,:eq")))
  }
}

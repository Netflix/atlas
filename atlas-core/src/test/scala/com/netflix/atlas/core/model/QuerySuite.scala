/*
 * Copyright 2014-2025 Netflix, Inc.
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
package com.netflix.atlas.core.model

import com.netflix.atlas.core.util.SortedTagMap
import munit.FunSuite

class QuerySuite extends FunSuite {

  import com.netflix.atlas.core.model.Query.*

  def matches(q: Query, tags: Map[String, String]): Boolean = {
    val result = q.matches(tags)
    assertEquals(result, q.matches(SortedTagMap(tags)))
    assertEquals(result, q.matches(SortedTagMap(tags).getOrNull _))
    result
  }

  def matchesAny(q: Query, tags: Map[String, List[String]]): Boolean = {
    val result = q.matchesAny(tags)
    assertEquals(result, q.matchesAny(tags))
    result
  }

  def couldMatch(q: Query, tags: Map[String, String]): Boolean = {
    val result = q.couldMatch(tags)
    assertEquals(result, q.couldMatch(SortedTagMap(tags)))
    result
  }

  test("matches true") {
    val q = True
    assert(matches(q, Map("foo" -> "bar")))
    assert(matches(q, Map("foo" -> "bar2")))
    assert(matches(q, Map("foo2" -> "bar")))
  }

  test("matches false") {
    val q = False
    assert(!matches(q, Map("foo" -> "bar")))
    assert(!matches(q, Map("foo" -> "bar2")))
    assert(!matches(q, Map("foo2" -> "bar")))
  }

  test("matches eq") {
    val q = Equal("foo", "bar")
    assert(matches(q, Map("foo" -> "bar")))
    assert(!matches(q, Map("foo" -> "bar2")))
    assert(!matches(q, Map("foo2" -> "bar")))
  }

  test("matches gt") {
    val q = GreaterThan("foo", "bar")
    assert(!matches(q, Map("foo" -> "bar")))
    assert(matches(q, Map("foo" -> "bar2")))
    assert(!matches(q, Map("foo2" -> "bar")))
  }

  test("matches ge") {
    val q = GreaterThanEqual("foo", "bar")
    assert(matches(q, Map("foo" -> "bar")))
    assert(matches(q, Map("foo" -> "bar2")))
    assert(!matches(q, Map("foo2" -> "bar")))
  }

  test("matches lt") {
    val q = LessThan("foo", "bar")
    assert(!matches(q, Map("foo" -> "bar")))
    assert(matches(q, Map("foo" -> "ba")))
    assert(!matches(q, Map("foo2" -> "bar")))
  }

  test("matches le") {
    val q = LessThanEqual("foo", "bar")
    assert(matches(q, Map("foo" -> "bar")))
    assert(matches(q, Map("foo" -> "ba")))
    assert(!matches(q, Map("foo2" -> "bar")))
  }

  test("matches re") {
    val q = Regex("foo", "^b.*")
    assert(matches(q, Map("foo" -> "bar")))
    assert(matches(q, Map("foo" -> "bar2")))
    assert(!matches(q, Map("foo" -> "fubar2")))
    assert(!matches(q, Map("foo2" -> "bar")))
  }

  test("matches re implicit start anchor") {
    val q = Regex("foo", "b.*")
    assert(matches(q, Map("foo" -> "bar")))
    assert(matches(q, Map("foo" -> "bar2")))
    assert(!matches(q, Map("foo" -> "fubar2")))
    assert(!matches(q, Map("foo2" -> "bar")))
  }

  test("matches reic") {
    val q = RegexIgnoreCase("foo", "^B.*")
    assert(matches(q, Map("foo" -> "bar")))
    assert(matches(q, Map("foo" -> "Bar2")))
    assert(!matches(q, Map("foo" -> "fubar2")))
    assert(!matches(q, Map("foo2" -> "bar")))
  }

  test("matches reic implicit start anchor") {
    val q = RegexIgnoreCase("foo", "B.*")
    assert(matches(q, Map("foo" -> "bar")))
    assert(matches(q, Map("foo" -> "Bar2")))
    assert(!matches(q, Map("foo" -> "fubar2")))
    assert(!matches(q, Map("foo2" -> "bar")))
  }

  test("matches in") {
    val q = In("foo", List("bar", "baz"))
    assert(matches(q, Map("foo" -> "bar")))
    assert(matches(q, Map("foo" -> "baz")))
    assert(!matches(q, Map("foo" -> "bbb")))
    assert(!matches(q, Map("foo2" -> "bar")))
  }

  test("matches has") {
    val q = HasKey("foo")
    assert(matches(q, Map("foo" -> "bar")))
    assert(!matches(q, Map("foo2" -> "bar")))
  }

  test("matches not") {
    val q = Not(HasKey("foo"))
    assert(!matches(q, Map("foo" -> "bar")))
    assert(matches(q, Map("foo2" -> "bar")))
  }

  test("matches and") {
    val q = And(HasKey("foo"), HasKey("bar"))
    assert(matches(q, Map("foo" -> "bar", "bar" -> "foo")))
    assert(!matches(q, Map("foo" -> "bar")))
    assert(!matches(q, Map("bar" -> "foo")))
  }

  test("matches or") {
    val q = Or(HasKey("foo"), HasKey("bar"))
    assert(matches(q, Map("foo" -> "bar", "bar" -> "foo")))
    assert(matches(q, Map("foo" -> "bar")))
    assert(matches(q, Map("bar" -> "foo")))
    assert(!matches(q, Map("foo2" -> "bar", "bar2" -> "foo")))
  }

  test("matchesAny true") {
    val q = True
    assert(matchesAny(q, Map("foo" -> List("bar"), "bar" -> List("foo"))))
  }

  test("matchesAny false") {
    val q = False
    assert(!matchesAny(q, Map("foo" -> List("bar"), "bar" -> List("foo"))))
  }

  test("matchesAny eq with key match") {
    val q = Equal("foo", "bar")
    assert(matchesAny(q, Map("foo" -> List("bar"), "bar" -> List("foo"))))
    assert(matchesAny(q, Map("foo" -> List("foo", "bar"), "bar" -> List("foo"))))
    assert(matchesAny(q, Map("foo" -> List("bar", "baz"), "bar" -> List("foo"))))
  }

  test("matchesAny eq with key no match") {
    val q = Equal("foo", "baz")
    assert(!matchesAny(q, Map("foo" -> List("bar"), "bar" -> List("foo"))))
    assert(!matchesAny(q, Map("foo" -> List("foo", "bar"), "bar" -> List("foo"))))
  }

  test("matchesAny eq without key no match") {
    val q = Equal("foo2", "bar")
    assert(!matchesAny(q, Map("foo" -> List("bar"), "bar" -> List("foo"))))
  }

  test("matchesAny gt with key") {
    val q = GreaterThan("foo", "bar")
    assert(!matchesAny(q, Map("foo" -> List("bar"), "bar" -> List("foo"))))
    assert(matchesAny(q, Map("foo" -> List("foo", "bar"), "bar" -> List("foo"))))
    assert(matchesAny(q, Map("foo" -> List("bar", "baz"), "bar" -> List("foo"))))
  }

  test("matchesAny gt without key") {
    val q = GreaterThan("foo2", "bar")
    assert(!matchesAny(q, Map("foo" -> List("bar"), "bar" -> List("foo"))))
  }

  test("matchesAny ge with key") {
    val q = GreaterThanEqual("foo", "bar")
    assert(matchesAny(q, Map("foo" -> List("bar"), "bar" -> List("foo"))))
    assert(matchesAny(q, Map("foo" -> List("foo", "bar"), "bar" -> List("foo"))))
    assert(matchesAny(q, Map("foo" -> List("bar", "baz"), "bar" -> List("foo"))))
  }

  test("matchesAny ge without key") {
    val q = GreaterThanEqual("foo2", "bar")
    assert(!matchesAny(q, Map("foo" -> List("bar"), "bar" -> List("foo"))))
  }

  test("matchesAny lt with key") {
    val q = LessThan("foo", "bar")
    assert(!matchesAny(q, Map("foo" -> List("bar"), "bar" -> List("foo"))))
    assert(matchesAny(q, Map("foo" -> List("bah", "bar"), "bar" -> List("foo"))))
    assert(!matchesAny(q, Map("foo" -> List("bar", "baz"), "bar" -> List("foo"))))
  }

  test("matchesAny lt without key") {
    val q = LessThan("foo2", "bar")
    assert(!matchesAny(q, Map("foo" -> List("bar"), "bar" -> List("foo"))))
  }

  test("matchesAny le with key") {
    val q = LessThanEqual("foo", "bar")
    assert(matchesAny(q, Map("foo" -> List("bar"), "bar" -> List("foo"))))
    assert(matchesAny(q, Map("foo" -> List("bah", "bar"), "bar" -> List("foo"))))
    assert(matchesAny(q, Map("foo" -> List("bar", "baz"), "bar" -> List("foo"))))
  }

  test("matchesAny le without key") {
    val q = LessThanEqual("foo2", "bar")
    assert(!matchesAny(q, Map("foo" -> List("bar"), "bar" -> List("foo"))))
  }

  test("matchesAny re with key match") {
    val q = Regex("foo", "b")
    assert(matchesAny(q, Map("foo" -> List("bar"), "bar" -> List("foo"))))
    assert(matchesAny(q, Map("foo" -> List("foo", "bar"), "bar" -> List("foo"))))
    assert(matchesAny(q, Map("foo" -> List("bar", "baz"), "bar" -> List("foo"))))
  }

  test("matchesAny re with key no match") {
    val q = Regex("foo", "z")
    assert(!matchesAny(q, Map("foo" -> List("bar"), "bar" -> List("foo"))))
    assert(!matchesAny(q, Map("foo" -> List("foo", "bar"), "bar" -> List("foo"))))
  }

  test("matchesAny re without key no match") {
    val q = Regex("foo2", "bar")
    assert(!matchesAny(q, Map("foo" -> List("bar"), "bar" -> List("foo"))))
  }

  test("matchesAny has with key match") {
    val q = HasKey("foo")
    assert(matchesAny(q, Map("foo" -> List("bar"), "bar" -> List("foo"))))
  }

  test("matchesAny has with key no match") {
    val q = HasKey("foo")
    assert(matchesAny(q, Map("foo" -> List("bar"), "bar" -> List("foo"))))
  }

  test("matchesAny !true") {
    val q = Not(True)
    assert(!matchesAny(q, Map("foo" -> List("bar"), "bar" -> List("foo"))))
  }

  test("matchesAny !false") {
    val q = Not(False)
    assert(matchesAny(q, Map("foo" -> List("bar"), "bar" -> List("foo"))))
  }

  test("couldMatch true") {
    val q = True
    assert(couldMatch(q, Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch false") {
    val q = False
    assert(!couldMatch(q, Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch eq with key match") {
    val q = Equal("foo", "bar")
    assert(couldMatch(q, Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch eq with key no match") {
    val q = Equal("foo", "baz")
    assert(!couldMatch(q, Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch eq without key") {
    val q = Equal("foo2", "bar")
    assert(couldMatch(q, Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch gt with key match") {
    val q = GreaterThan("foo", "bar")
    assert(couldMatch(q, Map("foo" -> "baz", "bar" -> "foo")))
  }

  test("couldMatch gt with key no match") {
    val q = GreaterThan("foo", "bar")
    assert(!couldMatch(q, Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch gt without key") {
    val q = GreaterThan("foo2", "bar")
    assert(couldMatch(q, Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch lt with key match") {
    val q = LessThan("foo", "bar")
    assert(couldMatch(q, Map("foo" -> "bah", "bar" -> "foo")))
  }

  test("couldMatch lt with key no match") {
    val q = LessThan("foo", "bar")
    assert(!couldMatch(q, Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch lt without key") {
    val q = LessThan("foo2", "bar")
    assert(couldMatch(q, Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch re with key match") {
    val q = Regex("foo", "b")
    assert(couldMatch(q, Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch re with key no match") {
    val q = Regex("foo", "z")
    assert(!couldMatch(q, Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch re without key") {
    val q = Regex("foo2", "bar")
    assert(couldMatch(q, Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch has with key match") {
    val q = HasKey("foo")
    assert(couldMatch(q, Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch has with key no match") {
    val q = HasKey("foo")
    assert(couldMatch(q, Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch !true") {
    val q = Not(True)
    assert(!couldMatch(q, Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch !false") {
    val q = Not(False)
    assert(couldMatch(q, Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch not key query") {
    val q = Not(Equal("foo", "bar"))
    assert(!couldMatch(q, Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch not key query, possibly matches") {
    val q = Not(Equal("a", "b"))
    assert(couldMatch(q, Map("foo" -> "bar", "bar" -> "foo")))
  }

  private val a = HasKey("A")
  private val b = HasKey("B")
  private val c = HasKey("C")
  private val d = HasKey("D")

  test("expr rewrite") {
    val input = Or(a, And(b, c))
    val expected = Or(a, Or(c, b))
    val output = input.rewrite {
      case And(p, q) => Or(q, p)
    }
    assertEquals(output, expected)
  }

  test("exactKeys eq") {
    val q = Equal("k", "v")
    assertEquals(Query.exactKeys(q), Set("k"))
  }

  test("exactKeys and") {
    val q = And(Equal("k", "v"), Equal("p", "q"))
    assertEquals(Query.exactKeys(q), Set("k", "p"))
  }

  test("exactKeys or") {
    val q = Or(Equal("k", "v"), Equal("p", "q"))
    assertEquals(Query.exactKeys(q), Set.empty[String])
  }

  test("exactKeys or same key") {
    val q = Or(Equal("k", "v"), Equal("k", "q"))
    assertEquals(Query.exactKeys(q), Set.empty[String])
  }

  test("allKeys no key") {
    val q1 = Query.False
    assertEquals(Query.allKeys(q1), Set.empty[String])
    val q2 = Query.True
    assertEquals(Query.allKeys(q2), Set.empty[String])
  }

  test("allKeys ignore not") {
    val q = Query.Not(Equal("k", "v"))
    assertEquals(Query.allKeys(q), Set("k"))
  }

  test("allKeys eq") {
    val q = Equal("k", "v")
    assertEquals(Query.allKeys(q), Set("k"))
  }

  test("allKeys and eq") {
    val q = And(Equal("k", "v"), Equal("p", "q"))
    assertEquals(Query.allKeys(q), Set("k", "p"))
  }

  test("allKeys and hasKey") {
    val q = Query.And(a, b)
    assertEquals(Query.allKeys(q), Set("A", "B"))
  }

  test("allKeys or") {
    val q = Or(Equal("k", "v"), Equal("p", "q"))
    assertEquals(Query.allKeys(q), Set("k", "p"))
  }

  test("allKeys or - same key") {
    val q = Or(Equal("k", "v"), Equal("k", "q"))
    assertEquals(Query.allKeys(q), Set("k"))
  }

  test("allKeys or - one side no key") {
    val q = Or(Equal("k", "v"), True)
    assertEquals(Query.allKeys(q), Set("k"))
  }

  test("cnfList (a)") {
    val q = a
    assertEquals(Query.cnfList(q), List(q))
    assertEquals(Query.cnf(q), q)
  }

  test("cnfList (a or b)") {
    val q = Or(a, b)
    assertEquals(Query.cnfList(q), List(q))
    assertEquals(Query.cnf(q), q)
  }

  test("cnfList (a and b) or c)") {
    val q = Or(And(a, b), c)
    assertEquals(Query.cnfList(q), List(Or(a, c), Or(b, c)))
    assertEquals(Query.cnf(q), And(Or(a, c), Or(b, c)))
  }

  test("cnfList (a and b) or (c and d))") {
    val q = Or(And(a, b), And(c, d))
    assertEquals(Query.cnfList(q), List(Or(a, c), Or(a, d), Or(b, c), Or(b, d)))
    assertEquals(Query.cnf(q), And(And(And(Or(a, c), Or(a, d)), Or(b, c)), Or(b, d)))
  }

  test("cnfList not(a or b)") {
    val q = Not(Or(a, b))
    assertEquals(Query.cnfList(q), List(Not(a), Not(b)))
    assertEquals(Query.cnf(q), And(Not(a), Not(b)))
  }

  test("cnfList not(a and b)") {
    val q = Not(And(a, b))
    assertEquals(Query.cnfList(q), List(Or(Not(a), Not(b))))
    assertEquals(Query.cnf(q), Or(Not(a), Not(b)))
  }

  test("dnfList (a)") {
    val q = a
    assertEquals(Query.dnfList(q), List(q))
    assertEquals(Query.dnf(q), q)
  }

  test("dnfList (a and b)") {
    val q = And(a, b)
    assertEquals(Query.dnfList(q), List(q))
    assertEquals(Query.dnf(q), q)
  }

  test("dnfList (a or b) and c)") {
    val q = And(Or(a, b), c)
    assertEquals(Query.dnfList(q), List(And(a, c), And(b, c)))
    assertEquals(Query.dnf(q), Or(And(a, c), And(b, c)))
  }

  test("dnfList (a or b) and (c or d))") {
    val q = And(Or(a, b), Or(c, d))
    assertEquals(Query.dnfList(q), List(And(a, c), And(a, d), And(b, c), And(b, d)))
    assertEquals(Query.dnf(q), Or(Or(Or(And(a, c), And(a, d)), And(b, c)), And(b, d)))
  }

  test("dnfList not(a or b)") {
    val q = Not(Or(a, b))
    assertEquals(Query.dnfList(q), List(And(Not(a), Not(b))))
    assertEquals(Query.dnf(q), And(Not(a), Not(b)))
  }

  test("dnfList not(a and b)") {
    val q = Not(And(a, b))
    assertEquals(Query.dnfList(q), List(Not(a), Not(b)))
    assertEquals(Query.dnf(q), Or(Not(a), Not(b)))
  }

  test("simplify and(true, eq)") {
    val q = And(True, Equal("a", "b"))
    assertEquals(Query.simplify(q), Equal("a", "b"))
  }

  test("simplify and(eq, true)") {
    val q = And(Equal("a", "b"), True)
    assertEquals(Query.simplify(q), Equal("a", "b"))
  }

  test("simplify and(false, eq)") {
    val q = And(False, Equal("a", "b"))
    assertEquals(Query.simplify(q), False)
  }

  test("simplify and(eq, false)") {
    val q = And(Equal("a", "b"), False)
    assertEquals(Query.simplify(q), False)
  }

  test("simplify and(eq, eq)") {
    val q = And(Equal("a", "b"), Equal("c", "d"))
    assertEquals(Query.simplify(q), q)
  }

  test("simplify and recursive") {
    val q = And(And(True, Equal("a", "b")), And(Equal("c", "d"), False))
    assertEquals(Query.simplify(q), False)
  }

  test("simplify or(true, eq)") {
    val q = Or(True, Equal("a", "b"))
    assertEquals(Query.simplify(q), True)
  }

  test("simplify or(eq, true)") {
    val q = Or(Equal("a", "b"), True)
    assertEquals(Query.simplify(q), True)
  }

  test("simplify or(false, eq)") {
    val q = Or(False, Equal("a", "b"))
    assertEquals(Query.simplify(q), Equal("a", "b"))
  }

  test("simplify or(eq, false)") {
    val q = Or(Equal("a", "b"), False)
    assertEquals(Query.simplify(q), Equal("a", "b"))
  }

  test("simplify or(eq, eq)") {
    val q = Or(Equal("a", "b"), Equal("c", "d"))
    assertEquals(Query.simplify(q), q)
  }

  test("simplify or recursive") {
    val q = Or(Or(True, Equal("a", "b")), Or(Equal("c", "d"), False))
    assertEquals(Query.simplify(q), True)
  }

  test("simplify not(true)") {
    val q = Not(True)
    assertEquals(Query.simplify(q), False)
  }

  test("simplify not(true), ignore") {
    val q = Not(True)
    assertEquals(Query.simplify(q, ignore = true), True)
  }

  test("simplify not(false)") {
    val q = Not(False)
    assertEquals(Query.simplify(q), True)
  }

  test("simplify not recursive") {
    val q = Not(And(Not(False), Equal("a", "b")))
    assertEquals(Query.simplify(q), Not(Equal("a", "b")))
  }

  test("simplify not recursive ignore") {
    val q = Not(And(Not(True), Equal("a", "b")))
    assertEquals(Query.simplify(q, ignore = true), Not(Equal("a", "b")))
  }

  test("simplify not recursive ignore - Or") {
    val q = Or(And(Not(True), Equal("a", "b")), False)
    assertEquals(Query.simplify(q, ignore = true), Equal("a", "b"))
  }

  test("expandInClauses, simple query") {
    val q = Equal("a", "b")
    assertEquals(Query.expandInClauses(q), List(q))
  }

  test("expandInClauses, in query") {
    val q = In("a", List("b", "c"))
    assertEquals(Query.expandInClauses(q), List(Equal("a", "b"), Equal("a", "c")))
  }

  test("expandInClauses, conjunction with in query") {
    val base = Equal("a", "1")
    val q = And(base, In("b", List("v1", "v2")))
    val expected = List(
      And(base, Equal("b", "v1")),
      And(base, Equal("b", "v2"))
    )
    assertEquals(Query.expandInClauses(q), expected)
  }

  test("expandInClauses, number of values exceeds limit") {
    val q = In("a", List("b", "c"))
    assertEquals(Query.expandInClauses(q, 1), List(q))
  }

  test("expandInClauses, number of values equals limit") {
    val q = In("a", List("b", "c"))
    assertEquals(Query.expandInClauses(q, 2), List(Equal("a", "b"), Equal("a", "c")))
  }

  test("expandInClauses, conjunction with multiple in queries") {
    val q = And(In("a", List("a1", "a2")), In("b", List("b1", "b2", "b3")))
    val expected = for (a <- List("a1", "a2"); b <- List("b1", "b2", "b3")) yield {
      And(Equal("a", a), Equal("b", b))
    }
    assertEquals(Query.expandInClauses(q), expected)
  }

  test("expandInClauses, disjunction") {
    val q = Or(Equal("a", "1"), In("b", List("1", "2")))
    assertEquals(Query.expandInClauses(q, 1), List(q))
  }

}

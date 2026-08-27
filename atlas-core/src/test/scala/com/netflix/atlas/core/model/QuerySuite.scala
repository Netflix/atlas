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
package com.netflix.atlas.core.model

import com.netflix.atlas.core.stacklang.Interpreter
import com.netflix.atlas.core.util.SortedTagMap
import munit.FunSuite

class QuerySuite extends FunSuite {

  import com.netflix.atlas.core.model.Query.*

  private val interpreter = Interpreter(QueryVocabulary.allWords)

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

  test("comment start escaped in patterns") {
    val q = Regex("a", "http:/*")
    assertEquals(q.toString, "a,http\\u003a\\u002f*,:re")
  }

  /** Conjunction of `n` disjunctions, the dnf of which has 2^n clauses. */
  private def orPairs(n: Int): Query = {
    val pair = (i: Int) => Or(Equal(s"k$i", "a"), Equal(s"k$i", "b"))
    (1 until n).foldLeft[Query](pair(0))((acc, i) => And(acc, pair(i)))
  }

  /** Same shape with the operators swapped, the cnf of which has 2^n clauses. */
  private def andPairs(n: Int): Query = {
    val pair = (i: Int) => And(Equal(s"k$i", "a"), Equal(s"k$i", "b"))
    (1 until n).foldLeft[Query](pair(0))((acc, i) => Or(acc, pair(i)))
  }

  test("dnfList is bounded") {
    // 2^14 clauses, above the largest expansion seen for expressions in use and well under
    // the limit.
    assertEquals(Query.dnfList(orPairs(14)).size, 16384)
    intercept[IllegalArgumentException] {
      Query.dnfList(orPairs(18))
    }
  }

  test("dnfList bound applies to disjunctions") {
    // Each side expands to 2^16 clauses, under the limit on its own. The concatenation is
    // not a product, but it still has to be bounded or a handful of disjunctions can
    // produce many times the limit.
    intercept[IllegalArgumentException] {
      Query.dnfList(Or(orPairs(16), orPairs(16)))
    }
  }

  test("cnfList is bounded") {
    assertEquals(Query.cnfList(andPairs(14)).size, 16384)
    intercept[IllegalArgumentException] {
      Query.cnfList(andPairs(18))
    }
  }

  test("cnfList bound applies to conjunctions") {
    intercept[IllegalArgumentException] {
      Query.cnfList(And(andPairs(16), andPairs(16)))
    }
  }

  test("expandInClauses is bounded") {
    // Each `:in` is small enough to expand on its own, the product across the conjunction is
    // what grows: 5^8 clauses.
    val in = (i: Int) => In(s"k$i", List("a", "b", "c", "d", "e"))
    val q = (1 until 8).foldLeft[Query](in(0))((acc, i) => And(acc, in(i)))
    intercept[IllegalArgumentException] {
      Query.expandInClauses(q)
    }
  }

  test("expandInClauses allows expansion up to the limit") {
    val in = (i: Int) => In(s"k$i", List("a", "b", "c", "d", "e"))
    val q = (1 until 4).foldLeft[Query](in(0))((acc, i) => And(acc, in(i)))
    assertEquals(Query.expandInClauses(q).size, 625)
  }

  private def leaf(i: Int): Query = Equal("k", s"v$i")

  /** Chain of `n` sub-queries nested to the left, the shape produced by `dnf` and `cnf`. */
  private def chain(n: Int, op: (Query, Query) => Query): Query = {
    (1 until n).foldLeft(leaf(0))((acc, i) => op(acc, leaf(i)))
  }

  /** Chain of `n` sub-queries nested to the right. */
  private def rightChain(n: Int, op: (Query, Query) => Query): Query = {
    (0 until n - 1).foldRight(leaf(n - 1))((i, acc) => op(leaf(i), acc))
  }

  /** Chain of `n` sub-queries nested to the left, alternating between the two operators. */
  private def alternatingChain(n: Int): Query = {
    (1 until n).foldLeft(leaf(0)) { (acc, i) =>
      if (i % 2 == 0) And(acc, leaf(i)) else Or(acc, leaf(i))
    }
  }

  /** Expected rendering of `chain(n, op)` where the operator is written as `token`. */
  private def expectedChain(n: Int, token: String): String = {
    val builder = new java.lang.StringBuilder
    builder.append("k,v0,:eq")
    (1 until n).foreach { i =>
      builder.append(",k,v").append(i).append(",:eq,").append(token)
    }
    builder.toString
  }

  test("toString of a long or chain") {
    // A query is allowed to expand to `Query.maxNormalFormClauses`, and `dnf` reduces the
    // clauses into a chain nested to the left, so rendering has to handle a chain that long.
    val q = chain(maxNormalFormClauses, Or.apply)
    assertEquals(q.toString, expectedChain(maxNormalFormClauses, ":or"))
  }

  test("toString of a long and chain") {
    val q = chain(maxNormalFormClauses, And.apply)
    assertEquals(q.toString, expectedChain(maxNormalFormClauses, ":and"))
  }

  test("toString of a chain matches the recursive rendering") {
    // The chains are short enough to render either way, so the iterative walk can be checked
    // against the straightforward definition. Only the operator of the node being rendered is
    // walked iteratively, so chains nested to the right and chains that alternate between the
    // operators are covered as well.
    val ops = List[(Query, Query) => Query](Or.apply, And.apply)
    val queries = ops.flatMap(op => List(chain(64, op), rightChain(64, op))) :+ alternatingChain(64)
    queries.foreach { q =>
      val expected = new java.lang.StringBuilder
      def recurse(sub: Query): Unit = sub match {
        case Or(a, b)  => recurse(a); expected.append(','); recurse(b); expected.append(",:or")
        case And(a, b) => recurse(a); expected.append(','); recurse(b); expected.append(",:and")
        case other     => other.append(expected)
      }
      recurse(q)
      assertEquals(q.toString, expected.toString)
      // `exprString` is documented as executable by the interpreter, so it has to round trip.
      assertEquals(interpreter.execute(q.toString).stack, List[Any](q))
    }
  }

  test("toString of alternating and or nesting") {
    // Chains of one operator interrupted by the other must still round trip.
    val q = And(Or(Equal("a", "1"), Equal("b", "2")), Or(Equal("c", "3"), Equal("d", "4")))
    assertEquals(q.toString, "a,1,:eq,b,2,:eq,:or,c,3,:eq,d,4,:eq,:or,:and")
    assertEquals(interpreter.execute(q.toString).stack, List[Any](q))
  }

  test("toString of a chain with a sub-query at the head") {
    // The walk down the left side of a chain stops on the first sub-query using the other
    // operator, so the head of the chain is not always a leaf.
    val q1 = And(And(Or(Equal("a", "1"), Equal("b", "2")), Equal("c", "3")), Equal("d", "4"))
    assertEquals(q1.toString, "a,1,:eq,b,2,:eq,:or,c,3,:eq,:and,d,4,:eq,:and")
    assertEquals(interpreter.execute(q1.toString).stack, List[Any](q1))

    val q2 = Or(Or(And(Equal("a", "1"), Equal("b", "2")), Equal("c", "3")), Equal("d", "4"))
    assertEquals(q2.toString, "a,1,:eq,b,2,:eq,:and,c,3,:eq,:or,d,4,:eq,:or")
    assertEquals(interpreter.execute(q2.toString).stack, List[Any](q2))
  }
}

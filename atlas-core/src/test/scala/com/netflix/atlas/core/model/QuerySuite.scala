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
package com.netflix.atlas.core.model

import org.scalatest.FunSuite

class QuerySuite extends FunSuite {

  import com.netflix.atlas.core.model.Query._

  test("matches true") {
    val q = True
    assert(q.matches(Map("foo" -> "bar")))
    assert(q.matches(Map("foo" -> "bar2")))
    assert(q.matches(Map("foo2" -> "bar")))
  }

  test("matches false") {
    val q = False
    assert(!q.matches(Map("foo" -> "bar")))
    assert(!q.matches(Map("foo" -> "bar2")))
    assert(!q.matches(Map("foo2" -> "bar")))
  }

  test("matches eq") {
    val q = Equal("foo", "bar")
    assert(q.matches(Map("foo" -> "bar")))
    assert(!q.matches(Map("foo" -> "bar2")))
    assert(!q.matches(Map("foo2" -> "bar")))
  }

  test("matches gt") {
    val q = GreaterThan("foo", "bar")
    assert(!q.matches(Map("foo" -> "bar")))
    assert(q.matches(Map("foo" -> "bar2")))
    assert(!q.matches(Map("foo2" -> "bar")))
  }

  test("matches ge") {
    val q = GreaterThanEqual("foo", "bar")
    assert(q.matches(Map("foo" -> "bar")))
    assert(q.matches(Map("foo" -> "bar2")))
    assert(!q.matches(Map("foo2" -> "bar")))
  }

  test("matches lt") {
    val q = LessThan("foo", "bar")
    assert(!q.matches(Map("foo" -> "bar")))
    assert(q.matches(Map("foo" -> "ba")))
    assert(!q.matches(Map("foo2" -> "bar")))
  }

  test("matches le") {
    val q = LessThanEqual("foo", "bar")
    assert(q.matches(Map("foo" -> "bar")))
    assert(q.matches(Map("foo" -> "ba")))
    assert(!q.matches(Map("foo2" -> "bar")))
  }

  test("matches re") {
    val q = Regex("foo", "^b.*")
    assert(q.matches(Map("foo" -> "bar")))
    assert(q.matches(Map("foo" -> "bar2")))
    assert(!q.matches(Map("foo" -> "fubar2")))
    assert(!q.matches(Map("foo2" -> "bar")))
  }

  test("matches re implicit start anchor") {
    val q = Regex("foo", "b.*")
    assert(q.matches(Map("foo" -> "bar")))
    assert(q.matches(Map("foo" -> "bar2")))
    assert(!q.matches(Map("foo" -> "fubar2")))
    assert(!q.matches(Map("foo2" -> "bar")))
  }

  test("matches reic") {
    val q = RegexIgnoreCase("foo", "^B.*")
    assert(q.matches(Map("foo" -> "bar")))
    assert(q.matches(Map("foo" -> "Bar2")))
    assert(!q.matches(Map("foo" -> "fubar2")))
    assert(!q.matches(Map("foo2" -> "bar")))
  }

  test("matches reic implicit start anchor") {
    val q = RegexIgnoreCase("foo", "B.*")
    assert(q.matches(Map("foo" -> "bar")))
    assert(q.matches(Map("foo" -> "Bar2")))
    assert(!q.matches(Map("foo" -> "fubar2")))
    assert(!q.matches(Map("foo2" -> "bar")))
  }

  test("matches in") {
    val q = In("foo", List("bar", "baz"))
    assert(q.matches(Map("foo" -> "bar")))
    assert(q.matches(Map("foo" -> "baz")))
    assert(!q.matches(Map("foo" -> "bbb")))
    assert(!q.matches(Map("foo2" -> "bar")))
  }

  test("matches has") {
    val q = HasKey("foo")
    assert(q.matches(Map("foo" -> "bar")))
    assert(!q.matches(Map("foo2" -> "bar")))
  }

  test("matches not") {
    val q = Not(HasKey("foo"))
    assert(!q.matches(Map("foo" -> "bar")))
    assert(q.matches(Map("foo2" -> "bar")))
  }

  test("matches and") {
    val q = And(HasKey("foo"), HasKey("bar"))
    assert(q.matches(Map("foo" -> "bar", "bar" -> "foo")))
    assert(!q.matches(Map("foo" -> "bar")))
    assert(!q.matches(Map("bar" -> "foo")))
  }

  test("matches or") {
    val q = Or(HasKey("foo"), HasKey("bar"))
    assert(q.matches(Map("foo" -> "bar", "bar" -> "foo")))
    assert(q.matches(Map("foo" -> "bar")))
    assert(q.matches(Map("bar" -> "foo")))
    assert(!q.matches(Map("foo2" -> "bar", "bar2" -> "foo")))
  }

  test("matchesAny true") {
    val q = True
    assert(q.matchesAny(Map("foo" -> List("bar"), "bar" -> List("foo"))))
  }

  test("matchesAny false") {
    val q = False
    assert(!q.matchesAny(Map("foo" -> List("bar"), "bar" -> List("foo"))))
  }

  test("matchesAny eq with key match") {
    val q = Equal("foo", "bar")
    assert(q.matchesAny(Map("foo" -> List("bar"), "bar" -> List("foo"))))
    assert(q.matchesAny(Map("foo" -> List("foo","bar"), "bar" -> List("foo"))))
    assert(q.matchesAny(Map("foo" -> List("bar","baz"), "bar" -> List("foo"))))
  }

  test("matchesAny eq with key no match") {
    val q = Equal("foo", "baz")
    assert(!q.matchesAny(Map("foo" -> List("bar"), "bar" -> List("foo"))))
    assert(!q.matchesAny(Map("foo" -> List("foo","bar"), "bar" -> List("foo"))))
  }

  test("matchesAny eq without key no match") {
    val q = Equal("foo2", "bar")
    assert(!q.matchesAny(Map("foo" -> List("bar"), "bar" -> List("foo"))))
  }

  test("matchesAny gt with key") {
    val q = GreaterThan("foo", "bar")
    assert(!q.matchesAny(Map("foo" -> List("bar"), "bar" -> List("foo"))))
    assert(q.matchesAny(Map("foo" -> List("foo","bar"), "bar" -> List("foo"))))
    assert(q.matchesAny(Map("foo" -> List("bar","baz"), "bar" -> List("foo"))))
  }

  test("matchesAny gt without key") {
    val q = GreaterThan("foo2", "bar")
    assert(!q.matchesAny(Map("foo" -> List("bar"), "bar" -> List("foo"))))
  }

  test("matchesAny ge with key") {
    val q = GreaterThanEqual("foo", "bar")
    assert(q.matchesAny(Map("foo" -> List("bar"), "bar" -> List("foo"))))
    assert(q.matchesAny(Map("foo" -> List("foo","bar"), "bar" -> List("foo"))))
    assert(q.matchesAny(Map("foo" -> List("bar","baz"), "bar" -> List("foo"))))
  }

  test("matchesAny ge without key") {
    val q = GreaterThanEqual("foo2", "bar")
    assert(!q.matchesAny(Map("foo" -> List("bar"), "bar" -> List("foo"))))
  }

  test("matchesAny lt with key") {
    val q = LessThan("foo", "bar")
    assert(!q.matchesAny(Map("foo" -> List("bar"), "bar" -> List("foo"))))
    assert(q.matchesAny(Map("foo" -> List("bah","bar"), "bar" -> List("foo"))))
    assert(!q.matchesAny(Map("foo" -> List("bar","baz"), "bar" -> List("foo"))))
  }

  test("matchesAny lt without key") {
    val q = LessThan("foo2", "bar")
    assert(!q.matchesAny(Map("foo" -> List("bar"), "bar" -> List("foo"))))
  }

  test("matchesAny le with key") {
    val q = LessThanEqual("foo", "bar")
    assert(q.matchesAny(Map("foo" -> List("bar"), "bar" -> List("foo"))))
    assert(q.matchesAny(Map("foo" -> List("bah","bar"), "bar" -> List("foo"))))
    assert(q.matchesAny(Map("foo" -> List("bar","baz"), "bar" -> List("foo"))))
  }

  test("matchesAny le without key") {
    val q = LessThanEqual("foo2", "bar")
    assert(!q.matchesAny(Map("foo" -> List("bar"), "bar" -> List("foo"))))
  }

  test("matchesAny re with key match") {
    val q = GreaterThan("foo", "b")
    assert(q.matchesAny(Map("foo" -> List("bar"), "bar" -> List("foo"))))
    assert(q.matchesAny(Map("foo" -> List("foo","bar"), "bar" -> List("foo"))))
    assert(q.matchesAny(Map("foo" -> List("bar","baz"), "bar" -> List("foo"))))
  }

  test("matchesAny re with key no match") {
    val q = Regex("foo", "z")
    assert(!q.matchesAny(Map("foo" -> List("bar"), "bar" -> List("foo"))))
    assert(!q.matchesAny(Map("foo" -> List("foo","bar"), "bar" -> List("foo"))))
  }

  test("matchesAny re without key no match") {
    val q = Regex("foo2", "bar")
    assert(!q.matchesAny(Map("foo" -> List("bar"), "bar" -> List("foo"))))
  }

  test("matchesAny has with key match") {
    val q = HasKey("foo")
    assert(q.matchesAny(Map("foo" -> List("bar"), "bar" -> List("foo"))))
  }

  test("matchesAny has with key no match") {
    val q = HasKey("foo")
    assert(q.matchesAny(Map("foo" -> List("bar"), "bar" -> List("foo"))))
  }

  test("matchesAny !true") {
    val q = Not(True)
    assert(!q.matchesAny(Map("foo" -> List("bar"), "bar" -> List("foo"))))
  }

  test("matchesAny !false") {
    val q = Not(False)
    assert(q.matchesAny(Map("foo" -> List("bar"), "bar" -> List("foo"))))
  }

  test("couldMatch true") {
    val q = True
    assert(q.couldMatch(Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch false") {
    val q = False
    assert(!q.couldMatch(Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch eq with key match") {
    val q = Equal("foo", "bar")
    assert(q.couldMatch(Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch eq with key no match") {
    val q = Equal("foo", "baz")
    assert(!q.couldMatch(Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch eq without key") {
    val q = Equal("foo2", "bar")
    assert(q.couldMatch(Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch gt with key match") {
    val q = GreaterThan("foo", "bar")
    assert(q.couldMatch(Map("foo" -> "baz", "bar" -> "foo")))
  }

  test("couldMatch gt with key no match") {
    val q = GreaterThan("foo", "bar")
    assert(!q.couldMatch(Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch gt without key") {
    val q = GreaterThan("foo2", "bar")
    assert(q.couldMatch(Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch lt with key match") {
    val q = LessThan("foo", "bar")
    assert(q.couldMatch(Map("foo" -> "bah", "bar" -> "foo")))
  }

  test("couldMatch lt with key no match") {
    val q = LessThan("foo", "bar")
    assert(!q.couldMatch(Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch lt without key") {
    val q = LessThan("foo2", "bar")
    assert(q.couldMatch(Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch re with key match") {
    val q = Regex("foo", "b")
    assert(q.couldMatch(Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch re with key no match") {
    val q = Regex("foo", "z")
    assert(!q.couldMatch(Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch re without key") {
    val q = Regex("foo2", "bar")
    assert(q.couldMatch(Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch has with key match") {
    val q = HasKey("foo")
    assert(q.couldMatch(Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch has with key no match") {
    val q = HasKey("foo")
    assert(q.couldMatch(Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch !true") {
    val q = Not(True)
    assert(q.couldMatch(Map("foo" -> "bar", "bar" -> "foo")))
  }

  test("couldMatch !false") {
    val q = Not(False)
    assert(q.couldMatch(Map("foo" -> "bar", "bar" -> "foo")))
  }

  val a = HasKey("A")
  val b = HasKey("B")
  val c = HasKey("C")
  val d = HasKey("D")

  test("expr rewrite") {
    val input = Or(a, And(b, c))
    val expected = Or(a, Or(c, b))
    val output = input.rewrite {
      case And(p, q) => Or(q, p)
    }
    assert(output === expected)
  }

  test("exactKeys eq") {
    val q = Equal("k", "v")
    assert(Query.exactKeys(q) === Set("k"))
  }

  test("exactKeys and") {
    val q = And(Equal("k", "v"), Equal("p", "q"))
    assert(Query.exactKeys(q) === Set("k", "p"))
  }

  test("exactKeys or") {
    val q = Or(Equal("k", "v"), Equal("p", "q"))
    assert(Query.exactKeys(q) === Set.empty)
  }

  test("exactKeys or same key") {
    val q = Or(Equal("k", "v"), Equal("k", "q"))
    assert(Query.exactKeys(q) === Set.empty)
  }

  test("cnfList (a)") {
    val q = a
    assert(Query.cnfList(q) === List(q))
    assert(Query.cnf(q) === q)
  }

  test("cnfList (a or b)") {
    val q = Or(a, b)
    assert(Query.cnfList(q) === List(q))
    assert(Query.cnf(q) === q)
  }

  test("cnfList (a and b) or c)") {
    val q = Or(And(a, b), c)
    assert(Query.cnfList(q) === List(Or(a, c), Or(b, c)))
    assert(Query.cnf(q) === And(Or(a, c), Or(b, c)))
  }

  test("cnfList (a and b) or (c and d))") {
    val q = Or(And(a, b), And(c, d))
    assert(Query.cnfList(q) === List(Or(a, c), Or(a, d), Or(b, c), Or(b, d)))
    assert(Query.cnf(q) === And(And(And(Or(a, c), Or(a, d)), Or(b, c)), Or(b, d)))
  }

  test("cnfList not(a or b)") {
    val q = Not(Or(a, b))
    assert(Query.cnfList(q) === List(Not(a), Not(b)))
    assert(Query.cnf(q) === And(Not(a), Not(b)))
  }

  test("cnfList not(a and b)") {
    val q = Not(And(a, b))
    assert(Query.cnfList(q) === List(Or(Not(a), Not(b))))
    assert(Query.cnf(q) === Or(Not(a), Not(b)))
  }

  test("dnfList (a)") {
    val q = a
    assert(Query.dnfList(q) === List(q))
    assert(Query.dnf(q) === q)
  }

  test("dnfList (a and b)") {
    val q = And(a, b)
    assert(Query.dnfList(q) === List(q))
    assert(Query.dnf(q) === q)
  }

  test("dnfList (a or b) and c)") {
    val q = And(Or(a, b), c)
    assert(Query.dnfList(q) === List(And(a, c), And(b, c)))
    assert(Query.dnf(q) === Or(And(a, c), And(b, c)))
  }

  test("dnfList (a or b) and (c or d))") {
    val q = And(Or(a, b), Or(c, d))
    assert(Query.dnfList(q) === List(And(a, c), And(a, d), And(b, c), And(b, d)))
    assert(Query.dnf(q) === Or(Or(Or(And(a, c), And(a, d)), And(b, c)), And(b, d)))
  }

  test("dnfList not(a or b)") {
    val q = Not(Or(a, b))
    assert(Query.dnfList(q) === List(And(Not(a), Not(b))))
    assert(Query.dnf(q) === And(Not(a), Not(b)))
  }

  test("dnfList not(a and b)") {
    val q = Not(And(a, b))
    assert(Query.dnfList(q) === List(Not(a), Not(b)))
    assert(Query.dnf(q) === Or(Not(a), Not(b)))
  }
}

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
package com.netflix.atlas.postgres

import com.netflix.atlas.core.index.TagQuery
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.DataVocabulary
import com.netflix.atlas.core.model.ModelExtractors
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.stacklang.Interpreter

import java.time.Instant

class SqlUtilsSuite extends PostgresSuite {

  private val time = Instant.ofEpochMilli(1647892800000L)
  private val suffix = "202203212000"

  private val interpreter = Interpreter(DataVocabulary.allWords)

  test("toSuffix") {
    assertEquals(SqlUtils.toSuffix(time), suffix)
  }

  test("extractTime") {
    assertEquals(SqlUtils.extractTime(s"foo_$suffix"), Some(time))
    assertEquals(SqlUtils.extractTime(s"foo"), None)
  }

  test("escapeLiteral") {
    assertEquals(SqlUtils.escapeLiteral("foo'bar"), "foo''bar")
  }

  test("listTables") {
    dropTables()
    service.runQueries { stmt =>
      executeUpdate("create table atlas.foo(a varchar(10))")
      executeUpdate("create table atlas.bar(a varchar(10))")
      val ts = List.newBuilder[String]
      val rs = stmt.executeQuery(SqlUtils.listTables)
      while (rs.next()) {
        ts += rs.getString(1)
      }
      assertEquals(ts.result().sorted, List("bar", "foo"))
    }
  }

  test("createTable: for name") {
    dropTables()
    val config = TableDefinition("sys.cpu", "cpu", List("nf.app", "id"), "varchar(255)")
    val sql = SqlUtils.createTable(config, time)
    executeUpdate(sql)
    executeUpdate(s"""
        |insert into atlas.cpu_$suffix
        |values (ARRAY[1, 2], '"nf.cluster"=>"www-main","nf.stack"=>"main"', 'www', 'system')
        |""".stripMargin)
  }

  test("createTable: for name, just tags column") {
    dropTables()
    val config = TableDefinition("sys.cpu", "cpu", Nil, "varchar(255)")
    val sql = SqlUtils.createTable(config, time)
    executeUpdate(sql)
    executeUpdate(s"""
        |insert into atlas.cpu_$suffix
        |values (ARRAY[1, 2], '"nf.cluster"=>"www-main","nf.stack"=>"main"')
        |""".stripMargin)
  }

  private def populateSelectTable(): TableDefinition = {
    SqlUtils.customFunctions.foreach(executeUpdate)
    dropTables()
    val table = TableDefinition("*", "others", List("name"), "varchar(255)")
    executeUpdate(SqlUtils.createTable(table, time))
    (0 until 10).foreach { i =>
      executeUpdate(s"""
           |insert into atlas.others_$suffix
           |values (ARRAY[$i, $i], '"nf.app"=>"www","nf.node"=>"i-$i"', 'cpu')
           |""".stripMargin)
      val d = i * 10
      executeUpdate(s"""
           |insert into atlas.others_$suffix
           |values (ARRAY[$d, $d], '"nf.app"=>"www","nf.node"=>"i-$i"', 'disk')
           |""".stripMargin)
    }
    executeUpdate(s"""
         |insert into atlas.others_$suffix
         |values (ARRAY[100.0, 100.0], '"nf.app"=>"db","id"=>"postgres"', 'disk')
         |""".stripMargin)
    table
  }

  private def parseQuery(str: String): Query = {
    interpreter.execute(str).stack match {
      case (q: Query) :: Nil => q
      case _                 => throw new IllegalArgumentException(str)
    }
  }

  private def parseDataExpr(str: String): DataExpr = {
    interpreter.execute(str).stack match {
      case ModelExtractors.DataExprType(t) :: Nil => t
      case _                                      => throw new IllegalArgumentException(str)
    }
  }

  private def stringList(sql: String): List[String] = {
    service.runQueries { stmt =>
      val vs = List.newBuilder[String]
      val rs = stmt.executeQuery(sql)
      while (rs.next()) {
        vs += rs.getString(1)
      }
      vs.result()
    }
  }

  private def arrayQuery(sql: String): List[java.lang.Double] = {
    service.runQueries { stmt =>
      val rs = stmt.executeQuery(sql)
      assert(rs.next())
      val array = rs.getArray(1)
      if (array == null) {
        Nil
      } else {
        val vs = array.getArray
          .asInstanceOf[Array[java.lang.Double]]
          .toList
        assert(!rs.next())
        vs
      }
    }
  }

  private def list(vs: java.lang.Double*): List[java.lang.Double] = vs.toList

  private def basicKeys(tq: TagQuery, expected: List[String]): Unit = {
    val table = populateSelectTable()
    val queries = SqlUtils.keyQueries(time, List(table), tq)
    queries.foreach { sql =>
      assertEquals(stringList(sql), expected)
    }
  }

  test("keys: all") {
    val tq = TagQuery(None)
    basicKeys(tq, List("id", "name", "nf.app", "nf.node"))
  }

  test("keys: none") {
    val query = parseQuery(":false")
    val tq = TagQuery(Some(query))
    basicKeys(tq, Nil)
  }

  test("keys: cpu") {
    val query = parseQuery("name,cpu,:eq")
    val tq = TagQuery(Some(query))
    basicKeys(tq, List("name", "nf.app", "nf.node"))
  }

  test("keys: db") {
    val query = parseQuery("nf.app,db,:eq")
    val tq = TagQuery(Some(query))
    basicKeys(tq, List("id", "name", "nf.app"))
  }

  private def basicValues(tq: TagQuery, expected: List[String]): Unit = {
    val table = populateSelectTable()
    val queries = SqlUtils.valueQueries(time, List(table), tq)
    queries.foreach { sql =>
      assertEquals(stringList(sql), expected)
    }
  }

  test("values: name") {
    val tq = TagQuery(None, Some("name"))
    basicValues(tq, List("cpu", "disk"))
  }

  test("values: with query no matches, name") {
    val query = parseQuery(":false")
    val tq = TagQuery(Option(query), Some("name"))
    basicValues(tq, Nil)
  }

  test("values: with query, name") {
    val query = parseQuery("name,cpu,:eq")
    val tq = TagQuery(Option(query), Some("name"))
    basicValues(tq, List("cpu"))
  }

  test("values: with query, nf.node") {
    val query = parseQuery("name,cpu,:eq")
    val tq = TagQuery(Option(query), Some("nf.node"))
    basicValues(tq, (0 until 10).map(i => s"i-$i").toList)
  }

  test("values: with query and limit, nf.node") {
    val query = parseQuery("name,cpu,:eq")
    val tq = TagQuery(Option(query), Some("nf.node"), limit = 2)
    basicValues(tq, (0 until 2).map(i => s"i-$i").toList)
  }

  test("values: with query, offset, and limit, nf.node") {
    val query = parseQuery("name,cpu,:eq")
    val tq = TagQuery(Option(query), Some("nf.node"), offset = "i-4", limit = 2)
    basicValues(tq, (5 until 7).map(i => s"i-$i").toList)
  }

  private def basicSelect(exprStr: String, v: Double): Unit = {
    val table = populateSelectTable()
    val expr = parseDataExpr(exprStr)
    val queries = SqlUtils.dataQueries(time, List(table), expr)
    queries.foreach { sql =>
      val expected = if (v.isNaN) Nil else List(v, v)
      assertEquals(arrayQuery(sql).map(_.doubleValue()), expected)
    }
  }

  test("select: sum") {
    basicSelect("name,cpu,:eq,:sum", 45.0)
  }

  test("select: sum, disk") {
    basicSelect("name,disk,:eq,:sum", 550.0)
  }

  test("select: sum, has") {
    basicSelect("name,cpu,:eq,nf.node,:has,:and,:sum", 45.0)
  }

  test("select: sum, has, no data") {
    basicSelect("name,cpu,:eq,foo,:has,:and,:sum", Double.NaN)
  }

  test("select: sum, in") {
    basicSelect("name,cpu,:eq,nf.node,(,i-2,i-4,),:in,:and,:sum", 6.0)
  }

  test("select: sum, greater than") {
    basicSelect("name,cpu,:eq,nf.node,i-7,:gt,:and,:sum", 17.0)
  }

  test("select: sum, greater than equal") {
    basicSelect("name,cpu,:eq,nf.node,i-7,:ge,:and,:sum", 24.0)
  }

  test("select: sum, less than") {
    basicSelect("name,cpu,:eq,nf.node,i-3,:lt,:and,:sum", 3.0)
  }

  test("select: sum, less than equal") {
    basicSelect("name,cpu,:eq,nf.node,i-3,:le,:and,:sum", 6.0)
  }

  test("select: sum, regex") {
    basicSelect("name,cpu,:eq,nf.node,i-[3-6],:re,:and,:sum", 18.0)
  }

  test("select: sum, regex mapped to like") {
    basicSelect("name,cpu,:eq,nf.node,i-.*,:re,:and,:sum", 45.0)
  }

  test("select: sum, regex ignore case") {
    basicSelect("name,cpu,:eq,nf.node,I-[3-6],:reic,:and,:sum", 18.0)
  }

  test("select: sum, or") {
    basicSelect("name,cpu,:eq,nf.node,I-[3-6],:reic,nf.node,i-1,:eq,:or,:and,:sum", 19.0)
  }

  test("select: sum, not") {
    basicSelect("name,cpu,:eq,nf.node,I-[3-6],:reic,nf.node,i-4,:eq,:not,:and,:and,:sum", 14.0)
  }

  test("select: count") {
    basicSelect("name,cpu,:eq,:count", 10.0)
  }

  test("select: max") {
    basicSelect("name,cpu,:eq,:max", 9.0)
  }

  test("select: min") {
    basicSelect("name,cpu,:eq,:min", 0.0)
  }

  private def basicSelectBy(exprStr: String, k: String, values: Map[String, Double]): Unit = {
    val table = populateSelectTable()
    val expr = parseDataExpr(exprStr)
    val queries = SqlUtils.dataQueries(time, List(table), expr)
    service.runQueries { stmt =>
      queries.foreach { sql =>
        val rs = stmt.executeQuery(sql)
        while (rs.next()) {
          val c = rs.getString(k)
          val expected = values(c)
          val vs = rs
            .getArray("values")
            .getArray
            .asInstanceOf[Array[java.lang.Double]]
            .toList
          assertEquals(vs.map(_.doubleValue()), List(expected, expected))
        }
      }
    }
  }

  test("select: sum by name") {
    basicSelectBy(
      "nf.app,www,:eq,:sum,(,name,),:by",
      "name",
      Map("cpu" -> 45.0, "disk" -> 450.0)
    )
  }

  test("select: sum by nf.node") {
    basicSelectBy(
      "nf.app,www,:eq,name,cpu,:eq,:and,:sum,(,nf.node,),:by",
      "nf.node",
      (0 until 10).map(i => s"i-$i" -> i.toDouble).toMap
    )
  }

  test("arrayAdd") {
    executeUpdate(SqlUtils.addNaN)
    executeUpdate(SqlUtils.arrayAdd)
    val sql =
      """
      select atlas_array_add(ARRAY[1.0, 2.0, null], ARRAY[3.0, null, null])
      """
    assertEquals(arrayQuery(sql), list(4.0, 2.0, null))
  }

  test("aggrSum") {
    executeUpdate(SqlUtils.addNaN)
    executeUpdate(SqlUtils.arrayAdd)
    executeUpdate(SqlUtils.aggrSum)
    val sql =
      """
      select atlas_aggr_sum(vs)
      from (values
        (ARRAY[1.0, 2.0, null]),
        (ARRAY[3.0, null, null]),
        (ARRAY[4.0, 5.0, null])
      ) as t(vs)
      """
    assertEquals(arrayQuery(sql), list(8.0, 7.0, null))
  }

  test("aggrSum, state should not be preserved") {
    executeUpdate(SqlUtils.addNaN)
    executeUpdate(SqlUtils.arrayAdd)
    executeUpdate(SqlUtils.aggrSum)
    val sql =
      """
      (
        select atlas_aggr_sum(vs)
        from (values
          (ARRAY[1.0, 2.0, null]),
          (ARRAY[3.0, null, null]),
          (ARRAY[4.0, 5.0, null])
        ) as a(vs)
      ) union all (
        select atlas_aggr_sum(vs)
        from (values
          (ARRAY[null::float8, null, null])
        ) as b(vs)
      )
      """
    service.runQueries { stmt =>
      val rs = stmt.executeQuery(sql)
      assert(rs.next())
      val vs1 = rs
        .getArray(1)
        .getArray
        .asInstanceOf[Array[java.lang.Double]]
        .toList
      assertEquals(vs1, list(8.0, 7.0, null))
      assert(rs.next())
      val vs2 = rs
        .getArray(1)
        .getArray
        .asInstanceOf[Array[java.lang.Double]]
        .toList
      assertEquals(vs2, List(null, null, null))
      assert(!rs.next())
    }
  }

  test("aggrCount") {
    executeUpdate(SqlUtils.arrayCount)
    executeUpdate(SqlUtils.arrayZeroToNull)
    executeUpdate(SqlUtils.aggrCount)
    val sql =
      """
      select atlas_aggr_count(vs)
      from (values
        (ARRAY[1.0, 2.0, null]),
        (ARRAY[3.0, null, null]),
        (ARRAY[4.0, 5.0, null])
      ) as t(vs)
      """
    assertEquals(arrayQuery(sql), list(3.0, 2.0, null))
  }

  test("aggrMax") {
    executeUpdate(SqlUtils.maxNaN)
    executeUpdate(SqlUtils.arrayMax)
    executeUpdate(SqlUtils.aggrMax)
    val sql =
      """
      select atlas_aggr_max(vs)
      from (values
        (ARRAY[1.0, 2.0, null]),
        (ARRAY[3.0, null, null]),
        (ARRAY[4.0, 5.0, null])
      ) as t(vs)
      """
    assertEquals(arrayQuery(sql), list(4.0, 5.0, null))
  }

  test("aggrMin") {
    executeUpdate(SqlUtils.minNaN)
    executeUpdate(SqlUtils.arrayMin)
    executeUpdate(SqlUtils.aggrMin)
    val sql =
      """
      select atlas_aggr_min(vs)
      from (values
        (ARRAY[1.0, 2.0, null]),
        (ARRAY[3.0, null, null]),
        (ARRAY[4.0, 5.0, null])
      ) as t(vs)
      """
    assertEquals(arrayQuery(sql), list(1.0, 2.0, null))
  }
}

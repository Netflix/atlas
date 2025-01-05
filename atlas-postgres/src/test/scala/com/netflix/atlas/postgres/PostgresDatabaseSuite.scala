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

import com.netflix.atlas.core.model.ArrayTimeSeq
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.EvalContext
import com.netflix.atlas.core.model.Query
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import java.time.Duration
import java.time.Instant

class PostgresDatabaseSuite extends PostgresSuite {

  private val time = Instant.ofEpochMilli(1647892800000L)
  private val step = 60_000

  override def baseConfig: Config = ConfigFactory.parseString(
    """
      |atlas.postgres {
      |  block-size = 2
      |  step = 60s
      |  tables = [
      |    {
      |      metric-name = "requests"
      |      table-name = "requests"
      |      columns = ["nf.app", "nf.node"]
      |      column-type = "varchar(120)"
      |    }
      |    {
      |      metric-name = "*"
      |      table-name = "others"
      |      columns = ["name"]
      |      column-type = "varchar(255)"
      |    }
      |  ]
      |}
      |""".stripMargin
  )

  private def populateRequestsTable(suffix: String): Unit = {
    (0 until 10).foreach { i =>
      executeUpdate(s"""
           |insert into atlas.requests_$suffix
           |values (ARRAY[$i, $i], '"name"=>"requests"', 'www', 'i-$i')
           |""".stripMargin)
      val d = i * 10
      executeUpdate(s"""
           |insert into atlas.requests_$suffix
           |values (ARRAY[$d, $d], '"name"=>"requests"', 'db', 'i-${i + 10}')
           |""".stripMargin)
    }
  }

  private def populateOthersTable(suffix: String): Unit = {
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
  }

  private def populateSelectTables(): Unit = {
    SqlUtils.customFunctions.foreach(executeUpdate)
    dropTables()
    val blockStep = Duration.ofMinutes(2)
    var blockTime = time
    (0 until 3).foreach { _ =>
      val suffix = SqlUtils.toSuffix(blockTime)
      service.tables.foreach(t => executeUpdate(SqlUtils.createTable(t, blockTime)))
      populateRequestsTable(suffix)
      populateOthersTable(suffix)
      blockTime = blockTime.plus(blockStep)
    }
  }

  private def checkData(
    data: Array[Double],
    size: Int,
    sizeWithData: Int,
    expected: Double
  ): Unit = {
    assertEquals(data.length, size)
    var i = 0
    while (i < size) {
      if (i < sizeWithData)
        assertEquals(data(i), expected, s"position $i")
      else
        assert(data(i).isNaN, s"position $i")
      i += 1
    }
  }

  test("sum") {
    populateSelectTables()
    val db = new PostgresDatabase(service)
    val context = EvalContext(time.toEpochMilli, time.plusSeconds(360).toEpochMilli, step)
    val expr = DataExpr.Sum(Query.Equal("name", "cpu"))
    val results = db.execute(context, expr)

    assertEquals(results.size, 1)
    val ts = results.head
    assertEquals(ts.tags, Map("name" -> "cpu"))
    val seq = ts.data.asInstanceOf[ArrayTimeSeq]
    assertEquals(seq.start, context.start)
    checkData(seq.data, 7, 6, 45.0)
  }

  test("sum, no matching data") {
    populateSelectTables()
    val db = new PostgresDatabase(service)
    val context = EvalContext(time.toEpochMilli, time.plusSeconds(360).toEpochMilli, step)
    val expr = DataExpr.Sum(Query.Equal("name", "foo"))
    val results = db.execute(context, expr)

    assertEquals(results.size, 1)
    val ts = results.head
    assertEquals(ts.tags, Map("name" -> "foo"))
    val seq = ts.data.asInstanceOf[ArrayTimeSeq]
    assertEquals(seq.start, context.start)
    checkData(seq.data, 7, 0, Double.NaN)
  }

  test("sum, no tables for part of range") {
    populateSelectTables()
    val db = new PostgresDatabase(service)
    val context = EvalContext(time.toEpochMilli, time.plusSeconds(600).toEpochMilli, step)
    val expr = DataExpr.Sum(Query.Equal("name", "cpu"))
    val results = db.execute(context, expr)

    assertEquals(results.size, 1)
    val ts = results.head
    assertEquals(ts.tags, Map("name" -> "cpu"))
    val seq = ts.data.asInstanceOf[ArrayTimeSeq]
    assertEquals(seq.start, context.start)
    checkData(seq.data, 11, 6, 45.0)
  }

  test("requests sum") {
    populateSelectTables()
    val db = new PostgresDatabase(service)
    val context = EvalContext(time.toEpochMilli, time.plusSeconds(360).toEpochMilli, step)
    val expr = DataExpr.Sum(Query.Equal("name", "requests"))
    val results = db.execute(context, expr)

    assertEquals(results.size, 1)
    val ts = results.head
    assertEquals(ts.tags, Map("name" -> "requests"))
    val seq = ts.data.asInstanceOf[ArrayTimeSeq]
    assertEquals(seq.start, context.start)
    checkData(seq.data, 7, 6, 495.0)
  }

  test("requests sum db only") {
    populateSelectTables()
    val db = new PostgresDatabase(service)
    val context = EvalContext(time.toEpochMilli, time.plusSeconds(360).toEpochMilli, step)
    val expr = DataExpr.Sum(Query.Equal("name", "requests").and(Query.Equal("nf.app", "db")))
    val results = db.execute(context, expr)

    assertEquals(results.size, 1)
    val ts = results.head
    assertEquals(ts.tags, Map("name" -> "requests", "nf.app" -> "db"))
    val seq = ts.data.asInstanceOf[ArrayTimeSeq]
    assertEquals(seq.start, context.start)
    checkData(seq.data, 7, 6, 450.0)
  }

  test("requests sum group by app") {
    populateSelectTables()
    val db = new PostgresDatabase(service)
    val context = EvalContext(time.toEpochMilli, time.plusSeconds(360).toEpochMilli, step)
    val expr = DataExpr.GroupBy(DataExpr.Sum(Query.Equal("name", "requests")), List("nf.app"))
    val results = db.execute(context, expr)

    assertEquals(results.size, 2)
    results.foreach { ts =>
      val app = ts.tags("nf.app")
      assertEquals(ts.tags, Map("name" -> "requests", "nf.app" -> app))
      val seq = ts.data.asInstanceOf[ArrayTimeSeq]
      assertEquals(seq.start, context.start)
      checkData(seq.data, 7, 6, if (app == "db") 450.0 else 45.0)
    }
  }

  test("requests sum with cpu") {
    populateSelectTables()
    val db = new PostgresDatabase(service)
    val context = EvalContext(time.toEpochMilli, time.plusSeconds(360).toEpochMilli, step)
    val expr =
      DataExpr.Sum(Query.In("name", List("requests", "cpu")).and(Query.Equal("nf.app", "www")))
    val results = db.execute(context, expr)

    assertEquals(results.size, 1)
    val ts = results.head
    assertEquals(ts.tags, Map("nf.app" -> "www"))
    val seq = ts.data.asInstanceOf[ArrayTimeSeq]
    assertEquals(seq.start, context.start)
    checkData(seq.data, 7, 6, 90.0)
  }

  test("requests sum consolidated") {
    populateSelectTables()
    val db = new PostgresDatabase(service)
    val context = EvalContext(time.toEpochMilli, time.plusSeconds(360).toEpochMilli, 2 * step)
    val expr =
      DataExpr.Sum(Query.In("name", List("requests", "cpu")).and(Query.Equal("nf.app", "www")))
    val results = db.execute(context, expr)

    assertEquals(results.size, 1)
    val ts = results.head
    assertEquals(ts.tags, Map("nf.app" -> "www"))
    val seq = ts.data.bounded(context.start, context.end)
    assertEquals(seq.start, context.start)
    checkData(seq.data, 3, 3, 90.0)
  }
}

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
import com.netflix.atlas.core.model.Query
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import java.time.Instant

class PostgresTagIndexSuite extends PostgresSuite {

  private val time = Instant.ofEpochMilli(1647892800000L)
  private val suffix = SqlUtils.toSuffix(time)

  override def baseConfig: Config = ConfigFactory.parseString(
    """
      |atlas.postgres {
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

  private def populateRequestsTable(): Unit = {
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

  private def populateOthersTable(): Unit = {
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
    service.tables.foreach(t => executeUpdate(SqlUtils.createTable(t, time)))
    populateRequestsTable()
    populateOthersTable()
  }

  test("findKeys") {
    populateSelectTables()
    val index = new PostgresTagIndex(service)
    val keys = index.findKeys(TagQuery(None, None))
    val expected = List("id", "name", "nf.app", "nf.node")
    assertEquals(keys, expected)
  }

  test("findKeys: cpu") {
    populateSelectTables()
    val index = new PostgresTagIndex(service)
    val keys = index.findKeys(TagQuery(Some(Query.Equal("name", "cpu")), None))
    val expected = List("name", "nf.app", "nf.node")
    assertEquals(keys, expected)
  }

  test("findKeys: disk") {
    populateSelectTables()
    val index = new PostgresTagIndex(service)
    val keys = index.findKeys(TagQuery(Some(Query.Equal("name", "disk")), None))
    val expected = List("id", "name", "nf.app", "nf.node")
    assertEquals(keys, expected)
  }

  test("findKeys: requests") {
    populateSelectTables()
    val index = new PostgresTagIndex(service)
    val keys = index.findKeys(TagQuery(Some(Query.Equal("name", "requests")), None))
    val expected = List("name", "nf.app", "nf.node")
    assertEquals(keys, expected)
  }

  test("findValues: name") {
    populateSelectTables()
    val index = new PostgresTagIndex(service)
    val keys = index.findValues(TagQuery(None, Some("name")))
    val expected = List("cpu", "disk", "requests")
    assertEquals(keys, expected)
  }

  test("findValues: nf.app") {
    populateSelectTables()
    val index = new PostgresTagIndex(service)
    val keys = index.findValues(TagQuery(None, Some("nf.app")))
    val expected = List("db", "www")
    assertEquals(keys, expected)
  }

  test("findValues: nf.node") {
    populateSelectTables()
    val index = new PostgresTagIndex(service)
    val keys = index.findValues(TagQuery(None, Some("nf.node")))
    val expected = (0 until 20).map(i => s"i-$i").toList.sorted
    assertEquals(keys, expected)
  }

  test("findValues: nf.node for cpu") {
    populateSelectTables()
    val index = new PostgresTagIndex(service)
    val keys = index.findValues(TagQuery(Some(Query.Equal("name", "cpu")), Some("nf.node")))
    val expected = (0 until 10).map(i => s"i-$i").toList.sorted
    assertEquals(keys, expected)
  }
}

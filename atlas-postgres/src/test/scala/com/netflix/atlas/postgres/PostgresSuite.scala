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

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import munit.FunSuite

abstract class PostgresSuite extends FunSuite {

  private var postgres: EmbeddedPostgres = _
  var service: PostgresService = _

  def baseConfig: Config = {
    ConfigFactory.empty()
  }

  override def beforeAll(): Unit = {
    postgres = EmbeddedPostgres
      .builder()
      .setCleanDataDirectory(true)
      .setPort(54321)
      .start()

    val dbConfig = ConfigFactory.parseString("""
        |atlas.postgres {
        |  driver = "org.postgresql.Driver"
        |  url = "jdbc:postgresql://localhost:54321/postgres"
        |  user = "postgres"
        |  password = "postgres"
        |  init-statements = ["create extension if not exists hstore"]
        |  tables = [
        |    {
        |      metric-name = "*"
        |      table-name = "others"
        |      columns = ["name", "atlas.dstype"]
        |    }
        |  ]
        |}
        |""".stripMargin)
    val config = baseConfig.withFallback(dbConfig).resolve()
    service = new PostgresService(config)
    service.start()

    executeUpdate(SqlUtils.createSchema)
  }

  override def afterAll(): Unit = {
    service.stop()
    postgres.close()
  }

  def executeUpdate(sql: String): Unit = {
    service.runQueries { stmt =>
      try stmt.executeUpdate(sql)
      catch {
        case e: Exception => throw new IllegalStateException(s"failed update: $sql", e)
      }
    }
  }

  def dropTables(): Unit = {
    service.runQueries { stmt =>
      val ts = List.newBuilder[String]
      val rs = stmt.executeQuery(SqlUtils.listTables)
      while (rs.next()) {
        ts += rs.getString(1)
      }
      ts.result().foreach { table =>
        executeUpdate(s"drop table atlas.$table")
      }
    }
  }
}

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

import com.netflix.iep.service.AbstractService
import com.typesafe.config.Config
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection

import java.sql.Connection
import java.sql.Statement
import scala.util.Using

/**
 * Manage connections to postgres database.
 */
class PostgresService(val config: Config) extends AbstractService {

  val tables: List[TableDefinition] = {
    import scala.jdk.CollectionConverters.*
    config
      .getConfigList("atlas.postgres.tables")
      .asScala
      .toList
      .map(TableDefinition.fromConfig)
  }

  private var ds: HikariDataSource = _

  override def startImpl(): Unit = {
    val c = config.getConfig("atlas.postgres")
    Class.forName(c.getString("driver"))
    val hc = new HikariConfig()
    hc.setJdbcUrl(c.getString("url"))
    hc.setUsername(c.getString("user"))
    hc.setPassword(c.getString("password"))
    ds = new HikariDataSource(hc)

    Using.resource(ds.getConnection) { connection =>
      // Ensure DB is appropriately configured
      Using.resource(connection.createStatement()) { stmt =>
        // Run init statements from config
        c.getStringList("init-statements").forEach { sql =>
          stmt.executeUpdate(sql)
        }

        // Setup helper functions
        SqlUtils.customFunctions.foreach { sql =>
          stmt.executeUpdate(sql)
        }
      }
    }
  }

  override def stopImpl(): Unit = {
    ds.close()
  }

  def getConnection: Connection = {
    ds.getConnection
  }

  def getCopyManager: CopyManager = {
    getConnection.unwrap(classOf[BaseConnection]).getCopyAPI
  }

  def runQueries[T](f: Statement => T): T = {
    Using.resource(getConnection) { connection =>
      Using.resource(connection.createStatement())(f)
    }
  }
}

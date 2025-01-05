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

import com.typesafe.config.ConfigFactory
import munit.FunSuite

class TableDefinitionSuite extends FunSuite {

  test("load from config") {
    val config = ConfigFactory.parseString("""
        |metric-name = "ipc.server.call"
        |table-name = "ipc_server_call"
        |columns = ["nf.app", "ipc.client.app"]
        |column-type = "varchar(120)"
        |""".stripMargin)
    val table = TableDefinition.fromConfig(config)
    val expected = TableDefinition(
      "ipc.server.call",
      "ipc_server_call",
      List("nf.app", "ipc.client.app"),
      "varchar(120)"
    )
    assertEquals(table, expected)
  }

  test("load from config, default column type") {
    val config = ConfigFactory.parseString("""
        |metric-name = "ipc.server.call"
        |table-name = "ipc_server_call"
        |columns = ["nf.app", "ipc.client.app"]
        |""".stripMargin)
    val table = TableDefinition.fromConfig(config)
    val expected = TableDefinition(
      "ipc.server.call",
      "ipc_server_call",
      List("nf.app", "ipc.client.app"),
      "varchar(255)"
    )
    assertEquals(table, expected)
  }
}

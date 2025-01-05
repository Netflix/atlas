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

import com.netflix.atlas.core.index.TagIndex
import com.netflix.atlas.core.index.TagQuery
import com.netflix.atlas.core.model.Tag
import com.netflix.atlas.core.model.TaggedItem

import java.sql.Statement
import java.time.Instant
import scala.util.Using

/** Lookups are delegated to PostgreSQL. Some operations like `size` are not supported. */
class PostgresTagIndex(postgres: PostgresService) extends TagIndex[TaggedItem] {

  override def findTags(query: TagQuery): List[Tag] = {
    val k = query.key.get
    findValues(query).map(v => Tag(k, v))
  }

  private def times(stmt: Statement): List[Instant] = {
    val ts = List.newBuilder[Instant]
    val rs = stmt.executeQuery(SqlUtils.listTables)
    while (rs.next()) {
      SqlUtils.extractTime(rs.getString(1)).foreach(ts.addOne)
    }
    ts.result().distinct
  }

  override def findKeys(query: TagQuery): List[String] = {
    Using.resource(postgres.getConnection) { connection =>
      Using.resource(connection.createStatement()) { stmt =>
        val queries = times(stmt).flatMap { t =>
          SqlUtils.keyQueries(t, postgres.tables, query)
        }
        if (queries.isEmpty) {
          Nil
        } else {
          val q = SqlUtils.union(queries)
          val vs = List.newBuilder[String]
          val rs = stmt.executeQuery(q)
          while (rs.next()) {
            vs += rs.getString(1)
          }
          vs.result().sorted.take(query.limit)
        }
      }
    }
  }

  override def findValues(query: TagQuery): List[String] = {
    Using.resource(postgres.getConnection) { connection =>
      Using.resource(connection.createStatement()) { stmt =>
        val queries = times(stmt).flatMap { t =>
          SqlUtils.valueQueries(t, postgres.tables, query)
        }
        if (queries.isEmpty) {
          Nil
        } else {
          val q = SqlUtils.union(queries)
          val vs = List.newBuilder[String]
          val rs = stmt.executeQuery(q)
          while (rs.next()) {
            vs += rs.getString(1)
          }
          vs.result().sorted.take(query.limit)
        }
      }
    }
  }

  override def findItems(query: TagQuery): List[TaggedItem] = {
    throw new UnsupportedOperationException("findItems")
  }

  override def iterator: Iterator[TaggedItem] = {
    throw new UnsupportedOperationException("iterator")
  }

  override def size: Int = 0
}

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

import com.netflix.atlas.core.db.Database
import com.netflix.atlas.core.db.TimeSeriesBuffer
import com.netflix.atlas.core.index.TagIndex
import com.netflix.atlas.core.model.ArrayBlock
import com.netflix.atlas.core.model.DataExpr
import com.netflix.atlas.core.model.EvalContext
import com.netflix.atlas.core.model.Query
import com.netflix.atlas.core.model.TaggedItem
import com.netflix.atlas.core.model.TimeSeries

import java.sql.ResultSet
import java.sql.Statement
import java.time.Instant
import java.util
import scala.collection.mutable
import scala.util.Using

/**
 * Database implementation that delegates to PostgreSQL with the blocks stored as rows in the
 * time based tables. This class only handles queries, the data loading would be managed separately
 * and is more specific to the backing long term storage, e.g. S3.
 */
class PostgresDatabase(postgres: PostgresService) extends Database {

  private val blockSize: Int = postgres.config.getInt("atlas.postgres.block-size")
  private val step: Long = postgres.config.getDuration("atlas.postgres.step").toMillis

  private val blockDuration = blockSize * step / 1000

  override val index: TagIndex[? <: TaggedItem] = {
    new PostgresTagIndex(postgres)
  }

  private def overlappingTimes(stmt: Statement, ctxt: EvalContext): List[Instant] = {
    val interval = Interval(ctxt.start, ctxt.end)
    val ts = List.newBuilder[Instant]
    val rs = stmt.executeQuery(SqlUtils.listTables)
    while (rs.next()) {
      SqlUtils.extractTime(rs.getString(1)).foreach { t =>
        if (interval.overlaps(Interval(t, t.plusSeconds(blockDuration)))) {
          ts += t
        }
      }
    }
    ts.result().distinct
  }

  private def extractTags(expr: DataExpr, rs: ResultSet): Map[String, String] = {
    expr.finalGrouping.map { k =>
      k -> rs.getString(k)
    }.toMap
  }

  private def copyValues(rs: ResultSet, block: ArrayBlock): Unit = {
    val sqlArray = rs.getArray("values")
    if (sqlArray != null) {
      val array = sqlArray.getArray.asInstanceOf[Array[java.lang.Double]]
      var i = 0
      while (i < array.length) {
        block.buffer(i) = array(i).doubleValue()
        i += 1
      }
    } else {
      // Avoid carrying state over if block data is not overwritten
      util.Arrays.fill(block.buffer, Double.NaN)
    }
  }

  @scala.annotation.tailrec
  private def aggr(expr: DataExpr, buffer: TimeSeriesBuffer, block: ArrayBlock): Unit = {
    expr match {
      case _: DataExpr.Sum               => buffer.add(block)
      case _: DataExpr.Count             => buffer.add(block)
      case _: DataExpr.Min               => buffer.min(block)
      case _: DataExpr.Max               => buffer.max(block)
      case DataExpr.Consolidation(af, _) => aggr(af, buffer, block)
      case DataExpr.GroupBy(af, _)       => aggr(af, buffer, block)
      case e                             => throw new MatchError(s"unsupported DataExpr: $e")
    }
  }

  override def execute(ctxt: EvalContext, expr: DataExpr): List[TimeSeries] = {
    val exactTags = Query.tags(expr.query)
    val data = mutable.HashMap.empty[Map[String, String], TimeSeriesBuffer]
    postgres.runQueries { stmt =>
      overlappingTimes(stmt, ctxt).foreach { t =>
        val block = ArrayBlock(t.toEpochMilli, blockSize)
        val queries = SqlUtils.dataQueries(t, postgres.tables, expr)
        val q = SqlUtils.unionAll(queries)
        Using.resource(stmt.executeQuery(q)) { rs =>
          while (rs.next()) {
            val tags = exactTags ++ extractTags(expr, rs)
            // The buffer step size always matches the storage for now. This can be optimized once
            // the aggegates pushed down to postgres support consolidation while performing the
            // aggregation.
            val buffer = data.getOrElseUpdate(
              tags,
              TimeSeriesBuffer(tags, step, ctxt.start, ctxt.end)
            )
            copyValues(rs, block)
            aggr(expr, buffer, block)
          }
        }
      }
    }

    // Consolidate the final result set if needed
    if (ctxt.step == step)
      data.values.toList
    else
      data.values.map(_.consolidate(ctxt.step, expr.cf)).toList
  }
}

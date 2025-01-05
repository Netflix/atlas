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
import com.netflix.atlas.core.model.Query
import com.netflix.spectator.impl.PatternMatcher

import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

/** Utilities for working with tables. */
object SqlUtils {

  private val schema = "atlas"

  private val TableNamePattern = "^.*_([0-9]{12})$".r

  private val suffixFormatter = DateTimeFormatter
    .ofPattern("yyyyMMddHHmm")
    .withZone(ZoneOffset.UTC)

  /** Convert the time to a suffix string that will be used on the table name. */
  def toSuffix(time: Instant): String = {
    suffixFormatter.format(time)
  }

  /** Extract the time based on the suffix for the table name. */
  def extractTime(table: String): Option[Instant] = {
    table match {
      case TableNamePattern(suffix) => Some(parseInstant(suffix))
      case _                        => None
    }
  }

  private def parseInstant(suffix: String): Instant = {
    LocalDateTime.parse(suffix, suffixFormatter).toInstant(ZoneOffset.UTC)
  }

  def createSchema: String = {
    s"create schema if not exists $schema"
  }

  def listTables: String = {
    s"""
    select table_name
      from information_schema.tables
     where table_schema = '$schema'
       and table_type = 'BASE TABLE'
    """
  }

  /**
   * Create a table if not already present.
   *
   * @param config
   *     Configuration for the table schema.
   * @param time
   *     Time suffix to use for the table. Typically tables will be used for a range
   *     of time and then deleted entirely when past the retention window.
   */
  def createTable(config: TableDefinition, time: Instant): String = {
    val suffix = toSuffix(time)
    val tableName = s"${config.tableName}_$suffix"
    val columns =
      "values float8[]" :: "tags hstore" :: config.columns.map(c => s"\"$c\" ${config.columnType}")
    val columnsStr = columns.mkString(", ")
    s"create table if not exists $schema.$tableName($columnsStr)"
  }

  /**
   * Return a set of key queries for an ASL query expression.
   *
   * @param time
   *     Timestamp of interest for the current context.
   * @param tables
   *     Set of tables defined.
   * @param tq
   *     Tag query to map to SQL.
   * @return
   *     Set of SQL queries to lookup the keys.
   */
  def keyQueries(time: Instant, tables: List[TableDefinition], tq: TagQuery): List[String] = {
    val suffix = toSuffix(time)
    val query = tq.query.getOrElse(Query.True)

    tables
      .filter { table =>
        query.couldMatch(table.tags)
      }
      .map { table =>
        val cs = table.columns.map(c => s"('${escapeLiteral(c)}')").mkString(", ")
        val limit = tq.limit
        val offset = s"key > '${escapeLiteral(tq.offset)}'"
        s"""
        select distinct vs.key as key
          from ((
            select (each(tags)).key as key
              from $schema.${table.tableName}_$suffix
             where ${toWhere(table.columns, query)}
          ) union (values
            $cs
          )) as vs
         where $offset
         order by key
         limit $limit
        """
      }
  }

  /**
   * Return a set of value queries for an ASL query expression.
   *
   * @param time
   *     Timestamp of interest for the current context.
   * @param tables
   *     Set of tables defined.
   * @param tq
   *     Tag query to map to SQL.
   * @return
   *     Set of SQL queries to lookup the values.
   */
  def valueQueries(time: Instant, tables: List[TableDefinition], tq: TagQuery): List[String] = {
    require(tq.key.isDefined)

    val suffix = toSuffix(time)
    val key = tq.key.get
    val query = tq.query.getOrElse(Query.True)

    tables
      .filter { table =>
        query.couldMatch(table.tags)
      }
      .map { table =>
        val column = formatColumn(table.columns, key)
        val limit = tq.limit
        val offset = s"and $column > '${escapeLiteral(tq.offset)}'"
        s"""
        select distinct $column as "${escapeLiteral(key)}"
          from $schema.${table.tableName}_$suffix
         where ${toWhere(table.columns, query)} $offset
         order by $column
         limit $limit
        """
      }
  }

  /**
   * Return a set of data queries for an ASL expression.
   *
   * @param time
   *     Timestamp of interest for the current context.
   * @param tables
   *     Set of tables defined.
   * @param expr
   *     Tag query to map to SQL.
   * @return
   *     Set of SQL queries to evaluate a data expression.
   */
  def dataQueries(time: Instant, tables: List[TableDefinition], expr: DataExpr): List[String] = {
    val suffix = toSuffix(time)
    tables
      .filter { table =>
        expr.query.couldMatch(table.tags)
      }
      .map { table =>
        if (expr.isGrouped) {
          val cs = expr.finalGrouping
            .map(c => formatColumn(table.columns, c))
          val selectColumns = cs
            .zip(expr.finalGrouping)
            .map {
              case (column, label) => s"$column as \"${escapeLiteral(label)}\""
            }
            .mkString(", ")
          val groupByColumns = cs.mkString(", ")
          s"""
          select $selectColumns, ${toAggr(expr)}
            from $schema.${table.tableName}_$suffix
           where ${toWhere(table.columns, expr.query)}
           group by $groupByColumns
          """
        } else {
          s"""
          select ${toAggr(expr)}
            from $schema.${table.tableName}_$suffix
           where ${toWhere(table.columns, expr.query)}
          """
        }
      }
  }

  @scala.annotation.tailrec
  private def toAggr(expr: DataExpr): String = {
    expr match {
      case _: DataExpr.Sum               => "atlas_aggr_sum(values) as values"
      case _: DataExpr.Count             => "atlas_aggr_count(values) as values"
      case _: DataExpr.Max               => "atlas_aggr_max(values) as values"
      case _: DataExpr.Min               => "atlas_aggr_min(values) as values"
      case DataExpr.GroupBy(af, _)       => toAggr(af)
      case DataExpr.Consolidation(af, _) => toAggr(af)
      case e: DataExpr.All               => throw new MatchError(s"unsupported DataExpr: $e")
    }
  }

  private def toWhere(columns: List[String], query: Query): String = {
    query match {
      case Query.True              => "TRUE"
      case Query.False             => "FALSE"
      case Query.And(q1, q2)       => s"(${toWhere(columns, q1)}) and (${toWhere(columns, q2)})"
      case Query.Or(q1, q2)        => s"(${toWhere(columns, q1)}) or (${toWhere(columns, q2)})"
      case Query.Not(q)            => s"not (${toWhere(columns, q)})"
      case Query.HasKey(k)         => s"${formatColumn(columns, k)} is not null"
      case Query.Equal(k, v)       => s"${formatColumn(columns, k)} = '${escapeLiteral(v)}'"
      case Query.GreaterThan(k, v) => s"${formatColumn(columns, k)} > '${escapeLiteral(v)}'"
      case Query.GreaterThanEqual(k, v) => s"${formatColumn(columns, k)} >= '${escapeLiteral(v)}'"
      case Query.LessThan(k, v)         => s"${formatColumn(columns, k)} < '${escapeLiteral(v)}'"
      case Query.LessThanEqual(k, v)    => s"${formatColumn(columns, k)} <= '${escapeLiteral(v)}'"
      case r: Query.Regex               => toRegexCondition(columns, r.k, r.pattern)
      case r: Query.RegexIgnoreCase     => toRegexIgnoreCaseCondition(columns, r.k, r.pattern)
      case Query.In(k, vs)              => toInCondition(columns, k, vs)
    }
  }

  private def toRegexCondition(columns: List[String], k: String, p: PatternMatcher): String = {
    val likePattern = p.toSqlPattern
    if (likePattern != null)
      s"${formatColumn(columns, k)} like '${escapeLiteral(likePattern)}'"
    else
      s"${formatColumn(columns, k)} ~ '${escapeLiteral(p.toString)}'"
  }

  private def toRegexIgnoreCaseCondition(
    columns: List[String],
    k: String,
    p: PatternMatcher
  ): String = {
    s"${formatColumn(columns, k)} ~* '${escapeLiteral(p.toString)}'"
  }

  private def toInCondition(columns: List[String], k: String, vs: List[String]): String = {
    s"${formatColumn(columns, k)} in ('${vs.map(escapeLiteral).mkString("', '")}')"
  }

  private def formatColumn(columns: List[String], k: String): String = {
    if (columns.contains(k))
      s"\"${escapeLiteral(k)}\""
    else
      s"tags -> '${escapeLiteral(k)}'"
  }

  private[postgres] def escapeLiteral(str: String): String = {
    val buf = new java.lang.StringBuilder(str.length)
    org.postgresql.core.Utils.escapeLiteral(buf, str, false)
    buf.toString
  }

  /** Add operation to use as part of sum aggregation. */
  def addNaN: String = {
    """
    create or replace function atlas_add(a float8, b float8) returns float8 as $$
    begin
      -- We use null to stand in for NaN. Treats NaN as 0 if the other value is not NaN.
      if a is null then
        return b;
      elsif b is null then
        return a;
      else
        return a + b;
      end if;
    end;
    $$ language plpgsql;
    """
  }

  /** Max operation to use as part of aggregation. */
  def maxNaN: String = {
    """
    create or replace function atlas_max(a float8, b float8) returns float8 as $$
    begin
      -- We use null to stand in for NaN. Treats NaN as 0 if the other value is not NaN.
      if a is null then
        return b;
      elsif b is null then
        return a;
      elsif a >= b then
        return a;
      else
        return b;
      end if;
    end;
    $$ language plpgsql;
    """
  }

  /** Min operation to use as part of aggregation. */
  def minNaN: String = {
    """
    create or replace function atlas_min(a float8, b float8) returns float8 as $$
    begin
      -- We use null to stand in for NaN. Treats NaN as 0 if the other value is not NaN.
      if a is null then
        return b;
      elsif b is null then
        return a;
      elsif a <= b then
        return a;
      else
        return b;
      end if;
    end;
    $$ language plpgsql;
    """
  }

  /** Add corresponding elements of two arrays. */
  def arrayAdd: String = {
    """
    create or replace function atlas_array_add(float8[], float8[]) returns float8[] as $$
    declare
      i int;
    begin
      -- Verify arrays have the same length
      if array_length($1, 1) != array_length($2, 1) then
        raise exception 'arrays must have the same length';
      end if;

      -- First array is used as the accumulator
      for i in 1..array_upper($2, 1) loop
        $1[i] := atlas_add($1[i], $2[i]);
      end loop;
      return $1;
    end;
    $$ language plpgsql;
    """
  }

  /** Count corresponding elements of two arrays. */
  def arrayCount: String = {
    """
    create or replace function atlas_array_count(float8[], float8[]) returns float8[] as $$
    declare
      acc float8[];
      i int;
    begin
      -- For first value, initialize the accumulator to 0.0
      acc := $1;
      if array_length($1, 1) is null then
        acc := array_fill(0.0, ARRAY[array_length($2, 1)]);
      end if;

      -- Verify arrays have the same length
      if array_length(acc, 1) != array_length($2, 1) then
        raise exception 'arrays must have the same length';
      end if;

      -- First array is used as the accumulator
      for i in 1..array_upper($2, 1) loop
        if $2[i] is not null then
          acc[i] := acc[i] + 1.0;
        end if;
      end loop;
      return acc;
    end;
    $$ language plpgsql;
    """
  }

  /** Compute max of corresponding elements of two arrays. */
  def arrayMax: String = {
    """
    create or replace function atlas_array_max(float8[], float8[]) returns float8[] as $$
    declare
      i int;
    begin
      -- Verify arrays have the same length
      if array_length($1, 1) != array_length($2, 1) then
        raise exception 'arrays must have the same length';
      end if;

      -- First array is used as the accumulator
      for i in 1..array_upper($2, 1) loop
        $1[i] := atlas_max($1[i], $2[i]);
      end loop;
      return $1;
    end;
    $$ language plpgsql;
    """
  }

  /** Compute min of corresponding elements of two arrays. */
  def arrayMin: String = {
    """
    create or replace function atlas_array_min(float8[], float8[]) returns float8[] as $$
    declare
      i int;
    begin
      -- Verify arrays have the same length
      if array_length($1, 1) != array_length($2, 1) then
        raise exception 'arrays must have the same length';
      end if;

      -- First array is used as the accumulator
      for i in 1..array_upper($2, 1) loop
        $1[i] := atlas_min($1[i], $2[i]);
      end loop;
      return $1;
    end;
    $$ language plpgsql;
    """
  }

  /** Convert zero values in an array to nulls. Used with count aggregation. */
  def arrayZeroToNull: String = {
    """
    create or replace function atlas_array_ztn(float8[]) returns float8[] as $$
    declare
      i int;
    begin
      for i in 1..array_upper($1, 1) loop
        if $1[i] = 0.0 then
          $1[i] := null;
        end if;
      end loop;
      return $1;
    end;
    $$ language plpgsql;
    """
  }

  /** Sum aggregation for an array of values. */
  def aggrSum: String = {
    """
    create or replace aggregate atlas_aggr_sum(float8[]) (
      sfunc = atlas_array_add,
      stype = float8[]
    )
    """
  }

  /** Count aggregation for an array of values. */
  def aggrCount: String = {
    """
    create or replace aggregate atlas_aggr_count(float8[]) (
      sfunc = atlas_array_count,
      stype = float8[],
      initcond = '{}',
      finalfunc = atlas_array_ztn
    )
    """
  }

  /** Max aggregation for an array of values. */
  def aggrMax: String = {
    """
    create or replace aggregate atlas_aggr_max(float8[]) (
      sfunc = atlas_array_max,
      stype = float8[]
    )
    """
  }

  /** Min aggregation for an array of values. */
  def aggrMin: String = {
    """
    create or replace aggregate atlas_aggr_min(float8[]) (
      sfunc = atlas_array_min,
      stype = float8[]
    )
    """
  }

  /** Set of custom functions to simplify usage. */
  def customFunctions: List[String] = List(
    addNaN,
    maxNaN,
    minNaN,
    arrayAdd,
    arrayCount,
    arrayMax,
    arrayMin,
    arrayZeroToNull,
    aggrSum,
    aggrCount,
    aggrMax,
    aggrMin
  )

  def union(queries: List[String]): String = {
    queries.mkString("(", ") union (", ")")
  }

  def unionAll(queries: List[String]): String = {
    queries.mkString("(", ") union all (", ")")
  }
}

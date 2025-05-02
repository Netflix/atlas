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
package com.netflix.atlas.core.model

import com.netflix.atlas.core.stacklang.Interpreter

/** Base type for event expressions. */
sealed trait EventExpr extends Expr {

  /** Query to determine if an event should be matched. */
  def query: Query
}

object EventExpr {

  /**
    * Specifies to just pass through the raw event if they match the query.
    *
    * @param query
    *     Query to determine if an event should be matched.
    */
  case class Raw(query: Query) extends EventExpr {

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, query)
    }
  }

  /**
    * Expression that specifies how to map an event to a simple row with the specified columns.
    *
    * @param query
    *     Query to determine if an event should be matched.
    * @param columns
    *     Set of columns to export into a row.
    */
  case class Table(query: Query, columns: List[String]) extends EventExpr {

    require(columns.nonEmpty, "set of columns cannot be empty")

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, query, columns, Interpreter.WordToken(":table"))
    }
  }

  /**
    * Expression that specifies how to map an event to a simple row with the specified columns.
    *
    * @param query
    *     Query to determine if an event should be matched.
    * @param sampleBy
    *     The set of tag values to extract for purposes of the sampling groups. A value will be
    *     sent for each distinct sample group.
    * @param projectionKeys
    *     Set of columns to export into a row.
    */
  case class Sample(query: Query, sampleBy: List[String], projectionKeys: List[String])
      extends EventExpr {

    require(sampleBy.nonEmpty, "sampleBy cannot be empty")

    override def append(builder: java.lang.StringBuilder): Unit = {
      Interpreter.append(builder, query, sampleBy, projectionKeys, Interpreter.WordToken(":sample"))
    }

    def dataExpr: DataExpr = {
      DataExpr.GroupBy(DataExpr.Sum(query), sampleBy)
    }
  }
}

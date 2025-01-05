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

/**
 * Definition for a custom table.
 *
 * @param metricName
 *     Metric name for the table. A table can be for a specific metric or can use `*` to indicate it is
 *     a generic table for any data.
 * @param tableName
 *     Base name to use for the table.
 * @param columns
 *     Tag keys to split out as separate columns rather than include in tag map.
 * @param columnType
 *     Type to use for the columns.
 */
case class TableDefinition(
  metricName: String,
  tableName: String,
  columns: List[String],
  columnType: String
) {

  def isNameSpecific: Boolean = {
    metricName != "*"
  }

  val tags: Map[String, String] = {
    if (isNameSpecific) Map("name" -> metricName) else Map.empty
  }
}

object TableDefinition {

  def fromConfig(config: Config): TableDefinition = {
    import scala.jdk.CollectionConverters.*
    TableDefinition(
      config.getString("metric-name"),
      config.getString("table-name"),
      config.getStringList("columns").asScala.toList,
      if (config.hasPath("column-type")) config.getString("column-type") else "varchar(255)"
    )
  }
}

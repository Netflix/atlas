/*
 * Copyright 2014-2017 Netflix, Inc.
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
package com.netflix.atlas.wiki.pages

import java.time.ZoneId
import java.time.format.TextStyle
import java.util.Locale

import com.netflix.atlas.wiki.GraphHelper
import com.netflix.atlas.wiki.SimplePage

class TimeZones extends SimplePage {
  override def name: String = "Time-Zones"

  override def content(graph: GraphHelper): String =
    s"""
       |All supported time zone ids.
       |
       |## Common
       |
       |At Netflix the most common ids in use are:
       |
       || Zone Id    | Short Display Name |
       ||------------|--------------------|
       || UTC        | UTC                |
       || US/Pacific | PT                 |
       |
       |## By Zone Id
       |
       |$supportedIds
       |
       |## By Display Name
       |
       |Short display names are used in some views, such as multi-timezone plots. This table
       |provides a reference of the display name to the id.
       |
       |$displayNameToZoneId
    """.stripMargin

  private def supportedIds: String = {
    import scala.collection.JavaConversions._
    val zones = ZoneId.getAvailableZoneIds.toList.sortWith(_ < _).map { id =>
      s"| $id | ${ZoneId.of(id).getDisplayName(TextStyle.SHORT, Locale.US)} |"
    }
    """
      || Zone Id | Short Display Name |
      ||---------|--------------------|
    """.stripMargin + zones.mkString("\n")
  }

  private def displayNameToZoneId: String = {
    import scala.collection.JavaConversions._
    val zones = ZoneId.getAvailableZoneIds.toList
    val byName = zones.groupBy(id => ZoneId.of(id).getDisplayName(TextStyle.SHORT, Locale.US))
    val sorted = byName.toList.sortWith(_._1 < _._1).map { case (name, ids) =>
      s"| $name | ${ids.mkString(", ")} |"
    }
    """
      || Short Display Name | Zone Ids |
      ||---------|--------------------|
    """.stripMargin + sorted.mkString("\n")
  }
}

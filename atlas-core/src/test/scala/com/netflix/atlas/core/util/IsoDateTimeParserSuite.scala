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
package com.netflix.atlas.core.util

import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import munit.FunSuite

class IsoDateTimeParserSuite extends FunSuite {

  private val utcZones = List("Z", "+00", "+0000", "+000000", "+00:00", "+00:00:00")

  private val offsetZones = List("07", "0700", "070000", "07:00", "07:00:00")

  test("date") {
    val t = "2020-07-28"
    val expected = ZonedDateTime.parse(s"${t}T00:00:00Z")
    assertEquals(expected, IsoDateTimeParser.parse(t, ZoneOffset.UTC))
  }

  utcZones.foreach { zone =>
    test(s"date $zone") {
      val t = s"2020-07-28"
      val expected = ZonedDateTime.parse(s"${t}T00:00:00Z")
      assertEquals(expected, IsoDateTimeParser.parse(s"$t$zone", ZoneOffset.UTC))
    }
  }

  offsetZones.foreach { offset =>
    List("-", "+").map(s => s"$s$offset").foreach { zone =>
      test(s"date $zone") {
        val t = s"2020-07-28"
        val z = if (zone.startsWith("-")) "-07:00:00" else "+07:00:00"
        val expected = ZonedDateTime.parse(s"${t}T00:00:00$z")
        assertEquals(expected, IsoDateTimeParser.parse(s"$t$zone", ZoneOffset.UTC))
      }
    }
  }

  test("date time hh:mm") {
    val t = "2020-07-28T21:07"
    val expected = ZonedDateTime.parse(s"$t:00Z")
    assertEquals(expected, IsoDateTimeParser.parse(t, ZoneOffset.UTC))
  }

  utcZones.foreach { zone =>
    test(s"date time hh:mm $zone") {
      val t = s"2020-07-28T21:07"
      val expected = ZonedDateTime.parse(s"$t:00Z")
      assertEquals(expected, IsoDateTimeParser.parse(s"$t$zone", ZoneOffset.UTC))
    }
  }

  offsetZones.foreach { offset =>
    List("-", "+").map(s => s"$s$offset").foreach { zone =>
      test(s"date time hh:mm $zone") {
        val t = s"2020-07-28T21:07"
        val z = if (zone.startsWith("-")) "-07:00:00" else "+07:00:00"
        val expected = ZonedDateTime.parse(s"$t:00$z")
        assertEquals(expected, IsoDateTimeParser.parse(s"$t$zone", ZoneOffset.UTC))
      }
    }
  }

  test("date time hh:mm:ss") {
    val t = "2020-07-28T21:07:56"
    val expected = ZonedDateTime.parse(s"${t}Z")
    assertEquals(expected, IsoDateTimeParser.parse(t, ZoneOffset.UTC))
  }

  utcZones.foreach { zone =>
    test(s"date time hh:mm:ss $zone") {
      val t = s"2020-07-28T21:07:56"
      val expected = ZonedDateTime.parse(s"${t}Z")
      assertEquals(expected, IsoDateTimeParser.parse(s"$t$zone", ZoneOffset.UTC))
    }
  }

  offsetZones.foreach { offset =>
    List("-", "+").map(s => s"$s$offset").foreach { zone =>
      test(s"date time hh:mm:ss $zone") {
        val t = s"2020-07-28T21:07:56"
        val z = if (zone.startsWith("-")) "-07:00:00" else "+07:00:00"
        val expected = ZonedDateTime.parse(s"$t$z")
        assertEquals(expected, IsoDateTimeParser.parse(s"$t$zone", ZoneOffset.UTC))
      }
    }
  }

  test("date time hh:mm:ss.mmm") {
    val t = "2020-07-28T21:07:56.195"
    val expected = ZonedDateTime.parse(s"${t}Z")
    assertEquals(expected, IsoDateTimeParser.parse(t, ZoneOffset.UTC))
  }

  utcZones.foreach { zone =>
    test(s"date time hh:mm:ss.mmm $zone") {
      val t = s"2020-07-28T21:07:56.195"
      val expected = ZonedDateTime.parse(s"${t}Z")
      assertEquals(expected, IsoDateTimeParser.parse(s"$t$zone", ZoneOffset.UTC))
    }
  }

  offsetZones.foreach { offset =>
    List("-", "+").map(s => s"$s$offset").foreach { zone =>
      test(s"date time hh:mm:ss.mmm $zone") {
        val t = s"2020-07-28T21:07:56.195"
        val z = if (zone.startsWith("-")) "-07:00:00" else "+07:00:00"
        val expected = ZonedDateTime.parse(s"$t$z")
        assertEquals(expected, IsoDateTimeParser.parse(s"$t$zone", ZoneOffset.UTC))
      }
    }
  }

  test("date odd zone") {
    val t = "2020-07-28"
    val zone = "+12:34:56"
    val expected = ZonedDateTime.parse(s"${t}T00:00:00$zone")
    assertEquals(expected, IsoDateTimeParser.parse(s"$t$zone", ZoneOffset.UTC))
  }

  test("date default US/Pacific") {
    val t = "2020-07-28"
    val zone = ZoneId.of("US/Pacific")
    val expected =
      ZonedDateTime.parse(s"${t}T00:00:00", DateTimeFormatter.ISO_DATE_TIME.withZone(zone))
    assertEquals(expected, IsoDateTimeParser.parse(t, zone))
  }
}

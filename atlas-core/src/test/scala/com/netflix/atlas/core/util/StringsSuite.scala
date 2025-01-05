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

import java.awt.Color
import java.math.BigInteger
import java.time.Duration
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.Locale
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import munit.FunSuite

class StringsSuite extends FunSuite {

  import com.netflix.atlas.core.util.Strings.*

  test("conversionsString") {
    assert(conversions(classOf[String])("42") == "42")
    assert(conversions(classOf[String])("forty-two") == "forty-two")
  }

  test("conversionsBooleanPrimitive") {
    assert(conversions(classOf[Boolean])("true") == true)
    assert(conversions(classOf[Boolean])("forty-two") == false)
  }

  test("conversionsBytePrimitive") {
    assert(conversions(classOf[Byte])("42") == 42.asInstanceOf[Byte])
    intercept[NumberFormatException] {
      conversions(classOf[Byte])("forty-two")
    }
  }

  test("conversionsShortPrimitive") {
    assert(conversions(classOf[Short])("42") == 42.asInstanceOf[Short])
    intercept[NumberFormatException] {
      conversions(classOf[Short])("forty-two")
    }
  }

  test("conversionsIntPrimitive") {
    assert(conversions(classOf[Int])("42") == 42)
    intercept[NumberFormatException] {
      conversions(classOf[Int])("forty-two")
    }
  }

  test("conversionsLongPrimitive") {
    assert(conversions(classOf[Long])("42") == 42L)
    intercept[NumberFormatException] {
      conversions(classOf[Long])("forty-two")
    }
  }

  test("conversionsFloatPrimitive") {
    assert(conversions(classOf[Float])("42.0") == 42.0f)
    intercept[NumberFormatException] {
      conversions(classOf[Float])("forty-two")
    }
  }

  test("conversionsDoublePrimitive") {
    assert(conversions(classOf[Double])("42.0") == 42.0)
    intercept[NumberFormatException] {
      conversions(classOf[Double])("forty-two")
    }
  }

  test("conversionsBooleanWrapper") {
    assert(conversions(classOf[java.lang.Boolean])("true") == true)
    assert(conversions(classOf[java.lang.Boolean])("forty-two") == false)
  }

  test("conversionsByteWrapper") {
    assert(conversions(classOf[java.lang.Byte])("42") == 42.asInstanceOf[Byte])
    intercept[NumberFormatException] {
      conversions(classOf[java.lang.Byte])("forty-two")
    }
  }

  test("conversionsShortWrapper") {
    assert(conversions(classOf[java.lang.Short])("42") == 42.asInstanceOf[Short])
    intercept[NumberFormatException] {
      conversions(classOf[Short])("forty-two")
    }
  }

  test("conversionsIntWrapper") {
    assert(conversions(classOf[java.lang.Integer])("42") == 42)
    intercept[NumberFormatException] {
      conversions(classOf[Int])("forty-two")
    }
  }

  test("conversionsLongWrapper") {
    assert(conversions(classOf[java.lang.Long])("42") == 42L)
    intercept[NumberFormatException] {
      conversions(classOf[Long])("forty-two")
    }
  }

  test("conversionsFloatWrapper") {
    assert(conversions(classOf[java.lang.Float])("42.0") == 42.0f)
    intercept[NumberFormatException] {
      conversions(classOf[Float])("forty-two")
    }
  }

  test("conversionsDoubleWrapper") {
    assert(conversions(classOf[java.lang.Double])("42.0") == 42.0)
    intercept[NumberFormatException] {
      conversions(classOf[Double])("forty-two")
    }
  }

  test("conversionsDateTime") {
    val time = ZonedDateTime.of(1983, 3, 24, 14, 32, 27, 0, ZoneOffset.UTC)
    assert(conversions(classOf[ZonedDateTime])("1983-03-24T14:32:27Z") == time)
    intercept[IllegalArgumentException] {
      conversions(classOf[ZonedDateTime])("forty-two")
    }
  }

  test("conversionsDateTimeZone - UTC") {
    val tz = ZoneId.of("UTC")
    assertEquals(conversions(classOf[ZoneId])("UTC"), tz)
  }

  test("conversionsDateTimeZone - US/Pacific") {
    val tz = ZoneId.of("US/Pacific")
    assertEquals(conversions(classOf[ZoneId])("US/Pacific"), tz)
  }

  test("conversionsPeriod") {
    assert(conversions(classOf[Duration])("PT42M") == Duration.ofMinutes(42))
    intercept[IllegalArgumentException] {
      conversions(classOf[Duration])("forty-two")
    }
  }

  test("conversionsPattern") {
    assert(conversions(classOf[Pattern])("^f").toString == "^f")
    intercept[IllegalArgumentException] {
      conversions(classOf[Pattern])("(")
    }
  }

  test("cast") {
    assert(cast[String](classOf[String], "foo") == "foo")
    intercept[IllegalArgumentException] {
      cast[System](classOf[System], "foo")
    }
  }

  test("escape") {
    var i = 0
    while (i < Short.MaxValue) {
      val str = Character.toString(i)
      assertEquals(escape(str, _ => true), s"\\u${zeroPad(i, 4)}")
      i += 1
    }
  }

  test("escape, comma and colon") {
    val decoded = ":foo-bar,baz"
    val encoded = "\\u003afoo-bar\\u002cbaz"
    assertEquals(escape(decoded, c => c == ',' || c == ':'), encoded)
    assertEquals(unescape(encoded), decoded)
    assertEquals(unescape(decoded), decoded)
  }

  test("unescape") {
    var i = 0
    while (i < Short.MaxValue) {
      val str = Character.toString(i)
      assertEquals(unescape(s"\\u${zeroPad(i, 4)}"), str)
      i += 1
    }
  }

  test("unescape, too short") {
    val input = "foo\\u000"
    assertEquals(unescape(input), input)
  }

  test("unescape, unknown type") {
    val input = "foo\\x0000"
    assertEquals(unescape(input), input)
  }

  test("unescape, invalid") {
    val input = "foo\\uzyff"
    assertEquals(unescape(input), input)
  }

  test("urlDecode") {
    val str = "a b %25 % %%% %21%zb"
    val expected = "a b % % %%% !%zb"
    assertEquals(urlDecode(str), expected)
  }

  test("hexDecode, escape with _") {
    val str = "a b %25 _ %_% _21%zb"
    val expected = "a b %25 _ %_% !%zb"
    assertEquals(hexDecode(str, '_'), expected)
  }

  test("urlEncode") {
    val str = "a&?= +[]()<>^$%\"':;-_|!@#*.~`\\/{}"
    val expected = "a%26%3F%3D%20%2B%5B%5D()%3C%3E%5E$%25%22':%3B-_%7C!@%23*.~`%5C/%7B%7D"
    assertEquals(urlEncode(str), expected)
    assertEquals(urlDecode(expected), str)
    assertEquals(urlDecode(expected.toLowerCase(Locale.US)), str)
  }

  test("urlEncode: CLDMTA-1582") {
    val str = "aggregator.http.post.(?!200)._count"
    val expected = "aggregator.http.post.(%3F!200)._count"
    assertEquals(urlEncode(str), expected)
  }

  test("parseQueryString, null") {
    val query = null
    val expected = Map.empty[String, List[String]]
    assertEquals(parseQueryString(query), expected)
  }

  test("parseQueryString") {
    val query = "foo=bar&foo=baz;bar&foo=%21&;foo&abc=42"
    val expected =
      Map("abc" -> List("42"), "bar" -> List("1"), "foo" -> List("bar", "baz", "!", "1").reverse)
    assertEquals(parseQueryString(query), expected)
  }

  test("parseDuration, at nanoseconds") {
    assertEquals(parseDuration("42ns"), Duration.ofNanos(42L))
  }

  test("parseDuration, at microseconds") {
    assertEquals(parseDuration("42us"), Duration.ofNanos(42_000L))
    assertEquals(parseDuration("42μs"), Duration.ofNanos(42_000L))
  }

  test("parseDuration, at milliseconds") {
    assertEquals(parseDuration("42ms"), Duration.ofMillis(42))
  }

  test("parseDuration, at seconds") {
    assertEquals(parseDuration("42seconds"), Duration.ofSeconds(42))
    assertEquals(parseDuration("42second"), Duration.ofSeconds(42))
    assertEquals(parseDuration("42s"), Duration.ofSeconds(42))
  }

  test("parseDuration, at minutes") {
    assertEquals(parseDuration("42minutes"), Duration.ofMinutes(42))
    assertEquals(parseDuration("42minute"), Duration.ofMinutes(42))
    assertEquals(parseDuration("42min"), Duration.ofMinutes(42))
    assertEquals(parseDuration("42m"), Duration.ofMinutes(42))
  }

  test("parseDuration, at hours") {
    assertEquals(parseDuration("42hours"), Duration.ofHours(42))
    assertEquals(parseDuration("42hour"), Duration.ofHours(42))
    assertEquals(parseDuration("42h"), Duration.ofHours(42))
  }

  // TODO: Need a combination of period with duration using java.time, similar to joda Period

  test("parseDuration, at days") {
    assertEquals(parseDuration("42days"), Duration.ofDays(42))
    assertEquals(parseDuration("42day"), Duration.ofDays(42))
    assertEquals(parseDuration("42d"), Duration.ofDays(42))
  }

  test("parseDuration, at weeks") {
    assertEquals(parseDuration("42weeks"), Duration.ofDays(42 * 7))
    assertEquals(parseDuration("42week"), Duration.ofDays(42 * 7))
    assertEquals(parseDuration("42wk"), Duration.ofDays(42 * 7))
    assertEquals(parseDuration("42w"), Duration.ofDays(42 * 7))
  }

  test("parseDuration, at months") {
    assertEquals(parseDuration("42months"), Duration.ofDays(42 * 30))
    assertEquals(parseDuration("42month"), Duration.ofDays(42 * 30))
  }

  test("parseDuration, at years") {
    assertEquals(parseDuration("42years"), Duration.ofDays(42 * 365))
    assertEquals(parseDuration("42year"), Duration.ofDays(42 * 365))
    assertEquals(parseDuration("42y"), Duration.ofDays(42 * 365))
  }

  test("parseDuration, at invalid unit") {
    val e = intercept[IllegalArgumentException] {
      parseDuration("42fubars")
    }
    assertEquals(e.getMessage, "unknown unit fubars")
  }

  test("parseDuration, iso") {
    // assertEquals(parseDuration("P42Y"), Period.years(42))
    assertEquals(parseDuration("PT42M"), Duration.ofMinutes(42))
  }

  test("toString Duration: weeks") {
    assertEquals(Strings.toString(Duration.ofDays(5 * 7)), "5w")
  }

  test("toString Duration: weeks + 10h") {
    // P5WT10H would be preferred, but it doesn't parse with java.time:
    // scala> Duration.parse("P5WT10H")
    // java.time.format.DateTimeParseException: Text cannot be parsed to a Duration
    //
    // P35DT10H would be better than 850h, but Duration.toString returns PT850H
    // scala> Duration.parse("P35DT10H")
    // res6: java.time.Duration = PT850H
    //
    // If it becomes enough of a pain point we can customize the output for the fallback
    assertEquals(Strings.toString(Duration.ofDays(5 * 7).plusHours(10)), "850h")
  }

  test("toString Duration: days") {
    assertEquals(Strings.toString(Duration.ofDays(5)), "5d")
  }

  test("toString Duration: hours") {
    assertEquals(Strings.toString(Duration.ofHours(5)), "5h")
  }

  test("toString Duration: minutes") {
    assertEquals(Strings.toString(Duration.ofMinutes(5)), "5m")
  }

  test("toString Duration: seconds") {
    assertEquals(Strings.toString(Duration.ofSeconds(5)), "5s")
  }

  test("parseDate, iso date only") {
    val expected = ZonedDateTime.of(2012, 2, 1, 0, 0, 0, 0, ZoneOffset.UTC)
    assertEquals(parseDate("2012-02-01"), expected)
  }

  // Note: will fail prior to 8u20:
  // https://github.com/Netflix/atlas/issues/9
  test("parseDate, iso date with time no seconds") {
    val expected = ZonedDateTime.of(2012, 2, 1, 4, 5, 0, 0, ZoneOffset.UTC)
    assertEquals(parseDate("2012-02-01T04:05"), expected)
    assertEquals(parseDate("2012-02-01T04:05Z"), expected)

    val result =
      ZonedDateTime.ofInstant(parseDate("2012-02-01T12:05+08:00").toInstant, ZoneOffset.UTC)
    assertEquals(result, expected)
  }

  // Note: will fail prior to 8u20:
  // https://github.com/Netflix/atlas/issues/9
  test("parseDate, iso date with time") {
    val expected = ZonedDateTime.of(2012, 2, 1, 4, 5, 6, 0, ZoneOffset.UTC)
    assertEquals(parseDate("2012-02-01T04:05:06"), expected)
  }

  test("parseDate, iso date with time and zone") {
    val expected = ZonedDateTime.of(2012, 1, 31, 20, 5, 6, 0, ZoneOffset.UTC)
    val result =
      ZonedDateTime.ofInstant(parseDate("2012-02-01T04:05:06+08:00").toInstant, ZoneOffset.UTC)
    assertEquals(result, expected)
  }

  test("parseDate, iso date with time with millis and zone") {
    val nanos = TimeUnit.MILLISECONDS.toNanos(123).toInt
    val expected = ZonedDateTime.of(2012, 2, 1, 4, 5, 6, nanos, ZoneOffset.UTC)
    assertEquals(parseDate("2012-02-01T04:05:06.123Z"), expected)
  }

  test("parseDate, iso date with time with millis and zone (+00:00)") {
    val nanos = TimeUnit.MILLISECONDS.toNanos(123).toInt
    val expected = ZonedDateTime.of(2012, 2, 1, 4, 5, 6, nanos, ZoneOffset.UTC)
    assertEquals(parseDate("2012-02-01T04:05:06.123+00:00"), expected)
  }

  test("parseDate, iso date with time with millis and zone (+0000)") {
    val nanos = TimeUnit.MILLISECONDS.toNanos(123).toInt
    val expected = ZonedDateTime.of(2012, 2, 1, 4, 5, 6, nanos, ZoneOffset.UTC)
    assertEquals(parseDate("2012-02-01T04:05:06.123+0000"), expected)
  }

  test("parseDate, iso date with time with millis and zone (+00)") {
    val nanos = TimeUnit.MILLISECONDS.toNanos(123).toInt
    val expected = ZonedDateTime.of(2012, 2, 1, 4, 5, 6, nanos, ZoneOffset.UTC)
    assertEquals(parseDate("2012-02-01T04:05:06.123+00"), expected)
  }

  test("parseDate, iso date with time with millis and zone offset") {
    val nanos = TimeUnit.MILLISECONDS.toNanos(123).toInt
    val expected = ZonedDateTime.of(2012, 2, 1, 7, 5, 6, nanos, ZoneOffset.UTC).toInstant
    assertEquals(parseDate("2012-02-01T04:05:06.123-03:00").toInstant, expected)
  }

  test("parseDate, iso formats") {
    val offsets = List(
      "",
      "Z",
      "+00",
      "+0000",
      "+000000",
      "+00:00",
      "+00:00:00",
      "-04",
      "-0402",
      "-040231",
      "-04:02",
      "-04:02:31"
    )
    val times = List(
      "2020-07-28",
      "2020-07-28T05:43",
      "2020-07-28T05:43:02",
      "2020-07-28T05:43:02.143"
    )
    for (t <- times; o <- offsets) {
      parseDate(s"$t$o")
    }
  }

  test("parseDate, iso date with time with millis") {
    val nanos = TimeUnit.MILLISECONDS.toNanos(123).toInt
    val expected = ZonedDateTime.of(2012, 2, 1, 4, 5, 6, nanos, ZoneOffset.UTC)
    assertEquals(parseDate("2012-02-01T04:05:06.123"), expected)
  }

  test("parseDate, iso invalid") {
    intercept[IllegalArgumentException] {
      parseDate("2012-02-")
    }
  }

  test("parseDate, relative minus") {
    val ref = ZonedDateTime.of(2012, 2, 1, 3, 0, 0, 0, ZoneOffset.UTC)
    val expected = ZonedDateTime.of(2012, 2, 1, 0, 0, 0, 0, ZoneOffset.UTC)
    assertEquals(parseDate("e-3h", ZoneOffset.UTC, Map("e" -> ref)), expected)
  }

  test("parseDate, relative plus") {
    val ref = ZonedDateTime.of(2012, 2, 1, 3, 0, 0, 0, ZoneOffset.UTC)
    val expected = ZonedDateTime.of(2012, 2, 1, 3, 0, 42, 0, ZoneOffset.UTC)
    assertEquals(parseDate("start+42s", ZoneOffset.UTC, Map("start" -> ref)), expected)
  }

  test("parseDate, relative iso") {
    val ref = ZonedDateTime.of(2012, 2, 2, 3, 0, 0, 0, ZoneOffset.UTC)
    val expected = ZonedDateTime.of(2012, 2, 1, 2, 54, 18, 0, ZoneOffset.UTC)
    assertEquals(parseDate("start-P1DT5M42S", ZoneOffset.UTC, Map("start" -> ref)), expected)
  }

  test("parseDate, epoch + 4h") {
    val expected = ZonedDateTime.of(1970, 1, 1, 4, 0, 0, 0, ZoneOffset.UTC)
    assertEquals(parseDate("epoch+4h", ZoneOffset.UTC), expected)
  }

  test("parseDate, relative invalid op") {
    val e = intercept[IllegalArgumentException] {
      parseDate("e*42h")
    }
    assertEquals(e.getMessage, "invalid date e*42h")
  }

  test("parseDate, named now") {
    val expected = System.currentTimeMillis
    val received = parseDate("now").toInstant.toEpochMilli
    assert(received >= expected)
  }

  test("parseDate, named epoch") {
    val received = parseDate("epoch").toInstant.toEpochMilli
    assertEquals(received, 0L)
  }

  test("parseDate, unix") {
    val ref = ZonedDateTime.of(2012, 2, 1, 3, 0, 0, 0, ZoneOffset.UTC)
    val refStr = "%d".format(ref.toInstant.toEpochMilli / 1000)
    val received = parseDate(refStr)
    assertEquals(received, ref)
  }

  test("parseDate, unix millis") {
    val ref = ZonedDateTime.of(2012, 2, 1, 3, 0, 0, 0, ZoneOffset.UTC)
    val refStr = "%d".format(ref.toInstant.toEpochMilli)
    val received = parseDate(refStr)
    assertEquals(received, ref)
  }

  test("parseDate, unix millis cutoff") {
    val ref = ZonedDateTime.of(2400, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
    val millisLast = ref.toInstant.toEpochMilli
    assertEquals(parseDate(millisLast.toString), ref)

    val microsRef = ZonedDateTime.of(1970, 6, 7, 1, 17, 45, 600_001_000, ZoneOffset.UTC)
    val microsFirst = millisLast + 1
    assertEquals(parseDate(microsFirst.toString), microsRef)
  }

  test("parseDate, unix micros") {
    val ref = ZonedDateTime.of(2012, 2, 1, 3, 0, 0, 0, ZoneOffset.UTC)
    val refStr = "%d".format(ref.toInstant.toEpochMilli * 1000L)
    val received = parseDate(refStr)
    assertEquals(received, ref)
  }

  test("parseDate, unix micros cutoff") {
    val ref = ZonedDateTime.of(2400, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
    val microsLast = ref.toInstant.toEpochMilli * 1000L
    assertEquals(parseDate(microsLast.toString), ref)

    val nanosRef = ZonedDateTime.of(1970, 6, 7, 1, 17, 45, 600_000_001, ZoneOffset.UTC)
    val nanosFirst = microsLast + 1
    assertEquals(parseDate(nanosFirst.toString), nanosRef)
  }

  test("parseDate, unix nanos") {
    val ref = ZonedDateTime.of(2012, 2, 1, 3, 0, 0, 0, ZoneOffset.UTC)
    val refStr = "%d".format(ref.toInstant.toEpochMilli * 1_000_000L)
    val received = parseDate(refStr)
    assertEquals(received, ref)
  }

  test("parseDate, unix with sub") {
    val ref = ZonedDateTime.of(2012, 2, 1, 3, 0, 0, 0, ZoneOffset.UTC)
    val refStr = "%d-3h".format(ref.toInstant.toEpochMilli / 1000)
    val received = parseDate(refStr)
    assertEquals(received, ref.minusHours(3))
  }

  test("parseDate, unix with add") {
    val ref = ZonedDateTime.of(2012, 2, 1, 3, 0, 0, 0, ZoneOffset.UTC)
    val refStr = "%d+3h".format(ref.toInstant.toEpochMilli / 1000)
    val received = parseDate(refStr)
    assertEquals(received, ref.plusHours(3))
  }

  test("parseDate, s=e-0h") {
    val ref = ZonedDateTime.of(2012, 2, 1, 3, 0, 0, 0, ZoneOffset.UTC)
    val expected = ZonedDateTime.of(2012, 2, 1, 3, 0, 0, 0, ZoneOffset.UTC)
    assertEquals(parseDate("e-0h", ZoneOffset.UTC, Map("e" -> ref)), expected)
  }

  test("parseDate, s=e") {
    val ref = ZonedDateTime.of(2012, 2, 1, 3, 0, 0, 0, ZoneOffset.UTC)
    val expected = ZonedDateTime.of(2012, 2, 1, 3, 0, 0, 0, ZoneOffset.UTC)
    assertEquals(parseDate("e", ZoneOffset.UTC, Map("e" -> ref)), expected)
  }

  test("parseColor") {
    assertEquals(parseColor("FF0000"), Color.RED)
    assertEquals(parseColor("ff0000"), Color.RED)
  }

  test("parseColor triple hex") {
    assertEquals(parseColor("f00"), Color.RED)
    assertEquals(parseColor("F00"), Color.RED)
  }

  test("parseColor with alpha") {
    val c = parseColor("0FFF0000")
    assertEquals(c.getAlpha, 15)
    assertEquals(c.getRed, 255)
    assertEquals(c.getGreen, 0)
    assertEquals(c.getBlue, 0)
  }

  test("isRelativeDate") {
    assert(isRelativeDate("now-6h"))
    assert(isRelativeDate("epoch+6h"))
    assert(isRelativeDate("s-6h"))
    assert(isRelativeDate("start+6h"))
    assert(isRelativeDate("e-6h"))
    assert(isRelativeDate("end+6h"))
  }

  test("isRelativeDate, custom") {
    assert(!isRelativeDate("now-6h", true))
    assert(!isRelativeDate("epoch+6h", true))
    assert(isRelativeDate("s-6h", true))
    assert(isRelativeDate("start+6h", true))
    assert(isRelativeDate("e-6h", true))
    assert(isRelativeDate("end+6h", true))
  }

  test("substitute") {
    val vars = Map("foo" -> "bar", "bar" -> "baz")
    assertEquals(substitute("$(foo)", vars), "bar")
    assertEquals(substitute("$(bar)", vars), "baz")
    assertEquals(substitute("$foo", vars), "bar")
    assertEquals(substitute("$(foo)$(bar)", vars), "barbaz")
    assertEquals(substitute("$(foo) ::: $(bar)", vars), "bar ::: baz")
    assertEquals(substitute("$(missing) ::: $(bar)", vars), "missing ::: baz")
    assertEquals(substitute("$missing ::: $bar", vars), "missing ::: baz")
  }

  test("substitute: ends with $") {
    val vars = Map("foo" -> "bar", "bar" -> "baz")
    assertEquals(substitute("foo$", vars), "foo$")
  }

  test("substitute: followed by whitespace") {
    val vars = Map("foo" -> "bar", "bar" -> "baz")
    assertEquals(substitute("$ foo", vars), "$ foo")
  }

  test("substitute: whitespace in paren") {
    val vars = Map(" foo" -> "bar", "bar" -> "baz")
    assertEquals(substitute("$( foo)", vars), "bar")
  }

  test("substitute: parens used to escape literal $") {
    val vars = Map("foo" -> "bar", "bar" -> "baz")
    assertEquals(substitute("$()foo", vars), "$foo")
  }

  test("substitute: unmatched open paren") {
    val vars = Map("foo" -> "bar", "bar" -> "baz")
    assertEquals(substitute("$(foo", vars), "$foo")
    assertEquals(substitute("foo$(", vars), "foo$")
  }

  test("substitute: unmatched closed paren") {
    val vars = Map("foo" -> "bar", "bar" -> "baz")
    assertEquals(substitute("$)foo", vars), "$)foo")
    assertEquals(substitute("foo$)", vars), "foo$)")
  }

  test("zeroPad int") {
    assertEquals(zeroPad(42, 1), "2a")
    assertEquals(zeroPad(42, 2), "2a")
    assertEquals(zeroPad(42, 3), "02a")
    assertEquals(zeroPad(42, 8), "0000002a")
    assertEquals(zeroPad(-42, 8), "ffffffd6")
    assertEquals(zeroPad(-42, 12), "0000ffffffd6")
  }

  test("zeroPad long") {
    assertEquals(zeroPad(42L, 1), "2a")
    assertEquals(zeroPad(42L, 2), "2a")
    assertEquals(zeroPad(42L, 3), "02a")
    assertEquals(zeroPad(42L, 8), "0000002a")
    assertEquals(zeroPad(-42L, 8), "ffffffffffffffd6")
    assertEquals(zeroPad(-42L, 18), "00ffffffffffffffd6")
  }

  test("zeroPad BigInteger") {
    val b = new BigInteger("42")
    assertEquals(zeroPad(b, 1), "2a")
    assertEquals(zeroPad(b, 2), "2a")
    assertEquals(zeroPad(b, 3), "02a")
    assertEquals(zeroPad(b, 8), "0000002a")
  }

  test("zeroPad byte array empty") {
    val b = Array.empty[Byte]
    assertEquals(zeroPad(b, 1), "0")
    assertEquals(zeroPad(b, 2), "00")
    assertEquals(zeroPad(b, 3), "000")
    assertEquals(zeroPad(b, 8), "00000000")
  }

  test("zeroPad byte array unsigned conversion") {
    val b = Array[Byte](-1)
    assertEquals(zeroPad(b, 1), "ff")
    assertEquals(zeroPad(b, 2), "ff")
    assertEquals(zeroPad(b, 3), "0ff")
    assertEquals(zeroPad(b, 8), "000000ff")
  }

  test("zeroPad byte array minimum padding of 2") {
    val b = Array[Byte](1)
    assertEquals(zeroPad(b, 1), "01")
    assertEquals(zeroPad(b, 2), "01")
    assertEquals(zeroPad(b, 3), "001")
    assertEquals(zeroPad(b, 8), "00000001")
  }

  test("zeroPad byte array all values") {
    (java.lang.Byte.MIN_VALUE until java.lang.Byte.MAX_VALUE).foreach { i =>
      val s = zeroPad(Array(i.toByte), 1)
      val v = Integer.parseInt(s, 16).byteValue()
      assertEquals(v, i.toByte)
    }
  }

  test("range: both absolute") {
    val (s, e) = timeRange("2018-07-24", "2018-07-24T00:05")
    assertEquals(s, parseDate("2018-07-24").toInstant)
    assertEquals(e, parseDate("2018-07-24T00:05").toInstant)
  }

  test("range: end is before start") {
    intercept[IllegalArgumentException] {
      timeRange("2018-07-24T00:05", "2018-07-24")
    }
  }

  test("range: start time is the same as end time") {
    val (s, e) = timeRange("2018-07-24", "2018-07-24")
    assertEquals(s, e)
  }

  test("range: both relative") {
    intercept[IllegalArgumentException] {
      timeRange("e-5m", "s+5m")
    }
  }

  test("range: unix time with op is not relative") {
    val (s, e) = timeRange("e-5m", "1733292000+5m")
    assertEquals(s, parseDate("2024-12-04T06:00:00Z").toInstant)
    assertEquals(e, parseDate("2024-12-04T06:05:00Z").toInstant)
  }

  test("range: start relative to end") {
    val (s, e) = timeRange("e-5m", "2018-07-24T00:05")
    assertEquals(s, parseDate("2018-07-24").toInstant)
    assertEquals(e, parseDate("2018-07-24T00:05").toInstant)
  }

  test("range: end relative to start") {
    val (s, e) = timeRange("2018-07-24", "s+5m")
    assertEquals(s, parseDate("2018-07-24").toInstant)
    assertEquals(e, parseDate("2018-07-24T00:05").toInstant)
  }

  test("range: explicit now") {
    val now = ZonedDateTime.parse("2018-07-24T12:00:00.000Z")
    val (s, e) = timeRange("2018-07-24", "now-3h", refs = Map("now" -> now))
    assertEquals(s, parseDate("2018-07-24T00:00:00.000Z").toInstant)
    assertEquals(e, parseDate("2018-07-24T09:00:00.000Z").toInstant)
  }
}

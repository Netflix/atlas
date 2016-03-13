/*
 * Copyright 2014-2016 Netflix, Inc.
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
import java.time.Duration
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import org.scalatest.FunSuite


class StringsSuite extends FunSuite {

  import com.netflix.atlas.core.util.Strings._

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
    assert(conversions(classOf[ZoneId])("UTC") === tz)
  }

  test("conversionsDateTimeZone - US/Pacific") {
    val tz = ZoneId.of("US/Pacific")
    assert(conversions(classOf[ZoneId])("US/Pacific") === tz)
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

  test("urlDecode") {
    val str = "a b %25 % %%% %21%zb"
    val expected = "a b % % %%% !%zb"
    assert(urlDecode(str) === expected)
  }

  test("urlEncode") {
    val str = "a&?= +[]()<>^$%\"':;-_|!@#*.~`\\/{}"
    val expected = "a%26%3F%3D%20%2B%5B%5D()%3C%3E%5E$%25%22':%3B-_%7C!@%23*.~`%5C/%7B%7D"
    assert(urlEncode(str) === expected)
  }

  test("urlEncode: CLDMTA-1582") {
    val str = "aggregator.http.post.(?!200)._count"
    val expected = "aggregator.http.post.(%3F!200)._count"
    assert(urlEncode(str) === expected)
  }

  test("parseQueryString, null") {
    val query = null
    val expected = Map.empty
    assert(parseQueryString(query) === expected)
  }

  test("parseQueryString") {
    val query = "foo=bar&foo=baz;bar&foo=%21&;foo&abc=42"
    val expected = Map(
      "abc" -> List("42"),
      "bar" -> List("1"),
      "foo" -> List("bar", "baz", "!", "1").reverse)
    assert(parseQueryString(query) === expected)
  }

  test("parseDuration, at seconds") {
    assert(parseDuration("42seconds") === Duration.ofSeconds(42))
    assert(parseDuration("42second") === Duration.ofSeconds(42))
    assert(parseDuration("42s") === Duration.ofSeconds(42))
  }

  test("parseDuration, at minutes") {
    assert(parseDuration("42minutes") === Duration.ofMinutes(42))
    assert(parseDuration("42minute") === Duration.ofMinutes(42))
    assert(parseDuration("42min") === Duration.ofMinutes(42))
    assert(parseDuration("42m") === Duration.ofMinutes(42))
  }

  test("parseDuration, at hours") {
    assert(parseDuration("42hours") === Duration.ofHours(42))
    assert(parseDuration("42hour") === Duration.ofHours(42))
    assert(parseDuration("42h") === Duration.ofHours(42))
  }

  // TODO: Need a combination of period with duration using java.time, similar to joda Period

  test("parseDuration, at days") {
    assert(parseDuration("42days") === Duration.ofDays(42))
    assert(parseDuration("42day") === Duration.ofDays(42))
    assert(parseDuration("42d") === Duration.ofDays(42))
  }

  test("parseDuration, at weeks") {
    assert(parseDuration("42weeks") === Duration.ofDays(42 * 7))
    assert(parseDuration("42week") === Duration.ofDays(42 * 7))
    assert(parseDuration("42wk") === Duration.ofDays(42 * 7))
    assert(parseDuration("42w") === Duration.ofDays(42 * 7))
  }

  test("parseDuration, at months") {
    assert(parseDuration("42months") === Duration.ofDays(42 * 30))
    assert(parseDuration("42month") === Duration.ofDays(42 * 30))
  }

  test("parseDuration, at years") {
    assert(parseDuration("42years") === Duration.ofDays(42 * 365))
    assert(parseDuration("42year") === Duration.ofDays(42 * 365))
    assert(parseDuration("42y") === Duration.ofDays(42 * 365))
  }

  test("parseDuration, at invalid unit") {
    val e = intercept[IllegalArgumentException] {
      parseDuration("42fubars")
    }
    assert(e.getMessage === "unknown unit fubars")
  }

  test("parseDuration, iso") {
    //assert(parseDuration("P42Y") === Period.years(42))
    assert(parseDuration("PT42M") === Duration.ofMinutes(42))
  }

  test("toString Duration: weeks") {
    assert(Strings.toString(Duration.ofDays(5 * 7)) === "5w")
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
    assert(Strings.toString(Duration.ofDays(5 * 7).plusHours(10)) === "850h")
  }

  test("toString Duration: days") {
    assert(Strings.toString(Duration.ofDays(5)) === "5d")
  }

  test("toString Duration: hours") {
    assert(Strings.toString(Duration.ofHours(5)) === "5h")
  }

  test("toString Duration: minutes") {
    assert(Strings.toString(Duration.ofMinutes(5)) === "5m")
  }

  test("toString Duration: seconds") {
    assert(Strings.toString(Duration.ofSeconds(5)) === "5s")
  }

  test("parseDate, iso date only") {
    val expected = ZonedDateTime.of(2012, 2, 1, 0, 0, 0, 0, ZoneOffset.UTC)
    assert(parseDate("2012-02-01") === expected)
  }

  // Note: will fail prior to 8u20:
  // https://github.com/Netflix/atlas/issues/9
  test("parseDate, iso date with time no seconds") {
    val expected = ZonedDateTime.of(2012, 2, 1, 4, 5, 0, 0, ZoneOffset.UTC)
    assert(parseDate("2012-02-01T04:05") === expected)
    assert(parseDate("2012-02-01T04:05Z") === expected)

    val result = ZonedDateTime.ofInstant(parseDate("2012-02-01T12:05+08:00").toInstant, ZoneOffset.UTC)
    assert(result === expected)
  }

  // Note: will fail prior to 8u20:
  // https://github.com/Netflix/atlas/issues/9
  test("parseDate, iso date with time") {
    val expected = ZonedDateTime.of(2012, 2, 1, 4, 5, 6, 0, ZoneOffset.UTC)
    assert(parseDate("2012-02-01T04:05:06") === expected)
  }

  test("parseDate, iso date with time and zone") {
    val expected = ZonedDateTime.of(2012, 1, 31, 20, 5, 6, 0, ZoneOffset.UTC)
    val result = ZonedDateTime.ofInstant(parseDate("2012-02-01T04:05:06+08:00").toInstant, ZoneOffset.UTC)
    assert(result === expected)
  }

  test("parseDate, iso date with time with millis and zone") {
    val nanos = TimeUnit.MILLISECONDS.toNanos(123).toInt
    val expected = ZonedDateTime.of(2012, 2, 1, 4, 5, 6, nanos, ZoneOffset.UTC)
    assert(parseDate("2012-02-01T04:05:06.123Z") === expected)
  }

  test("parseDate, iso date with time with millis and zone offset") {
    val nanos = TimeUnit.MILLISECONDS.toNanos(123).toInt
    val expected = ZonedDateTime.of(2012, 2, 1, 7, 5, 6, nanos, ZoneOffset.UTC).toInstant
    assert(parseDate("2012-02-01T04:05:06.123-03:00").toInstant === expected)
  }

  test("parseDate, iso date with time with millis") {
    val nanos = TimeUnit.MILLISECONDS.toNanos(123).toInt
    val expected = ZonedDateTime.of(2012, 2, 1, 4, 5, 6, nanos, ZoneOffset.UTC)
    assert(parseDate("2012-02-01T04:05:06.123") === expected)
  }

  test("parseDate, iso invalid") {
    intercept[IllegalArgumentException] {
      parseDate("2012-02-")
    }
  }

  test("parseDate, relative minus") {
    val ref = ZonedDateTime.of(2012, 2, 1, 3, 0, 0, 0, ZoneOffset.UTC)
    val expected = ZonedDateTime.of(2012, 2, 1, 0, 0, 0, 0, ZoneOffset.UTC)
    assert(parseDate(ref, "e-3h", ZoneOffset.UTC) === expected)
  }

  test("parseDate, relative plus") {
    val ref = ZonedDateTime.of(2012, 2, 1, 3, 0, 0, 0, ZoneOffset.UTC)
    val expected = ZonedDateTime.of(2012, 2, 1, 3, 0, 42, 0, ZoneOffset.UTC)
    assert(parseDate(ref, "start+42s", ZoneOffset.UTC) === expected)
  }

  test("parseDate, relative iso") {
    val ref = ZonedDateTime.of(2012, 2, 2, 3, 0, 0, 0, ZoneOffset.UTC)
    val expected = ZonedDateTime.of(2012, 2, 1, 2, 54, 18, 0, ZoneOffset.UTC)
    assert(parseDate(ref, "start-P1DT5M42S", ZoneOffset.UTC) === expected)
  }

  test("parseDate, epoch + 4h") {
    val ref = ZonedDateTime.of(2012, 2, 2, 3, 0, 0, 0, ZoneOffset.UTC)
    val expected = ZonedDateTime.of(1970, 1, 1, 4, 0, 0, 0, ZoneOffset.UTC)
    assert(parseDate(ref, "epoch+4h", ZoneOffset.UTC) === expected)
  }

  test("parseDate, relative invalid op") {
    val e = intercept[IllegalArgumentException] {
      parseDate("e*42h")
    }
    assert(e.getMessage === "invalid date e*42h")
  }

  test("parseDate, named now") {
    val expected = System.currentTimeMillis
    val received = parseDate("now").toInstant.toEpochMilli
    assert(received >= expected)
  }

  test("parseDate, named epoch") {
    val received = parseDate("epoch").toInstant.toEpochMilli
    assert(received === 0)
  }

  test("parseDate, unix") {
    val ref = ZonedDateTime.of(2012, 2, 1, 3, 0, 0, 0, ZoneOffset.UTC)
    val refStr = "%d".format(ref.toInstant.toEpochMilli / 1000)
    val received = parseDate(refStr)
    assert(received === ref)
  }

  test("parseDate, unix millis") {
    val ref = ZonedDateTime.of(2012, 2, 1, 3, 0, 0, 0, ZoneOffset.UTC)
    val refStr = "%d".format(ref.toInstant.toEpochMilli)
    val received = parseDate(refStr)
    assert(received === ref)
  }

  test("parseDate, s=e-0h") {
    val ref = ZonedDateTime.of(2012, 2, 1, 3, 0, 0, 0, ZoneOffset.UTC)
    val expected = ZonedDateTime.of(2012, 2, 1, 3, 0, 0, 0, ZoneOffset.UTC)
    assert(parseDate(ref, "e-0h", ZoneOffset.UTC) === expected)
  }

  test("parseDate, s=e") {
    val ref = ZonedDateTime.of(2012, 2, 1, 3, 0, 0, 0, ZoneOffset.UTC)
    val expected = ZonedDateTime.of(2012, 2, 1, 3, 0, 0, 0, ZoneOffset.UTC)
    assert(parseDate(ref, "e", ZoneOffset.UTC) === expected)
  }

  test("parseColor") {
    assert(parseColor("FF0000") === Color.RED)
    assert(parseColor("ff0000") === Color.RED)
  }

  test("parseColor triple hex") {
    assert(parseColor("f00") === Color.RED)
    assert(parseColor("F00") === Color.RED)
  }

  test("parseColor with alpha") {
    val c = parseColor("0FFF0000")
    assert(c.getAlpha === 15)
    assert(c.getRed === 255)
    assert(c.getGreen === 0)
    assert(c.getBlue === 0)
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
    assert(substitute("$(foo)", vars) === "bar")
    assert(substitute("$(bar)", vars) === "baz")
    assert(substitute("$foo", vars) === "bar")
    assert(substitute("$(foo)$(bar)", vars) === "barbaz")
    assert(substitute("$(foo) ::: $(bar)", vars) === "bar ::: baz")
    assert(substitute("$(missing) ::: $(bar)", vars) === "missing ::: baz")
    assert(substitute("$missing ::: $bar", vars) === "missing ::: baz")
  }
}

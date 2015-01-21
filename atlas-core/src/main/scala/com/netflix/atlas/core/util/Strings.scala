/*
 * Copyright 2015 Netflix, Inc.
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
import java.net.URLDecoder
import java.net.URLEncoder
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatterBuilder
import java.util.regex.Pattern


/**
 * Helper functions for working with strings.
 */
object Strings {

  /**
   * URL query parameter.
   */
  private val QueryParam = """^([^=]+)=(.*)$""".r

  /**
   * Simple variable syntax with $varname.
   */
  private val SimpleVar = """\$([-_.a-zA-Z0-9]+)""".r

  /**
   * Simple variable syntax where variable name is enclosed in parenthesis,
   * e.g., $(varname).
   */
  private val ParenVar = """\$\(([^\(\)]+)\)""".r

  /**
   * Period following conventions of unix `at` command.
   */
  private val AtPeriod = """^(\d+)([a-z]+)$""".r

  /**
   * Period following the ISO8601 conventions.
   */
  private val IsoPeriod = """^(P.*)$""".r

  /**
   * Date following the ISO8601 conventions.
   */
  private val IsoDate = """^(\d{4}-\d{2}-\d{2}(?:[-+Z].*)?)$""".r
  private val IsoDateTime = """^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2})?Z?)$""".r
  private val IsoOffsetDateTime = """^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2})?[-+].*)$""".r

  /**
   * Date relative to a given reference point.
   */
  private val RelativeDate = """^([a-z]+)([\-+])(.+)$""".r

  /**
   * Named date such as `epoch` or `now`.
   */
  private val NamedDate = """^([a-z]+)$""".r

  /**
   * Unix data in seconds since the epoch.
   */
  private val UnixDate = """^([0-9]+)$""".r

  private val isoDateFmt = (new DateTimeFormatterBuilder)
    .appendPattern("yyyy-MM-dd")
    .toFormatter

  /**
   * Conversion functions that map a string value to an instance of a given
   * class.
   */
  private[util] val conversions = {
    Map[Class[_], (String) => Any](
      classOf[String] -> (v => v),

      classOf[Boolean] -> (v => java.lang.Boolean.valueOf(v)),
      classOf[Byte] -> (v => java.lang.Byte.valueOf(v)),
      classOf[Short] -> (v => java.lang.Short.valueOf(v)),
      classOf[Int] -> (v => java.lang.Integer.valueOf(v)),
      classOf[Long] -> (v => java.lang.Long.valueOf(v)),
      classOf[Float] -> (v => java.lang.Float.valueOf(v)),
      classOf[Double] -> (v => java.lang.Double.valueOf(v)),
      classOf[Number] -> (v => java.lang.Double.valueOf(v)),

      classOf[java.lang.Boolean] -> (v => java.lang.Boolean.valueOf(v)),
      classOf[java.lang.Byte] -> (v => java.lang.Byte.valueOf(v)),
      classOf[java.lang.Short] -> (v => java.lang.Short.valueOf(v)),
      classOf[java.lang.Integer] -> (v => java.lang.Integer.valueOf(v)),
      classOf[java.lang.Long] -> (v => java.lang.Long.valueOf(v)),
      classOf[java.lang.Float] -> (v => java.lang.Float.valueOf(v)),
      classOf[java.lang.Double] -> (v => java.lang.Double.valueOf(v)),

      classOf[ZonedDateTime] -> (v => parseDate(v)),
      classOf[ZoneId] -> (v => ZoneId.of(v)),
      classOf[Duration] -> (v => parseDuration(v)),

      classOf[Pattern] -> (v => Pattern.compile(v)),

      classOf[Color] -> (v => parseColor(v)))
  }

  /**
   * Returns true if a conversion exists for the specified class.
   */
  def conversionExists(c: Class[_]): Boolean = {
    conversions.contains(c)
  }

  private def enumValueOf[T <: Enum[T]](c: Class[_], v: String): T = {
    Enum.valueOf(c.asInstanceOf[Class[T]], v)
  }

  /**
   * Cast a string value to an internal type.
   */
  def cast[T](c: Class[_], v: String): T = {
    if (c.isEnum) enumValueOf(c, v) else {
      conversions.get(c) match {
        case Some(f) => f(v).asInstanceOf[T]
        case None =>
          throw new IllegalArgumentException("unsupported property type " +
            c.getName + ", must be one of " +
            conversions.keys.mkString(", "))
      }
    }
  }

  private val uriEscapes: Array[String] = {
    def hex(c: Char) = "%%%02X".format(c.toInt)
    val array = new Array[String](128)
    var pos = 0
    while (pos < array.length) {
      val c = pos.toChar
      array(pos) = if (Character.isISOControl(c)) hex(c) else c.toString
      pos += 1
    }

    array(' ') = hex(' ')
    array('+') = hex('+')
    array('#') = hex('#')
    array('"') = hex('"')
    array('%') = hex('%')
    array('&') = hex('&')
    array(';') = hex(';')
    array('<') = hex('<')
    array('=') = hex('=')
    array('>') = hex('>')
    array('?') = hex('?')
    array('[') = hex('[')
    array('\\') = hex('\\')
    array(']') = hex(']')
    array('^') = hex('^')
    array('{') = hex('{')
    array('|') = hex('|')
    array('}') = hex('}')
    array
  }

  /**
   * Lenient url-encoder. The URLEncoder class provided in the jdk is eager to
   * percent encode making atlas expressions hard to read. This version assumes
   * the only escaping necessary for '%', '&amp;', '+', '?', '=', and ' '.
   */
  def urlEncode(s: String): String = {
    val buf = new StringBuilder
    val size = s.length
    var pos = 0
    while (pos < size) {
      val c = s.charAt(pos).toInt
      if (c < 128)
        buf.append(uriEscapes(c))
      else
        buf.append(URLEncoder.encode(c.toChar.toString, "UTF-8"))
      pos += 1
    }
    buf.toString
  }

  private def isHexChar(c: Char): Boolean = {
    ('0' <= c && c <= '9') ||
      ('A' <= c && c <= 'H') ||
      ('a' <= c && c <= 'h')
  }

  /**
   * Lenient url-decoder. The URLDecoder class provided in the jdk throws
   * if there is an invalid hex encoded value. This function will map invalid
   * encodes to a %25 (a literal percent sign) and then decode it normally.
   */
  def urlDecode(s: String): String = {
    val buf = new StringBuilder
    val size = s.length
    var pos = 0
    while (pos < size) {
      val c = s.charAt(pos)
      if (c == '%') {
        if (size - pos <= 2) {
          buf.append("%25")
        } else {
          val c1 = s.charAt(pos + 1)
          val c2 = s.charAt(pos + 2)
          if (isHexChar(c1) && isHexChar(c2)) {
            buf.append(c).append(c1).append(c2)
            pos += 2
          } else {
            buf.append("%25")
          }
        }
      } else {
        buf.append(c)
      }
      pos += 1
    }
    URLDecoder.decode(buf.toString, "UTF-8")
  }

  /**
   * Helper function for parseQueryString.
   */
  private def add(acc: Map[String, List[String]], k: String, v: String) = {
    val dk = urlDecode(k)
    val dv = urlDecode(v)
    acc + (dk -> (dv :: acc.getOrElse(dk, Nil)))
  }

  /**
   * Returns a map corresponding to the URL query parameters in the string.
   */
  def parseQueryString(query: String): Map[String, List[String]] = {
    if (query == null) Map.empty else {
      val params = Map.empty[String, List[String]]
      query.split("[&;]+").foldLeft(params) { (acc, p) =>
        p match {
          case QueryParam(k, v) => add(acc, k, v)
          case k                => add(acc, k, "1")
        }
      }
    }
  }

  /**
   * Substitute variables into a string.
   */
  def substitute(str: String, vars: Map[String, String]): String = {
    import scala.util.matching.Regex
    def f(m: Regex.Match): String = vars.getOrElse(m.group(1), m.group(1))
    val tmp = SimpleVar.replaceAllIn(str, f _)
    ParenVar.replaceAllIn(tmp, f _)
  }

  /**
   * Returns true if a date string is relative.
   */
  def isRelativeDate(str: String): Boolean = isRelativeDate(str, false)

  /**
   * Returns true if a date string is relative. If custom ref is true it will
   * check if it is a relative date against a custom reference point other than
   * now or the epoch.
   */
  def isRelativeDate(str: String, customRef: Boolean): Boolean = str match {
    case RelativeDate(r, _, _) => !customRef || (r != "now" && r != "epoch")
    case _                     => false
  }

  /**
   * Return the time associated with a given string. The time will be relative
   * to `now`.
   */
  def parseDate(str: String, tz: ZoneId = ZoneOffset.UTC): ZonedDateTime = {
    parseDate(ZonedDateTime.now(tz), str, tz)
  }

  /**
   * Return the time associated with a given string.
   *
   * - now, n:
   * - start, s:
   * - end, e:
   * - epoch:
   *
   * - seconds, s:
   * - minutes, m:
   * - hours, h:
   * - days, d:
   * - weeks, w:
   * - months
   * - years, y:
   */
  def parseDate(ref: ZonedDateTime, str: String, tz: ZoneId): ZonedDateTime = str match {
    case IsoDate(_) =>
      val date = LocalDate.parse(str, DateTimeFormatter.ISO_DATE.withZone(tz))
      ZonedDateTime.of(date, LocalTime.MIN, tz)
    case IsoDateTime(_) =>
      val z = if (str.endsWith("Z")) ZoneOffset.UTC else tz
      ZonedDateTime.parse(str, DateTimeFormatter.ISO_DATE_TIME.withZone(z))
    case IsoOffsetDateTime(_) =>
      ZonedDateTime.parse(str, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
    case RelativeDate(r, op, p) => op match {
      case "-" => parseRefVar(ref, r).minus(parseDuration(p))
      case "+" => parseRefVar(ref, r).plus(parseDuration(p))
      case _   => throw new IllegalArgumentException("invalid operation " + op)
    }
    case NamedDate(r) =>
      parseRefVar(ref, r)
    case UnixDate(d) =>
      // If the value is too big assume it is a milliseconds unit like java uses. The overlap is
      // fairly small and not in the range we typically use:
      // scala> Instant.ofEpochMilli(Integer.MAX_VALUE)
      // res1: java.time.Instant = 1970-01-25T20:31:23.647Z
      val v = d.toLong
      val t = if (v > Integer.MAX_VALUE) v else v * 1000L
      ZonedDateTime.ofInstant(Instant.ofEpochMilli(t), tz)
    case _ =>
      throw new IllegalArgumentException("invalid date " + str)
  }

  /**
   * Returns the datetime object associated with a given reference point.
   */
  private def parseRefVar(ref: ZonedDateTime, v: String): ZonedDateTime = {
    v match {
      case "now"   => ZonedDateTime.now(ZoneOffset.UTC)
      case "epoch" => ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC)
      case _       => ref
    }
  }

  /**
   * Parse a string that follows the ISO8601 spec or `at` time range spec
   * into a period object.
   */
  def parseDuration(str: String): Duration = str match {
    case AtPeriod(a, u) => parseAtDuration(a, u)
    case IsoPeriod(p)   => Duration.parse(str) //isoPeriodFmt.parsePeriod(p)
    case _              => throw new IllegalArgumentException("invalid period " + str)
  }

  /**
   * Convert an `at` command time range into a joda period object.
   */
  private def parseAtDuration(amount: String, unit: String): Duration = {
    val v = amount.toInt
    unit match {
      case "seconds" | "second" | "s"         => Duration.ofSeconds(v)
      case "minutes" | "minute" | "min" | "m" => Duration.ofMinutes(v)
      case "hours"   | "hour"   | "h"         => Duration.ofHours(v)
      case "days"    | "day"    | "d"         => Duration.ofDays(v)
      case "weeks"   | "week"   | "wk"  | "w" => Duration.ofDays(v * 7)
      case "months"  | "month"                => Duration.ofDays(v * 30)
      case "years"   | "year"   | "y"         => Duration.ofDays(v * 365)
      case _ => throw new IllegalArgumentException("unknown unit " + unit)
    }
  }

  /**
   * Parse a color expressed as a hexadecimal RRGGBB string.
   */
  def parseColor(str: String): Color = {
    val len = str.length
    require(len == 3 || len == 6 || len == 8, "color must be hex string [AA]RRGGBB")
    val colorStr = if (len == 3) str.map(c => "%s%s".format(c, c)).mkString else str
    if (len <= 6)
      new Color(Integer.parseInt(colorStr, 16), false)
    else
      new Color(java.lang.Long.parseLong(colorStr, 16).toInt, true)
  }

  // Standardized date/time constants:
  private final val oneSecond = 1000L
  private final val oneMinute = oneSecond * 60L
  private final val oneHour = oneMinute * 60L
  private final val oneDay = oneHour * 24L
  private final val oneWeek = oneDay * 7L

  /**
   * Returns a string representation of a period.
   */
  def toString(d: Duration): String = {
    d.toMillis match {
      case t if t % oneWeek   == 0 => s"${t / oneWeek}w"
      case t if t % oneDay    == 0 => s"${t / oneDay}d"
      case t if t % oneHour   == 0 => s"${t / oneHour}h"
      case t if t % oneMinute == 0 => s"${t / oneMinute}m"
      case t if t % oneSecond == 0 => s"${t / oneSecond}s"
      case _                       => d.toString
    }
  }

  /**
   * Strip the margin from multi-line strings.
   */
  def stripMargin(str: String): String = {
    val s = str.stripMargin.trim
    s.replaceAll("\n\n+", "@@@").
      replaceAll("\n", " ").
      replaceAll("@@@", "\n\n")
  }
}

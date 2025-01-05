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
import java.net.URLEncoder
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
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
    * Period following conventions of unix `at` command.
    */
  private val AtPeriod = """^(\d+)([a-zμ]+)$""".r

  /**
    * Period following the ISO8601 conventions.
    */
  private val IsoPeriod = """^(P.*)$""".r

  /**
    * Date relative to a given reference point.
    */
  private val RelativeDate = """^([a-z]+)([\-+])([^\-+]+)$""".r

  /**
    * Named date such as `epoch` or `now`.
    */
  private val NamedDate = """^([a-z]+)$""".r

  /**
    * Unix data in seconds since the epoch.
    */
  private val UnixDate = """^([0-9]+)$""".r

  /**
    * Unix data in seconds since the epoch.
    */
  private val UnixDateWithOp = """^([0-9]+)([\-+])([^\-+]+)$""".r

  /**
    * When parsing a timestamp string, timestamps after this point will be treated as
    * milliseconds rather than seconds. This time if treated as millis represents
    * `1970-01-25T20:31:23.647Z` so the region of overlap is well outside the range of
    * dates that we use.
    */
  private val secondsCutoff = Integer.MAX_VALUE

  /**
    * When parsing a timestamp string, timestamps after this point will be treated as
    * microseconds rather than milliseconds. It is several hundred years in the future
    * when treated as milliseconds, so it is well outside the bounds of dates we use.
    */
  private val millisCutoff = {
    ZonedDateTime.of(2400, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant.toEpochMilli
  }

  /**
    * When parsing a timestamp string, timestamps after this point will be treated as
    * nanoseconds rather than microseconds. It is several hundred years in the future
    * when treated as microseconds, so it is well outside the bounds of dates we use.
    */
  private val microsCutoff = millisCutoff * 1000L

  /**
    * Conversion functions that map a string value to an instance of a given
    * class.
    */
  private[util] val conversions = {
    Map[Class[?], String => Any](
      classOf[String]            -> (v => v),
      classOf[Boolean]           -> (v => java.lang.Boolean.valueOf(v)),
      classOf[Byte]              -> (v => java.lang.Byte.valueOf(v)),
      classOf[Short]             -> (v => java.lang.Short.valueOf(v)),
      classOf[Int]               -> (v => java.lang.Integer.valueOf(v)),
      classOf[Long]              -> (v => java.lang.Long.valueOf(v)),
      classOf[Float]             -> (v => java.lang.Float.valueOf(v)),
      classOf[Double]            -> (v => java.lang.Double.valueOf(v)),
      classOf[Number]            -> (v => java.lang.Double.valueOf(v)),
      classOf[java.lang.Boolean] -> (v => java.lang.Boolean.valueOf(v)),
      classOf[java.lang.Byte]    -> (v => java.lang.Byte.valueOf(v)),
      classOf[java.lang.Short]   -> (v => java.lang.Short.valueOf(v)),
      classOf[java.lang.Integer] -> (v => java.lang.Integer.valueOf(v)),
      classOf[java.lang.Long]    -> (v => java.lang.Long.valueOf(v)),
      classOf[java.lang.Float]   -> (v => java.lang.Float.valueOf(v)),
      classOf[java.lang.Double]  -> (v => java.lang.Double.valueOf(v)),
      classOf[ZonedDateTime]     -> (v => parseDate(v)),
      classOf[ZoneId]            -> (v => ZoneId.of(v)),
      classOf[Duration]          -> (v => parseDuration(v)),
      classOf[Pattern]           -> (v => Pattern.compile(v)),
      classOf[Color]             -> (v => parseColor(v))
    )
  }

  /**
    * Returns true if a conversion exists for the specified class.
    */
  def conversionExists(c: Class[?]): Boolean = {
    conversions.contains(c)
  }

  private def enumValueOf[T <: Enum[T]](c: Class[?], v: String): T = {
    Enum.valueOf(c.asInstanceOf[Class[T]], v)
  }

  /**
    * Cast a string value to an internal type.
    */
  def cast[T](c: Class[?], v: String): T = {
    if (c.isEnum) enumValueOf(c, v)
    else {
      conversions.get(c) match {
        case Some(f) => f(v).asInstanceOf[T]
        case None =>
          throw new IllegalArgumentException(
            "unsupported property type " +
              c.getName + ", must be one of " +
              conversions.keys.mkString(", ")
          )
      }
    }
  }

  /**
    * Escape special characters in the input string to unicode escape sequences (\uXXXX).
    */
  def escape(input: String, isSpecial: Int => Boolean): String = {
    val builder = new java.lang.StringBuilder(input.length)
    escape(builder, input, isSpecial)
    builder.toString
  }

  /**
    * Escape special characters in the input string to unicode escape sequences (\uXXXX).
    */
  def escape(builder: java.lang.StringBuilder, input: String, isSpecial: Int => Boolean): Unit = {
    val length = input.length
    var i = 0
    while (i < length) {
      val cp = input.codePointAt(i)
      val len = Character.charCount(cp)
      if (isSpecial(cp))
        escapeCodePoint(cp, builder)
      else
        builder.appendCodePoint(cp)
      i += len
    }
  }

  private def escapeCodePoint(cp: Int, builder: java.lang.StringBuilder): Unit = {
    builder.append("\\u")
    builder.append(zeroPad(cp, 4))
  }

  /**
    * Unescape unicode characters in the input string. Ignore any invalid or unrecognized
    * escape sequences.
    */
  def unescape(input: String): String = {
    val length = input.length
    val builder = new java.lang.StringBuilder(length)
    var i = 0
    while (i < length) {
      val c = input.charAt(i)
      if (c == '\\') {
        // Ensure there is enough space for an encoded character, there must be at
        // least 5 characters left in the string (uXXXX).
        if (length - i <= 5) {
          builder.append(input.substring(i))
          i = length
        } else if (input.charAt(i + 1) == 'u') {
          try {
            val cp = Integer.parseInt(input.substring(i + 2, i + 6), 16)
            builder.appendCodePoint(cp)
            i += 5
          } catch {
            case _: NumberFormatException => builder.append(c)
          }
        } else {
          // Some other escape, copy into buffer and move on
          builder.append(c)
        }
      } else {
        builder.append(c)
      }
      i += 1
    }
    builder.toString
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
    val buf = new java.lang.StringBuilder(s.length)
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

  /**
    * Lenient url-decoder. The URLDecoder class provided in the jdk throws
    * if there is an invalid hex encoded value. This function will map invalid
    * encodes to a %25 (a literal percent sign) and then decode it normally.
    */
  def urlDecode(s: String): String = hexDecode(s)

  /** Hex decode an input string.
    *
    * @param input
    *     Input string to decode.
    * @param escapeChar
    *     Character used to indicate the start of a hex encoded symbol.
    * @return
    *     Decoded string.
    */
  def hexDecode(input: String, escapeChar: Char = '%'): String = {
    val buf = new java.lang.StringBuilder(input.length)
    val size = input.length
    var pos = 0
    while (pos < size) {
      val c = input.charAt(pos)
      if (c == escapeChar) {
        if (size - pos <= 2) {
          // Not enough room left for two hex characters, copy the rest of
          // the string to the buffer and end the loop.
          buf.append(input.substring(pos))
          pos = size
        } else {
          val c1 = hexValue(input.charAt(pos + 1))
          val c2 = hexValue(input.charAt(pos + 2))
          if (c1 >= 0 && c2 >= 0) {
            // Both are hex chars, add decoded character to buffer
            val nc = (c1 << 4 | c2).asInstanceOf[Char]
            buf.append(nc)
            pos += 2
          } else {
            // Not a valid hex encoding, just echo the escape character
            // back into the buffer and move on.
            buf.append(c)
          }
        }
      } else {
        buf.append(c)
      }
      pos += 1
    }
    buf.toString
  }

  /** Converts a hex character into an integer value. Returns -1 if the input is not a
    * hex character.
    */
  private def hexValue(c: Char): Int = c match {
    case v if v >= '0' && v <= '9' => v - '0'
    case v if v >= 'A' && v <= 'F' => v - 'A' + 10
    case v if v >= 'a' && v <= 'f' => v - 'a' + 10
    case _                         => -1
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
    if (query == null) Map.empty
    else {
      val params = Map.empty[String, List[String]]
      query.split("[&;]+").foldLeft(params) { (acc, p) =>
        p match {
          case QueryParam(k, v) => add(acc, k, v)
          case k                => add(acc, k, "1")
        }
      }
    }
  }

  private val allowedInVarName = {
    val alphabet = new Array[Boolean](128)
    alphabet('.') = true
    alphabet('-') = true
    alphabet('_') = true
    ('a' to 'z').foreach(c => alphabet(c) = true)
    ('A' to 'Z').foreach(c => alphabet(c) = true)
    ('0' to '9').foreach(c => alphabet(c) = true)
    alphabet
  }

  private def simpleVar(str: String, i: Int, key: java.lang.StringBuilder): Int = {
    var j = i
    while (j < str.length) {
      val c = str.charAt(j)
      if (c < allowedInVarName.length && allowedInVarName(c))
        key.append(c)
      else
        return j
      j += 1
    }
    j
  }

  private def parenVar(str: String, i: Int, key: java.lang.StringBuilder): Int = {
    var j = i
    while (j < str.length) {
      val c = str.charAt(j)
      if (c != ')')
        key.append(c)
      else
        return j + 1
      j += 1
    }
    key.setLength(0)
    i
  }

  private def getKey(str: String, i: Int, key: java.lang.StringBuilder): Int = {
    val c = str.charAt(i + 1)
    if (c == '(') parenVar(str, i + 2, key) else simpleVar(str, i + 1, key)
  }

  /**
    * Substitute variables from the map into a string. If a key used in the
    * input string is not set, then the key will be used as the value.
    */
  def substitute(str: String, vars: Map[String, String]): String = {
    substitute(str, k => vars.getOrElse(k, k))
  }

  /**
    * Substitute variables into a string.
    */
  def substitute(str: String, vars: String => String): String = {
    val key = new java.lang.StringBuilder(str.length)
    val buf = new java.lang.StringBuilder(str.length * 2)
    var i = 0
    while (i < str.length) {
      val c = str.charAt(i)
      if (c != '$' || i == str.length - 1) {
        buf.append(c)
        i += 1
      } else {
        i = getKey(str, i, key)
        val k = key.toString
        // Empty keys are treated as '$' literals
        if (k.isEmpty)
          buf.append('$')
        else
          buf.append(vars(k))
        key.setLength(0)
      }
    }
    buf.toString
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
    * Returns the name of the reference point for relative dates. For example, with the
    * relative date `now-5m`, it would return `now`.
    */
  def extractReferencePointDate(str: String): Option[String] = str match {
    case RelativeDate(r, _, _) => Some(r)
    case NamedDate(r)          => Some(r)
    case _                     => None
  }

  /**
    * Return the time associated with a given string. Times can be relative to a reference
    * point using syntax `<ref><+/-><duration>`. Supported references points are `s`, `e`, `now`,
    * and `epoch`. See `parseDuration` for more details about durations.
    */
  def parseDate(
    str: String,
    tz: ZoneId = ZoneOffset.UTC,
    refs: Map[String, ZonedDateTime] = Map.empty
  ): ZonedDateTime = {
    str match {
      case RelativeDate(r, op, p) =>
        applyDateOffset(parseRefVar(refs, r), op, p)
      case NamedDate(r) =>
        parseRefVar(refs, r)
      case UnixDate(d) =>
        parseUnixDate(d, tz)
      case UnixDateWithOp(d, op, p) =>
        applyDateOffset(parseUnixDate(d, tz), op, p)
      case str =>
        try IsoDateTimeParser.parse(str, tz)
        catch {
          case e: Exception => throw new IllegalArgumentException(s"invalid date $str", e)
        }
    }
  }

  private def parseUnixDate(d: String, tz: ZoneId): ZonedDateTime = {
    val t = d.toLong match {
      case v if v <= secondsCutoff => Instant.ofEpochSecond(v)
      case v if v <= millisCutoff  => Instant.ofEpochMilli(v)
      case v if v <= microsCutoff  => ofEpoch(v, 1_000_000L, 1_000L)
      case v                       => ofEpoch(v, 1_000_000_000L, 1L)
    }
    ZonedDateTime.ofInstant(t, tz)
  }

  private def applyDateOffset(t: ZonedDateTime, op: String, p: String): ZonedDateTime = {
    val d = parseDuration(p)
    op match {
      case "-" => t.minus(d)
      case "+" => t.plus(d)
      case _   => throw new IllegalArgumentException("invalid operation " + op)
    }
  }

  private def ofEpoch(v: Long, f1: Long, f2: Long): Instant = {
    Instant.ofEpochSecond(v / f1, (v % f1) * f2)
  }

  /**
    * Returns the datetime object associated with a given reference point.
    */
  private def parseRefVar(refs: Map[String, ZonedDateTime], v: String): ZonedDateTime = {
    refs.getOrElse(
      v,
      v match {
        case "epoch" => ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC)
        case _       => ZonedDateTime.now(ZoneOffset.UTC)
      }
    )
  }

  /**
    * Parse a string that follows the ISO8601 spec or `at` time range spec
    * into a period object.
    */
  def parseDuration(str: String): Duration = str match {
    case AtPeriod(a, u) => parseAtDuration(a, u)
    case IsoPeriod(_)   => Duration.parse(str)
    case _              => throw new IllegalArgumentException("invalid period " + str)
  }

  /**
    * Convert an `at` command time range into a joda period object.
    */
  private def parseAtDuration(amount: String, unit: String): Duration = {
    val v = amount.toLong

    // format: off
    unit match {
      case "ns"                               => Duration.ofNanos(v)
      case "us" | "μs"                        => Duration.ofNanos(v * 1000L)
      case "ms"                               => Duration.ofMillis(v)
      case "seconds" | "second" | "s"         => Duration.ofSeconds(v)
      case "minutes" | "minute" | "min" | "m" => Duration.ofMinutes(v)
      case "hours"   | "hour"   | "h"         => Duration.ofHours(v)
      case "days"    | "day"    | "d"         => Duration.ofDays(v)
      case "weeks"   | "week"   | "wk"  | "w" => Duration.ofDays(v * 7)
      case "months"  | "month"                => Duration.ofDays(v * 30)
      case "years"   | "year"   | "y"         => Duration.ofDays(v * 365)
      case _                                  => throw new IllegalArgumentException("unknown unit " + unit)
    }
    // format: on
  }

  /**
    * Parse start and end time strings that can be relative to each other and resolve to
    * precise instants.
    *
    * @param s
    *     Start time string in a format supported by `parseDate`.
    * @param e
    *     End time string in a format supported by `parseDate`.
    * @param tz
    *     Time zone to assume for the times if a zone is not explicitly specified. Defaults
    *     to UTC.
    * @return
    *     Tuple `start -> end`.
    */
  def timeRange(
    s: String,
    e: String,
    tz: ZoneId = ZoneOffset.UTC,
    refs: Map[String, ZonedDateTime] = Map.empty
  ): (Instant, Instant) = {
    val range = if (Strings.isRelativeDate(s, true) || s == "e") {
      require(!Strings.isRelativeDate(e, true), "start and end are both relative")
      val end = Strings.parseDate(e, tz, refs)
      val start = Strings.parseDate(s, tz, refs + ("e" -> end))
      start.toInstant -> end.toInstant
    } else {
      val start = Strings.parseDate(s, tz, refs)
      val end = Strings.parseDate(e, tz, refs + ("s" -> start))
      start.toInstant -> end.toInstant
    }
    require(isBeforeOrEqual(range._1, range._2), "end time is before start time")
    range
  }

  private def isBeforeOrEqual(s: Instant, e: Instant): Boolean = s.isBefore(e) || s.equals(e)

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
      case t if t % oneWeek == 0   => s"${t / oneWeek}w"
      case t if t % oneDay == 0    => s"${t / oneDay}d"
      case t if t % oneHour == 0   => s"${t / oneHour}h"
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
    s.replaceAll("\n\n+", "@@@").replaceAll("\n", " ").replaceAll("@@@", "\n\n")
  }

  /**
    * Left pad the input string with zeros to the specified width. This is
    * typically used as an alternative to performing zero padding using
    * `String.format`.
    */
  def zeroPad(s: String, width: Int): String = {
    val n = width - s.length
    if (n <= 0) s
    else {
      val builder = new java.lang.StringBuilder(width)
      var i = 0
      while (i < n) {
        builder.append('0')
        i += 1
      }
      builder.append(s)
      builder.toString
    }
  }

  /**
    * Convert integer value to hex string and zero pad. It is intended for positive values
    * and the integer value will be treated as unsigned.
    */
  def zeroPad(v: Int, width: Int): String = zeroPad(Integer.toHexString(v), width)

  /**
    * Convert long value to hex string and zero pad. It is intended for positive values
    * and the integer value will be treated as unsigned.
    */
  def zeroPad(v: Long, width: Int): String = zeroPad(java.lang.Long.toHexString(v), width)

  /** Convert BigInteger value to hex string and zero pad. */
  def zeroPad(v: BigInteger, width: Int): String = zeroPad(v.toString(16), width)

  // Pre-computed set of hex strings for each byte value
  private val byteHexStrings = {
    (0 until 256).map(i => zeroPad(Integer.toHexString(i), 2)).toArray
  }

  /**
    * Convert integer represented as a byte array to a hex string and zero pad. This can be
    * used to avoid a conversion to BigInteger if the hex string is the only result needed.
    * The minimum padding width is 2, smaller values will get ignored.
    */
  def zeroPad(v: Array[Byte], width: Int): String = {
    val n = width - 2 * v.length
    val builder = new java.lang.StringBuilder(math.max(width, 2 * v.length))
    var i = 0
    while (i < n) {
      builder.append('0')
      i += 1
    }
    i = 0
    while (i < v.length) {
      val idx = java.lang.Byte.toUnsignedInt(v(i))
      builder.append(byteHexStrings(idx))
      i += 1
    }
    builder.toString
  }
}

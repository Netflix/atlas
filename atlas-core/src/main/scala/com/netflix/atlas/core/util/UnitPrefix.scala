/*
 * Copyright 2014-2022 Netflix, Inc.
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

object UnitPrefix {

  import java.lang.{Double => JDouble}

  // format: off

  val one   = UnitPrefix("", "", 1.0)

  val deca  = UnitPrefix("da", "deca",  1.0e1)
  val hecto = UnitPrefix("h",  "hecto", 1.0e2)
  val kilo  = UnitPrefix("k",  "kilo",  1.0e3)
  val mega  = UnitPrefix("M",  "mega",  1.0e6)
  val giga  = UnitPrefix("G",  "giga",  1.0e9)
  val tera  = UnitPrefix("T",  "tera",  1.0e12)
  val peta  = UnitPrefix("P",  "peta",  1.0e15)
  val exa   = UnitPrefix("E",  "exa",   1.0e18)
  val zetta = UnitPrefix("Z",  "zetta", 1.0e21)
  val yotta = UnitPrefix("Y",  "yotta", 1.0e24)

  val deci  = UnitPrefix("d",      "deci",  1.0e-1)
  val centi = UnitPrefix("c",      "centi", 1.0e-2)
  val milli = UnitPrefix("m",      "milli", 1.0e-3)
  val micro = UnitPrefix("\u03BC", "micro", 1.0e-6)
  val nano  = UnitPrefix("n",      "nano",  1.0e-9)
  val pico  = UnitPrefix("p",      "pico",  1.0e-12)
  val femto = UnitPrefix("f",      "femto", 1.0e-15)
  val atto  = UnitPrefix("a",      "atto",  1.0e-18)
  val zepto = UnitPrefix("z",      "zepto", 1.0e-21)
  val yocto = UnitPrefix("y",      "yocto", 1.0e-24)

  val kibi  = UnitPrefix("Ki",  "kibi",  1024.0)
  val mebi  = UnitPrefix("Mi",  "mebi",  kibi.factor * 1024.0)
  val gibi  = UnitPrefix("Gi",  "gibi",  mebi.factor * 1024.0)
  val tebi  = UnitPrefix("Ti",  "tebi",  gibi.factor * 1024.0)
  val pebi  = UnitPrefix("Pi",  "pebi",  tebi.factor * 1024.0)
  val exbi  = UnitPrefix("Ei",  "exbi",  pebi.factor * 1024.0)
  val zebi  = UnitPrefix("Zi",  "zebi",  exbi.factor * 1024.0)
  val yobi  = UnitPrefix("Yi",  "yobi",  zebi.factor * 1024.0)

  val picos  = UnitPrefix("ps",      "picos",  1.0e-12)
  val nanos  = UnitPrefix("ns",      "nanos",  1.0e-9)
  val micros = UnitPrefix("\u03BCs", "micros", 1.0e-6)
  val millis = UnitPrefix("ms",      "millis", 1.0e-3)
  val sec    = UnitPrefix("s",       "sec",    1.0)
  val min    = UnitPrefix("m",       "min",    60.0)
  val hour   = UnitPrefix("h",       "hour",   3600.0)
  val day    = UnitPrefix("d",       "day",    hour.factor * 24)
  val week   = UnitPrefix("w",       "week",   day.factor * 7)
  val year   = UnitPrefix("y",       "year",   day.factor * 365)

  // format: on

  val binaryPrefixes: List[UnitPrefix] = List(kibi, mebi, gibi, tebi, pebi, exbi, zebi, yobi)

  val durationSmallPrefixes: List[UnitPrefix] = List(millis, micros, nanos, picos)
  val durationBigPrefixes: List[UnitPrefix] = List(year, week, day, hour, min, sec)

  private val maxValue = 1e27
  private val minValue = 1e-27

  private val decimalBigPrefixes = List(yotta, zetta, exa, peta, tera, giga, mega, kilo)
  private val decimalSmallPrefixes = List(milli, micro, nano, pico, femto, atto, zepto, yocto)

  private val binaryBigPrefixes = binaryPrefixes.reverse

  def format(v: Double, fmtstr: String = "%.1f%s", scifmt: String = "%.0e"): String = {
    decimal(v).format(v, fmtstr, scifmt)
  }

  private def hasExtremeExponent(v: Double): Boolean = {
    val d = math.abs(v)
    JDouble.isFinite(v) && (isLarge(d) || isSmall(d))
  }

  private def isLarge(v: Double): Boolean = v >= maxValue

  private def isSmall(v: Double): Boolean = v <= minValue && v >= Double.MinPositiveValue

  /** Returns an appropriate decimal prefix for `value`. */
  def decimal(value: Double): UnitPrefix = {
    math.abs(value) match {
      case v if isNearlyZero(v)      => one
      case v if !JDouble.isFinite(v) => one
      case v if v >= kilo.factor     => decimalBigPrefixes.find(_.factor <= v).getOrElse(yotta)
      case v if v < 1.0              => decimalSmallPrefixes.find(_.factor <= v).getOrElse(yocto)
      case _                         => one
    }
  }

  /**
    * Returns an appropriate binary prefix for `value`. If the value is less than 1, then we
    * fall back to using the decimal small prefixes. It is expected that binary prefixes would
    * only get used with data greater than or equal to a byte.
    */
  def binary(value: Double): UnitPrefix = {
    math.abs(value) match {
      case v if isNearlyZero(v)      => one
      case v if !JDouble.isFinite(v) => one
      case v if v >= kibi.factor     => binaryBigPrefixes.find(_.factor <= v).getOrElse(yobi)
      case v if v < 1.0              => decimalSmallPrefixes.find(_.factor <= v).getOrElse(yocto)
      case _                         => one
    }
  }

  /**
    * Returns an appropriate duration prefix for `value`.
    */
  def duration(value: Double): UnitPrefix = {
    math.abs(value) match {
      case v if isNearlyZero(v)      => sec
      case v if !JDouble.isFinite(v) => sec
      case v if v >= sec.factor =>
        durationBigPrefixes.find(_.factor <= v).getOrElse(year)
      case v if v < 1.0 =>
        durationSmallPrefixes.find(_.factor <= v).getOrElse(picos)
      case _ => sec
    }
  }

  /** Returns an appropriate decimal prefix for `value`. */
  def forRange(value: Double, digits: Double): UnitPrefix = {
    val f = math.pow(10.0, digits)

    def withinRange(prefix: UnitPrefix, v: Double): Boolean = {
      val a = math.abs(v)
      a >= prefix.factor / f && a < prefix.factor * f
    }
    math.abs(value) match {
      case v if isNearlyZero(v)      => one
      case v if !JDouble.isFinite(v) => one
      case v if withinRange(one, v)  => one
      case v if v >= kilo.factor / f =>
        decimalBigPrefixes.reverse.find(withinRange(_, v)).getOrElse(yotta)
      case v if v < 1.0 / f => decimalSmallPrefixes.find(withinRange(_, v)).getOrElse(yocto)
      case _                => one
    }
  }

  /** Returns an appropriate binary prefix for `value`. */
  def binaryRange(value: Double, digits: Double): UnitPrefix = {
    val f = math.pow(10.0, digits)

    def withinRange(prefix: UnitPrefix, v: Double): Boolean = {
      val a = math.abs(v)
      a >= prefix.factor / f && a < prefix.factor * f
    }
    math.abs(value) match {
      case v if isNearlyZero(v)      => one
      case v if !JDouble.isFinite(v) => one
      case v if withinRange(one, v)  => one
      case v if v >= kibi.factor / f => binaryPrefixes.find(withinRange(_, v)).getOrElse(yobi)
      case v if v < 1.0 / f => decimalSmallPrefixes.find(withinRange(_, v)).getOrElse(yocto)
      case _                => one
    }
  }

  def durationRange(value: Double): UnitPrefix = {
    math.abs(value) match {
      case v if isNearlyZero(v)          => sec
      case v if !JDouble.isFinite(v)     => sec
      case v if v < sec.factor && v >= 1 => sec
      case v if v >= sec.factor =>
        var last = sec
        var found: UnitPrefix = null
        durationBigPrefixes.reverse.foreach { prefix =>
          if (prefix != last) {
            if (found == null && v >= last.factor && v < prefix.factor) {
              found = last
            }
            last = prefix
          }
        }
        if (found == null) year else found
      case v if v < 1.0 => durationSmallPrefixes.find(v > _.factor).getOrElse(nanos)
      case _            => sec
    }
  }

  private def isNearlyZero(v: Double): Boolean = {
    v.isNaN || scala.math.abs(v - 0.0) < 1e-12
  }
}

/**
  * Common prefixes used for units or human readable strings.
  *
  * @param symbol  the symbol shown for the prefix
  * @param text    text for the prefix
  * @param factor  the multiplication factor for the prefix
  */
case class UnitPrefix(symbol: String, text: String, factor: Double) {

  def format(value: Double, fmtstr: String): String = {
    fmtstr.format(value / factor, symbol)
  }

  def format(v: Double, fmtstr: String = "%.1f%s", scifmt: String = "%.0e"): String = {
    if (UnitPrefix.hasExtremeExponent(v)) {
      val fmt = if (v >= 0.0) " " + scifmt else scifmt
      fmt.format(v)
    } else {
      format(v, fmtstr)
    }
  }

  def next: UnitPrefix = {
    UnitPrefix.decimalSmallPrefixes.reverse.find(_.factor > factor).getOrElse {
      UnitPrefix.decimalBigPrefixes.reverse.find(_.factor > factor).getOrElse(UnitPrefix.yotta)
    }
  }
}

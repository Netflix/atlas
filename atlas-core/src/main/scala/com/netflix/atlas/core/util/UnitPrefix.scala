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

object UnitPrefix {

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

  private val decimalBigPrefixes = List(yotta, zetta, exa, peta, tera, giga, mega, kilo)
  private val decimalSmallPrefixes = List(milli, micro, nano, pico, femto, atto, zepto, yocto)

  def format(v: Double, fmtstr: String = "%.1f%s"): String = {
    val unit = decimal(v)
    unit.format(v / unit.factor, fmtstr)
  }

  /** Returns an appropriate prefix for `value`. */
  def decimal(value: Double): UnitPrefix = {
    math.abs(value) match {
      case v if isNearlyZero(v)   => one
      case v if v >= kilo.factor  => decimalBigPrefixes.find(_.factor <= v).getOrElse(yotta)
      case v if v < 1.0           => decimalSmallPrefixes.find(_.factor <= v).getOrElse(yocto)
      case _                      => one
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
    fmtstr.format(value, symbol)
  }
}

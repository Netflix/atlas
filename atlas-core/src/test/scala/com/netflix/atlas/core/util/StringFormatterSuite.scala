/*
 * Copyright 2014-2026 Netflix, Inc.
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

import munit.FunSuite

class StringFormatterSuite extends FunSuite {

  // Basic string formatting
  test("format: simple string") {
    assertEquals(StringFormatter.format("hello"), "hello")
  }

  test("format: string with %s") {
    assertEquals(StringFormatter.format("hello %s", "world"), "hello world")
  }

  test("format: multiple strings") {
    assertEquals(StringFormatter.format("%s %s", "hello", "world"), "hello world")
  }

  test("format: literal %%") {
    assertEquals(StringFormatter.format("100%% done"), "100% done")
  }

  test("format: mixed literal and placeholder") {
    assertEquals(StringFormatter.format("%%s is %s", "this"), "%s is this")
  }

  // Integer formatting
  test("format: signed decimal %d") {
    assertEquals(StringFormatter.format("%d", 42), "42")
    assertEquals(StringFormatter.format("%d", -42), "-42")
  }

  test("format: unsigned decimal %u") {
    assertEquals(StringFormatter.format("%u", 42), "42")
    assertEquals(StringFormatter.format("%u", -1), "18446744073709551615")
  }

  test("format: hex lowercase %x") {
    assertEquals(StringFormatter.format("%x", 255), "ff")
    assertEquals(StringFormatter.format("%x", 0), "0")
  }

  test("format: hex uppercase %X") {
    assertEquals(StringFormatter.format("%X", 255), "FF")
    assertEquals(StringFormatter.format("%X", 0), "0")
  }

  // Float formatting
  test("format: float %f default precision") {
    assertEquals(StringFormatter.format("%f", 3.14159), "3.141590")
  }

  test("format: float %f with precision") {
    assertEquals(StringFormatter.format("%.2f", 3.14159), "3.14")
    assertEquals(StringFormatter.format("%.0f", 3.14159), "3")
  }

  // Width
  test("format: width right align") {
    assertEquals(StringFormatter.format("%5s", "hi"), "   hi")
    assertEquals(StringFormatter.format("%5d", 42), "   42")
  }

  test("format: width left align") {
    assertEquals(StringFormatter.format("%-5s", "hi"), "hi   ")
    assertEquals(StringFormatter.format("%-5d", 42), "42   ")
  }

  test("format: width zero padding") {
    assertEquals(StringFormatter.format("%05d", 42), "00042")
    assertEquals(StringFormatter.format("%05x", 255), "000ff")
  }

  test("format: width zero padding with sign") {
    assertEquals(StringFormatter.format("%05d", -42), "-0042")
    assertEquals(StringFormatter.format("%+05d", 42), "+0042")
  }

  test("format: width ignores zero flag for left align") {
    assertEquals(StringFormatter.format("%-05d", 42), "42   ")
  }

  // Precision
  test("format: string precision truncates") {
    assertEquals(StringFormatter.format("%.3s", "hello"), "hel")
    assertEquals(StringFormatter.format("%.10s", "hi"), "hi")
  }

  test("format: string width and precision") {
    assertEquals(StringFormatter.format("%8.3s", "hello"), "     hel")
  }

  // Flags
  test("format: show sign +") {
    assertEquals(StringFormatter.format("%+d", 42), "+42")
    assertEquals(StringFormatter.format("%+d", -42), "-42")
    assertEquals(StringFormatter.format("%+f", 3.14), "+3.140000")
  }

  test("format: space sign") {
    assertEquals(StringFormatter.format("% d", 42), " 42")
    assertEquals(StringFormatter.format("% d", -42), "-42")
  }

  test("format: space sign ignored if + present") {
    assertEquals(StringFormatter.format("%+ d", 42), "+42")
  }

  // Positional arguments
  test("format: positional index") {
    assertEquals(StringFormatter.format("%2$s %1$s", "world", "hello"), "hello world")
  }

  test("format: positional with formatting") {
    assertEquals(StringFormatter.format("%2$d + %1$d = %3$d", 1, 2, 3), "2 + 1 = 3")
  }

  test("format: mixed positional and auto index") {
    assertEquals(StringFormatter.format("%s %2$s %s", "a", "b", "c"), "a b c")
  }

  // Security limits
  test("format: width at max limit") {
    val result = StringFormatter.format("%256s", "x")
    assertEquals(result.length, 256)
  }

  test("format: width exceeds limit") {
    intercept[IllegalArgumentException] {
      StringFormatter.format("%257s", "x")
    }
  }

  test("format: width zero not allowed for strings") {
    intercept[IllegalArgumentException] {
      StringFormatter.format("%0s", "x")
    }
  }

  test("format: string precision at max limit") {
    val longStr = "x" * 300
    val result = StringFormatter.format("%.256s", longStr)
    assertEquals(result.length, 256)
  }

  test("format: string precision exceeds limit") {
    intercept[IllegalArgumentException] {
      StringFormatter.format("%.257s", "x")
    }
  }

  test("format: float precision at max limit") {
    StringFormatter.format("%.32f", 3.14) // Should not throw
  }

  test("format: float precision exceeds limit") {
    intercept[IllegalArgumentException] {
      StringFormatter.format("%.33f", 3.14)
    }
  }

  test("format: precision not allowed for integers") {
    intercept[IllegalArgumentException] {
      StringFormatter.format("%.2d", 42)
    }
  }

  // Error cases
  test("format: missing argument") {
    intercept[IllegalArgumentException] {
      StringFormatter.format("%s %s", "only one")
    }
  }

  test("format: invalid format spec") {
    intercept[IllegalArgumentException] {
      StringFormatter.format("%z", "x")
    }
  }

  test("format: positional index out of bounds") {
    intercept[IllegalArgumentException] {
      StringFormatter.format("%3$s", "a", "b")
    }
  }

  test("format: incompatible type for integer") {
    intercept[IllegalArgumentException] {
      StringFormatter.format("%d", "not a number")
    }
  }

  test("format: incompatible type for float") {
    intercept[IllegalArgumentException] {
      StringFormatter.format("%f", "not a number")
    }
  }

  // Complex examples
  test("format: complex pattern") {
    val result = StringFormatter.format(
      "Server: %1$s, Port: %2$05d, Status: %3$-8s, CPU: %4$6.2f%%",
      "localhost",
      8080,
      "running",
      42.5
    )
    assertEquals(result, "Server: localhost, Port: 08080, Status: running , CPU:  42.50%")
  }

  test("format: table-like output") {
    val header = StringFormatter.format("%-10s %8s %8s", "Name", "Count", "Percent")
    val row1 = StringFormatter.format("%-10s %8d %7.2f%%", "success", 1234, 98.5)
    val row2 = StringFormatter.format("%-10s %8d %7.2f%%", "error", 19, 1.5)
    assertEquals(header, "Name          Count  Percent")
    assertEquals(row1, "success        1234   98.50%")
    assertEquals(row2, "error            19    1.50%")
  }

  test("format: hex addresses") {
    assertEquals(StringFormatter.format("0x%08X", 0xDEADBEEFL), "0xDEADBEEF")
    assertEquals(StringFormatter.format("0x%08x", 0xDEADBEEFL), "0xdeadbeef")
  }

  test("format: byte values") {
    assertEquals(StringFormatter.format("%d", 127.toByte), "127")
    assertEquals(StringFormatter.format("%d", -1.toByte), "-1")
    assertEquals(StringFormatter.format("%u", -1.toByte), "18446744073709551615")
  }

  test("format: short values") {
    assertEquals(StringFormatter.format("%d", 32767.toShort), "32767")
    assertEquals(StringFormatter.format("%d", -1.toShort), "-1")
  }

  test("format: long values") {
    assertEquals(StringFormatter.format("%d", Long.MaxValue), "9223372036854775807")
    assertEquals(StringFormatter.format("%d", Long.MinValue), "-9223372036854775808")
  }

  test("format: double values") {
    assertEquals(StringFormatter.format("%.2f", 0.0), "0.00")
    assertEquals(StringFormatter.format("%.2f", -0.0), "0.00")
    assertEquals(StringFormatter.format("%f", Double.MinPositiveValue), "0.000000")
  }

  // Edge cases
  test("format: empty pattern") {
    assertEquals(StringFormatter.format(""), "")
  }

  test("format: no placeholders") {
    assertEquals(StringFormatter.format("no placeholders here"), "no placeholders here")
  }

  test("format: string with numbers") {
    assertEquals(StringFormatter.format("%d", "42"), "42")
    assertEquals(StringFormatter.format("%f", "3.14"), "3.140000")
  }

  test("format: multiple flags") {
    assertEquals(StringFormatter.format("%+010d", 42), "+000000042")
    assertEquals(StringFormatter.format("%-+10d", 42), "+42       ")
  }

  // Security tests - DoS prevention
  test("format: prevents DoS with huge width") {
    intercept[IllegalArgumentException] {
      StringFormatter.format("%999999s", "x")
    }
  }

  test("format: prevents DoS with huge precision") {
    intercept[IllegalArgumentException] {
      StringFormatter.format("%.999999s", "x" * 1000000)
    }
  }

  test("format: safe within limits") {
    // These should work fine and not cause memory issues
    val result1 = StringFormatter.format("%256s", "x")
    assertEquals(result1.length, 256)
    val result2 = StringFormatter.format("%.256s", "x" * 300)
    assertEquals(result2.length, 256)
  }
}

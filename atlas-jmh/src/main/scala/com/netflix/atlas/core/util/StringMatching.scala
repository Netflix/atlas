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

import java.util.regex.Pattern

import com.netflix.spectator.impl.PatternMatcher
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Threads
import org.openjdk.jmh.infra.Blackhole

/**
  * There was an old suggestion that max via bit manip would be faster and avoid branch instructions. That
  * doesn't appear to be the case:
  *
  * ```
  * > run -wi 10 -i 10 -f1 -t1 .*StringMatching.*
  * ```
  */
@State(Scope.Thread)
class StringMatching {

  private val value = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_."
  private val prefix = value
  private val substr = "XYZ"

  private val flags = Pattern.CASE_INSENSITIVE

  private val startsWithMatcher = PatternMatcher.compile(s"^$prefix")
  private val ssIndexOfMatcher = PatternMatcher.compile(substr)
  private val icIndexOfMatcher = PatternMatcher.compile(substr).ignoreCase()
  private val regexMatcher = Pattern.compile(s"^$prefix")
  private val icRegexMatcher = Pattern.compile(s"^$prefix", flags)
  private val ssRegexMatcher = Pattern.compile(s"^.*$substr")
  private val ssRegexMatcher2 = Pattern.compile(substr)
  private val ssICRegexMatcher = Pattern.compile(s"^.*$substr", flags)
  private val ssICRegexMatcher2 = Pattern.compile(substr, flags)

  @Threads(1)
  @Benchmark
  def testPrefixRegex(bh: Blackhole): Unit = {
    bh.consume(regexMatcher.matcher(value).find())
  }

  // TODO
  @Threads(1)
  @Benchmark
  def testPrefixRegexNewMatcher(bh: Blackhole): Unit = {
    bh.consume(regexMatcher.matcher(value).find)
  }

  @Threads(1)
  @Benchmark
  def testPrefixStartsWith(bh: Blackhole): Unit = {
    bh.consume(startsWithMatcher.matches(value))
  }

  @Threads(1)
  @Benchmark
  def testPrefixICRegex(bh: Blackhole): Unit = {
    bh.consume(icRegexMatcher.matcher(value).find())
  }

  // regionMatches is slower than regex when ignoring case. This seems to be mostly due to
  // the regex doing simple ascii conversion for case (not using UNICODE_CASE flag) where the
  // String class is trying to do a unicode aware case conversion.
  //
  // On jdk11 the note above is no longer true if the encoding for the String is latin1.
  @Threads(1)
  @Benchmark
  def testPrefixICStartsWith(bh: Blackhole): Unit = {
    bh.consume(value.regionMatches(true, 0, prefix, 0, prefix.length))
  }

  @Threads(1)
  @Benchmark
  def testSubstrIndexOf(bh: Blackhole): Unit = {
    bh.consume(ssIndexOfMatcher.matches(value))
  }

  @Threads(1)
  @Benchmark
  def testSubstrRegex(bh: Blackhole): Unit = {
    bh.consume(ssRegexMatcher.matcher(value).find())
  }

  @Threads(1)
  @Benchmark
  def testSubstrRegex2(bh: Blackhole): Unit = {
    bh.consume(ssRegexMatcher2.matcher(value).find())
  }

  @Threads(1)
  @Benchmark
  def testSubstrICIndexOf(bh: Blackhole): Unit = {
    bh.consume(icIndexOfMatcher.matches(value))
  }

  @Threads(1)
  @Benchmark
  def testSubstrICRegex(bh: Blackhole): Unit = {
    bh.consume(ssICRegexMatcher.matcher(value).find())
  }

  @Threads(1)
  @Benchmark
  def testSubstrICRegex2(bh: Blackhole): Unit = {
    bh.consume(ssICRegexMatcher2.matcher(value).find())
  }

}

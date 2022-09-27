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
package com.netflix.atlas.core.model

import com.netflix.atlas.core.model.Query.Regex
import com.netflix.atlas.core.stacklang.Interpreter
import munit.FunSuite

class QueryVocabularySuite extends FunSuite {

  val interpreter = new Interpreter(QueryVocabulary.allWords)

  test("contains, escape") {
    var exp = interpreter.execute("a,^$.?*+[](){}\\#&!%,:contains").stack(0)
    assertEquals(
      exp.asInstanceOf[Regex].pattern.toString,
      ".*\\^\\$\\.\\?\\*\\+\\[\\]\\(\\)\\{\\}\\\\#&!%"
    )
    exp = interpreter.execute("a,space and ~,:contains").stack(0)
    assertEquals(
      exp.asInstanceOf[Regex].pattern.toString,
      ".*space\\u0020and\\u0020~"
    )
  }

  test("contains, matches escaped") {
    val q = interpreter
      .execute("foo,my $var. [work-in-progress],:contains")
      .stack(0)
      .asInstanceOf[Regex]
    assert(q.matches(Map("foo" -> "my $var. [work-in-progress]")))
    assert(q.matches(Map("foo" -> "initialize my $var. [work-in-progress], not a range")))
    assert(!q.matches(Map("foo" -> "my $var. [work-in progress]")))
  }

}

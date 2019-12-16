/*
 * Copyright 2014-2019 Netflix, Inc.
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
package com.netflix.atlas.akka

import java.nio.charset.StandardCharsets

import akka.http.scaladsl.model.IllegalUriException
import akka.http.scaladsl.model.Uri
import org.scalatest.funsuite.AnyFunSuite

class UriParsingSuite extends AnyFunSuite {

  private def query(mode: Uri.ParsingMode): String = {
    Uri("/foo?regex=a|b|c").query(StandardCharsets.UTF_8, mode).get("regex").get
  }

  test("relaxed: regex with |") {
    assert(query(Uri.ParsingMode.Relaxed) === "a|b|c")
  }

  test("strict: regex with |") {
    intercept[IllegalUriException] {
      assert(query(Uri.ParsingMode.Strict) === "a|b|c")
    }
  }
}

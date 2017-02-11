/*
 * Copyright 2014-2017 Netflix, Inc.
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
package com.netflix.atlas.json

import org.scalatest.FunSuite

import scala.util.Try

class ReflectionSuite extends FunSuite {
  import ReflectionSuite._

  test("create instance") {
    val desc = Reflection.createDescription(classOf[Simple])
    val args = desc.newInstanceArgs
    assert(desc.newInstance(args) === Simple(27, null))

    desc.setField(args, "foo", 42)
    assert(desc.newInstance(args) === Simple(42, null))

    desc.setField(args, "bar", "abc")
    assert(desc.newInstance(args) === Simple(42, "abc"))
  }

  test("create instance, additional field") {
    val desc = Reflection.createDescription(classOf[Simple])
    val args = desc.newInstanceArgs
    desc.setField(args, "notPresent", 42)
    assert(desc.newInstance(args) === Simple(27, null))
  }

  test("create instance, invalid type") {
    val desc = Reflection.createDescription(classOf[Simple])
    val args = desc.newInstanceArgs
    desc.setField(args, "bar", 42)
    intercept[IllegalArgumentException] { desc.newInstance(args) }
  }

  test("isCaseClass") {
    assert(Reflection.isCaseClass(classOf[Simple]) === true)
    assert(Reflection.isCaseClass(classOf[Bar]) === false)
  }

  test("isCaseClass Option") {
    assert(Reflection.isCaseClass(classOf[Option[_]]) === false)
  }

  test("isCaseClass Either") {
    assert(Reflection.isCaseClass(classOf[Either[_, _]]) === false)
  }

  test("isCaseClass Try") {
    assert(Reflection.isCaseClass(classOf[Try[_]]) === false)
  }
}

object ReflectionSuite {

  case class SimpleInner(foo: Int, bar: String = "abc")

  class Bar(foo: Int)

}

case class Simple(foo: Int = 27, bar: String)

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
package com.netflix.atlas.json

import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.exc.ValueInstantiationException
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.JavaTypeable
import munit.FunSuite

class CaseClassDeserializerSuite extends FunSuite {

  import CaseClassDeserializerSuite.*

  private val mapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)

  def decode[T: JavaTypeable](json: String): T = {
    val javaType = implicitly[JavaTypeable[T]].asJavaType(mapper.getTypeFactory)
    mapper.readValue[T](json, javaType)
  }

  test("read simple object") {
    val expected = SimpleObject(123, "abc", Some("def"))
    val actual = decode[SimpleObject]("""{"foo": 123, "bar": "abc", "baz": "def"}""")
    assertEquals(actual, expected)
  }

  test("read array") {
    intercept[JsonMappingException] {
      decode[SimpleObject]("""[]""")
    }
  }

  test("read int") {
    intercept[JsonMappingException] {
      decode[SimpleObject]("""42""")
    }
  }

  test("invalid type for field (quoted number)") {
    val expected = SimpleObject(123, "abc", Some("def"))
    val actual = decode[SimpleObject]("""{"foo": "123", "bar": "abc", "baz": "def"}""")
    assertEquals(actual, expected)
  }

  test("invalid type for field (invalid number)") {
    intercept[JsonMappingException] {
      decode[SimpleObject]("""{"foo": "that", "bar": "abc", "baz": "def"}""")
    }
  }

  test("read simple object missing Option") {
    val expected = SimpleObject(42, "abc", None)
    val actual = decode[SimpleObject]("""{"foo": 42, "bar": "abc"}""")
    assertEquals(actual, expected)
  }

  test("read simple object missing required") {
    val expected = SimpleObject(123, null, Some("def"))
    val actual = decode[SimpleObject]("""{"foo": 123, "baz": "def"}""")
    assertEquals(actual, expected)
  }

  test("read simple object with defaults") {
    val expected = SimpleObjectWithDefaults()
    val actual = decode[SimpleObjectWithDefaults]("""{}""")
    assertEquals(actual, expected)
  }

  test("read simple object with overridden defaults") {
    val expected = SimpleObjectWithDefaults(foo = 21)
    val actual = decode[SimpleObjectWithDefaults]("""{"foo": 21}""")
    assertEquals(actual, expected)
  }

  test("read simple object with one default") {
    val expected = SimpleObjectWithOneDefault(21)
    val actual = decode[SimpleObjectWithOneDefault]("""{"foo": 21}""")
    assertEquals(actual, expected)
  }

  test("read simple object with one default missing required") {
    val expected = SimpleObjectWithOneDefault(0)
    val actual = decode[SimpleObjectWithOneDefault]("""{}""")
    assertEquals(actual, expected)
  }

  test("read simple object with validation") {
    val expected = SimpleObjectWithValidation("abc")
    val actual = decode[SimpleObjectWithValidation]("""{"foo": "abc"}""")
    assertEquals(actual, expected)
  }

  test("read simple object with error") {
    intercept[ValueInstantiationException] {
      decode[SimpleObjectUnknownError]("""{"foo": "abc"}""")
    }
  }

  test("read with Option[Int] field") {
    val expected = OptionInt(Some(42))
    val actual = decode[OptionInt]("""{"v":42}""")
    assertEquals(actual, expected)
  }

  test("read with Option[Int] field, null") {
    val expected = OptionInt(None)
    val actual = decode[OptionInt]("""{"v":null}""")
    assertEquals(actual, expected)
  }

  test("read with Option[Long] field") {
    val expected = OptionLong(Some(42L))
    val actual = decode[OptionLong]("""{"v":42}""")
    assertEquals(actual, expected)
  }

  test("read with List[Option[Int]] field") {
    val expected = ListOptionInt(List(Some(42), Some(21)))
    val actual = decode[ListOptionInt]("""{"v":[42, 21]}""")
    assertEquals(actual, expected)
  }

  test("read with List[String] field") {
    val expected = ListString(List("a", "b"))
    val actual = decode[ListString]("""{"vs":["a", "b"]}""")
    assertEquals(actual, expected)
  }

  test("read with List[String] field, null") {
    // Behavior changed in 2.19.0: https://github.com/FasterXML/jackson-module-scala/issues/722
    val expected = ListString(Nil)
    val actual = decode[ListString]("""{"vs":null}""")
    assertEquals(actual, expected)
  }

  test("read with List[String] field, with default, null") {
    val expected = ListStringDflt()
    val actual = decode[ListStringDflt]("""{"vs":null}""")
    assertEquals(actual, expected)
  }

  test("generics") {
    val expected = Outer(List(List(Inner("a"), Inner("b")), List(Inner("c"))))
    val actual = decode[Outer]("""{"vs":[[{"v":"a"},{"v":"b"}],[{"v":"c"}]]}""")
    assertEquals(actual, expected)
  }

  test("generics 2") {
    val expected = OuterT(List(List(Inner("a"), Inner("b")), List(Inner("c"))))
    val actual =
      decode[OuterT[List[List[Inner]]]]("""{"vs":[[{"v":"a"},{"v":"b"}],[{"v":"c"}]]}""")
    assertEquals(actual, expected)
  }

  test("honors @JsonAlias annotation") {
    val expected = AliasAnno("foo")
    val actual = decode[AliasAnno]("""{"v":"foo"}""")
    assertEquals(actual, expected)
  }

  test("honors @JsonProperty annotation") {
    val expected = PropAnno("foo")
    val actual = decode[PropAnno]("""{"v":"foo"}""")
    assertEquals(actual, expected)
  }

  test("honors @JsonDeserialize.contentAs annotation") {
    val expected = DeserAnno(Some(42L))
    val actual = decode[DeserAnno]("""{"value":42}""")
    assertEquals(actual, expected)
    // Line above will pass even if a java.lang.Integer is created. The
    // check below will fail with:
    // java.lang.ClassCastException: java.lang.Integer cannot be cast to java.lang.Long
    assertEquals(actual.value.get.asInstanceOf[java.lang.Long], java.lang.Long.valueOf(42L))
  }

  test("honors @JsonDeserialize.using annotation") {
    val expected = DeserUsingAnno(43L)
    val actual = decode[DeserUsingAnno]("""{"value":42}""")
    assertEquals(actual, expected)
  }
}

object CaseClassDeserializerSuite {

  case class SimpleObject(foo: Int, bar: String, baz: Option[String])

  case class SimpleObjectWithDefaults(foo: Int = 42, bar: String = "abc")

  case class SimpleObjectWithOneDefault(foo: Int, bar: String = "abc")

  case class SimpleObjectWithValidation(foo: String) {
    require(foo != null)
  }

  case class SimpleObjectUnknownError(foo: String) {
    throw new IllegalArgumentException
  }

  case class OptionInt(v: Option[Int])

  case class OptionLong(v: Option[Long])

  case class ListOptionInt(v: List[Option[Int]])

  case class ListString(vs: List[String])

  case class ListStringDflt(vs: List[String] = Nil)

  case class Inner(v: String)

  case class Outer(vs: List[List[Inner]])

  case class OuterT[T](vs: T)

  case class AliasAnno(@JsonAlias(Array("v")) value: String)

  case class PropAnno(@JsonProperty("v") value: String)

  case class DeserAnno(@JsonDeserialize(contentAs = classOf[java.lang.Long]) value: Option[Long])

  case class DeserUsingAnno(@JsonDeserialize(`using` = classOf[AddOneDeserializer]) value: Long)

  class AddOneDeserializer extends JsonDeserializer[java.lang.Long] {

    override def deserialize(p: JsonParser, ctxt: DeserializationContext): java.lang.Long = {
      val v = p.getLongValue
      p.nextToken()
      java.lang.Long.valueOf(v + 1)
    }
  }
}

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

import java.math.BigInteger
import java.util
import java.util.Optional
import java.util.regex.Pattern
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.node.ObjectNode
import munit.FunSuite

import java.time.Instant

/**
  * Test case for the capabilities we need from a json parser. Mostly to document what we are using
  * and ease transition to alternative libraries if we want to switch.
  */
class JsonSuite extends FunSuite {

  import java.lang.Double as JDouble

  import com.netflix.atlas.json.Json.*

  test("garbage") {
    intercept[JsonParseException] { decode[Boolean]("true dklfjal;k;hfnklanf'") }
    intercept[IllegalArgumentException] { decode[Map[String, AnyRef]]("""{"f":"b"} {"b":"d"}""") }
  }

  test("true") {
    val v = true
    assertEquals(encode(v), "true")
    assertEquals(decode[Boolean](encode(v)), v)
  }

  test("false") {
    val v = false
    assertEquals(encode(v), "false")
    assertEquals(decode[Boolean](encode(v)), v)
  }

  test("byte") {
    val v = 42.asInstanceOf[Byte]
    assertEquals(encode(v), "42")
    assertEquals(decode[Byte](encode(v)), v)
  }

  test("short") {
    val v = 42.asInstanceOf[Short]
    assertEquals(encode(v), "42")
    assertEquals(decode[Short](encode(v)), v)
  }

  test("int") {
    val v = 42
    assertEquals(encode(v), "42")
    assertEquals(decode[Int](encode(v)), v)
  }

  test("long") {
    val v = 42L
    assertEquals(encode(v), "42")
    assertEquals(decode[Long](encode(v)), v)
  }

  test("float") {
    val v = 42.0f
    assertEquals(encode(v), "42.0")
    assertEquals(decode[Float](encode(v)), v)
  }

  test("double") {
    val v = 42.0
    assertEquals(encode(v), "42.0")
    assertEquals(decode[Double](encode(v)), v)
  }

  // This is non-standard, but we prefer to differentiate from infinity or other special values
  test("double NaN") {
    val v = Double.NaN
    assertEquals(encode(v), "\"NaN\"")
    assert(JDouble.isNaN(decode[Double](encode(v))))
  }

  test("double array with string non numeric values") {
    val expected = Array(
      1.0,
      JDouble.NaN,
      JDouble.NEGATIVE_INFINITY,
      JDouble.POSITIVE_INFINITY,
      JDouble.POSITIVE_INFINITY
    )
    val vs = """[1.0,"NaN","-Infinity","+Infinity","Infinity"]"""
    assert(java.util.Arrays.equals(decode[Array[Double]](vs), expected))
  }

  test("object double array and string non numeric values") {
    val expected = Array(
      1.0,
      JDouble.NaN,
      JDouble.NEGATIVE_INFINITY,
      JDouble.POSITIVE_INFINITY,
      JDouble.POSITIVE_INFINITY
    )
    val vs = """{"values":[1.0,"NaN","-Infinity","+Infinity","Infinity"]}"""
    assert(java.util.Arrays.equals(decode[JsonSuiteArrayDouble](vs).values, expected))
  }

  test("double list with string non numeric values") {
    val expected = Array(
      1.0,
      JDouble.NaN,
      JDouble.NEGATIVE_INFINITY,
      JDouble.POSITIVE_INFINITY,
      JDouble.POSITIVE_INFINITY
    )
    val vs = """[1.0,"NaN","-Infinity","+Infinity","Infinity"]"""
    assert(java.util.Arrays.equals(decode[List[Double]](vs).toArray, expected))
  }

  test("object double list and non numeric values") {
    val expected = Array(
      1.0,
      JDouble.NaN,
      JDouble.NEGATIVE_INFINITY,
      JDouble.POSITIVE_INFINITY,
      JDouble.POSITIVE_INFINITY
    )
    val vs = """{"vs":[1.0,NaN,-Infinity,+Infinity,Infinity]}"""
    assert(java.util.Arrays.equals(decode[JsonSuiteListDouble](vs).vs.toArray, expected))
  }

  test("object double list and string non numeric values".ignore) {
    val expected = Array(
      1.0,
      JDouble.NaN,
      JDouble.NEGATIVE_INFINITY,
      JDouble.POSITIVE_INFINITY,
      JDouble.POSITIVE_INFINITY
    )
    val vs = """{"vs":[1.0,"NaN","-Infinity","+Infinity","Infinity"]}"""
    assert(java.util.Arrays.equals(decode[JsonSuiteListDouble](vs).vs.toArray, expected))
  }

  test("BigInteger") {
    val v = new BigInteger("42")
    assertEquals(encode(v), "42")
    assertEquals(decode[BigInteger](encode(v)), v)
  }

  test("String") {
    val v = "42"
    assertEquals(encode(v), "\"42\"")
    assertEquals(decode[String](encode(v)), v)
  }

  test("Pattern") {
    val v = Pattern.compile("^42.*")
    assertEquals(encode(v), "\"^42.*\"")
    assertEquals(decode[Pattern](encode(v)).toString, v.toString)
  }

  test("enum") {
    val v = JsonToken.NOT_AVAILABLE
    assertEquals(encode(v), "\"NOT_AVAILABLE\"")
    assertEquals(decode[JsonToken](encode(v)), v)
  }

  test("java8 Instant") {
    val v = java.time.Instant.parse("2012-01-13T04:37:52Z")
    assertEquals(encode(v), "1326429472000")
    assertEquals(decode[java.time.Instant](encode(v)), v)
  }

  test("java8 Duration") {
    val v = java.time.Duration.ofSeconds(42)
    assertEquals(encode(v), "42000")
    assertEquals(decode[java.time.Duration](encode(v)), v)
  }

  test("java8 Period") {
    val v = java.time.Period.ofDays(42)
    assertEquals(encode(v), "\"P42D\"")
    assertEquals(decode[java.time.Period](encode(v)), v)
  }

  test("java8 Optional[String] -- None") {
    val v = Optional.empty[String]()
    assertEquals(encode(v), "null")
    assertEquals(decode[Optional[String]](encode(v)), v)
  }

  test("java8 Optional[String] -- Some") {
    val v = Optional.of("42")
    assertEquals(encode(v), "\"42\"")
    assertEquals(decode[Optional[String]](encode(v)), v)
  }

  test("scala Option[String] -- None") {
    val v = None
    assertEquals(encode(v), "null")
    assertEquals(decode[Option[String]](encode(v)), v)
  }

  test("scala Option[String] -- Some") {
    val v = Some("42")
    assertEquals(encode(v), "\"42\"")
    assertEquals(decode[Option[String]](encode(v)), v)
  }

  test("scala Option[Int] -- None") {
    val v = None
    assertEquals(encode(v), "null")
    assertEquals(decode[Option[Int]](encode(v)), v)
  }

  test("scala Option[Int] -- Some") {
    val v = Some(42)
    assertEquals(encode(v), "42")
    assertEquals(decode[Option[Int]](encode(v)), v)
  }

  // Ugly nested generics, just to see if it would work
  test("scala List[Map[String, List[Option[Int]]]]") {
    val v = List(Map("foo" -> List(Some(42), None, Some(43))))
    assertEquals(encode(v), """[{"foo":[42,null,43]}]""")
    assertEquals(decode[List[Map[String, List[Option[Int]]]]](encode(v)), v)
  }

  test("scala List[Int]") {
    val v = List(42, 43, 44)
    assertEquals(encode(v), "[42,43,44]")
    assertEquals(decode[List[Int]](encode(v)), v)
  }

  test("scala Set[Int]") {
    val v = Set(42, 43, 44)
    assertEquals(encode(v), "[42,43,44]")
    assertEquals(decode[Set[Int]](encode(v)), v)
  }

  test("scala Map[String, Int]") {
    val v = Map("foo" -> 42)
    assertEquals(encode(v), "{\"foo\":42}")
    assertEquals(decode[Map[String, Int]](encode(v)), v)
  }

  test("scala Tuple2[String, Int]") {
    val v = "foo" -> 42
    assertEquals(encode(v), "[\"foo\",42]")
    assertEquals(decode[(String, Int)](encode(v)), v)
  }

  test("comments".ignore) {
    // UI now handles most config, comments aren't needed and trying to stick with standard
    // json if possible
    // factory.enable(ALLOW_COMMENTS) to enable in jackson if we want to reenable
    val v = Map("foo" -> 42, "bar" -> 43)
    val json = """
      /* Not standard, but often requested when used in config files */
      {
        "foo": 42,
        // Single line comment
        "bar": 43
      }
    """
    assertEquals(decode[Map[String, Int]](json), v)
  }

  // It seems like this manages to break the singleton property of the object...
  test("case object".ignore) {
    val v = JsonSuiteObject
    assertEquals(encode(v), """{}""")
    val result = decode[JsonSuiteObject.type](encode(v))
    assertEquals(result, v)
  }

  test("case class simple") {
    val v = JsonSuiteSimple(42, "forty-two")
    assertEquals(encode(v), """{"foo":42,"bar":"forty-two"}""")
    assertEquals(decode[JsonSuiteSimple](encode(v)), v)
  }

  // We don't want null values for fields of case classes, if a field is optional it should be
  // specified as Option[T].
  test("case class simple -- missing field".ignore) {
    val v = JsonSuiteSimple(42, "forty-two")
    val json = """{"foo":42}"""
    assertEquals(decode[JsonSuiteSimple](json), v)
  }

  // We want to ignore unknown fields so we have the option of introducing new fields without
  // breaking old code that may not supported yet. Hopefully used sparingly, but nice to know we
  // have the option.
  test("case class simple -- unknown field") {
    val v = JsonSuiteSimple(42, "forty-two")
    val json = """{"foo":42,"bar":"forty-two","baz":"new field"}"""
    assertEquals(decode[JsonSuiteSimple](json), v)
  }

  test("case class nested -- Some") {
    val v = JsonSuiteNested(Map("a" -> JsonSuiteSimple(42, "forty-two")), Some("forty-two"))
    assertEquals(encode(v), """{"simple":{"a":{"foo":42,"bar":"forty-two"}},"bar":"forty-two"}""")
    assertEquals(decode[JsonSuiteNested](encode(v)), v)
  }

  test("case class nested -- None") {
    // Note, as of 2.7.4 scala Option seems to be treated similar to
    // java 8 Optional. It needs the NON_ABSENT include setting rather
    // than NON_NULL
    val v = JsonSuiteNested(Map("a" -> JsonSuiteSimple(42, "forty-two")), None)
    assertEquals(encode(v), """{"simple":{"a":{"foo":42,"bar":"forty-two"}}}""")
    assertEquals(decode[JsonSuiteNested](encode(v)), v)
  }

  test("case class nested -- missing Option") {
    val v = JsonSuiteNested(Map("a" -> JsonSuiteSimple(42, "forty-two")), None)
    val json = """{"simple":{"a":{"foo":42,"bar":"forty-two"}}}"""
    assertEquals(decode[JsonSuiteNested](json), v)
  }

  test("case class Option[Long]".ignore) {
    val v = JsonSuiteOptionLong(Some(42L))
    assertEquals(encode(v), """{"foo":42}""")
    assertEquals(decode[JsonSuiteOptionLong](encode(v)), v)
    assert(decode[JsonSuiteOptionLong](encode(v)).foo == v.foo)

    // java.lang.ClassCastException: java.lang.Integer cannot be cast to java.lang.Long
    assert(decode[JsonSuiteOptionLong](encode(v)).foo.get == v.foo.get)
  }

  test("case class Array[Double]") {
    val v = JsonSuiteArrayDouble("foo", Array(1.0, 2.0, 3.0, 4.0))
    assertEquals(encode(v), """{"name":"foo","values":[1.0,2.0,3.0,4.0]}""")
    assertEquals(decode[JsonSuiteArrayDouble](encode(v)).name, v.name)
    assert(util.Arrays.equals(decode[JsonSuiteArrayDouble](encode(v)).values, v.values))
  }

  // Should get base64 encoded
  test("array Byte") {
    val v = Array[Byte](1, 2, 3, 4)
    assertEquals(encode(v), "\"AQIDBA==\"")
    assert(util.Arrays.equals(decode[Array[Byte]](encode(v)), v))
  }

  test("array Short") {
    val v = Array[Short](1, 2, 3, 4)
    assertEquals(encode(v), """[1,2,3,4]""")
    assert(util.Arrays.equals(decode[Array[Short]](encode(v)), v))
  }

  test("array Int") {
    val v = Array[Int](1, 2, 3, 4)
    assertEquals(encode(v), """[1,2,3,4]""")
    assert(util.Arrays.equals(decode[Array[Int]](encode(v)), v))
  }

  test("array Long") {
    val v = Array[Long](1L, 2L, 3L, 4L)
    assertEquals(encode(v), """[1,2,3,4]""")
    assert(util.Arrays.equals(decode[Array[Long]](encode(v)), v))
  }

  test("array Float") {
    val v = Array[Float](1.0f, 2.0f, 3.0f, 4.0f)
    assertEquals(encode(v), """[1.0,2.0,3.0,4.0]""")
    assert(util.Arrays.equals(decode[Array[Float]](encode(v)), v))
  }

  test("array Double") {
    val v = Array[Double](1.0, 2.0, 3.0, 4.0)
    assertEquals(encode(v), """[1.0,2.0,3.0,4.0]""")
    assert(util.Arrays.equals(decode[Array[Double]](encode(v)), v))
  }

  test("array case class") {
    val v = Array[JsonSuiteSimple](JsonSuiteSimple(42, "a"), JsonSuiteSimple(43, "b"))
    assertEquals(encode(v), """[{"foo":42,"bar":"a"},{"foo":43,"bar":"b"}]""")
    assertEquals(decode[Array[JsonSuiteSimple]](encode(v)).toList, v.toList)
  }

  // CLDMTA-2174
  test("case class defined in object") {
    import com.netflix.atlas.json.JsonSuiteObjectWithClass.*
    val v = ClassInObject("a", 42)
    assertEquals(encode(v), """{"s":"a","v":42}""")
    assertEquals(decode[ClassInObject](encode(v)), v)
  }

  // CLDMTA-2174
  test("list of case class defined in object") {
    import com.netflix.atlas.json.JsonSuiteObjectWithClass.*
    val v = List(ClassInObject("a", 42))
    assertEquals(encode(v), """[{"s":"a","v":42}]""")
    assertEquals(decode[List[ClassInObject]](encode(v)), v)
  }

  test("smile encode/decode") {
    val v = List(1, 2, 3)
    val b = Json.smileEncode(v)
    assertEquals(Json.smileDecode[List[Int]](b), v)
  }

  // See ObjWithLambda comments for details
  test("object with lambda") {
    val obj = new ObjWithLambda
    obj.setFoo("abc")
    val json = Json.encode(obj)
    assertEquals(json, """{"foo":"abc"}""")
  }

  test("object with defaults") {
    val obj = Json.decode[JsonSuiteObjectWithDefaults]("{}")
    val json = Json.encode(obj)
    assertEquals(json, """{"foo":42,"bar":"abc","values":[]}""")
  }

  test("object with missing key") {
    val obj = Json.decode[JsonSuiteSimple]("{}")
    val json = Json.encode(obj)
    assertEquals(json, """{"foo":0}""")
  }

  test("dots in field name") {
    val obj = Json.decode[JsonKeyWithDot]("""{"a.b": "bar"}""")
    assertEquals(obj, JsonKeyWithDot("bar"))
  }

  test("object with empty array renders empty array") {
    val obj = Json.decode[JsonSuiteArrayString]("""{"name":"name", "values":[]}""")
    val json = Json.encode(obj)
    assertEquals(json, """{"name":"name","values":[]}""")
  }

  test("unmatched surrogate pairs") {
    val json = "{\"test\": 1, \"test\uD83D\": 2}"
    val jsonNode = Json.decode[ObjectNode](json)
    val smile = Json.smileEncode(jsonNode)
    val smileNode = Json.smileDecode[ObjectNode](smile)
    val expected = Json.decode[ObjectNode](json.replace('\uD83D', '\uFFFD'))
    assertEquals(smileNode, expected)
  }

  test("decode from JsonData") {
    def parse(json: String): List[JsonSuiteSimple] = {
      val node = Json.decode[JsonNode](json)
      if (node.isArray)
        Json.decode[List[JsonSuiteSimple]](node)
      else
        List(Json.decode[JsonSuiteSimple](node))
    }
    val list = parse("""[{"foo":42,"bar":"abc"}]""")
    val obj = parse("""{"foo":42,"bar":"abc"}""")
    assertEquals(list, obj)
  }

  test("JsonSupport NaN encoding") {
    val obj = JsonObjectWithDefaultSupport(Double.NaN)
    assertEquals(obj.toJson, """{"v":"NaN"}""")
  }

  test("JsonSupport default encoding") {
    val obj = JsonObjectWithDefaultSupport(42.0)
    assertEquals(Json.encode(List(obj)), """[{"v":42.0}]""")
  }

  test("JsonSupport custom encoding") {
    val obj = JsonObjectWithSupport(42.0)
    assertEquals(Json.encode(List(obj)), """[{"custom":42.0}]""")
  }

  test("customize mapper") {
    val now = Instant.now()
    assertEquals(Json.encode(now), now.toEpochMilli.toString)

    try {
      Json.configure { mapper =>
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
      }
      assertEquals(Json.encode(now), s"\"${now.toString}\"")
    } finally {
      Json.configure { mapper =>
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true)
      }
    }
  }
}

case class JsonKeyWithDot(`a.b`: String)

case object JsonSuiteObject

case class JsonSuiteSimple(foo: Int, bar: String)

case class JsonSuiteNested(simple: Map[String, JsonSuiteSimple], bar: Option[String])

case class JsonSuiteArrayDouble(name: String, values: Array[Double])

case class JsonSuiteArrayString(name: String, values: Option[Array[String]])

// https://github.com/FasterXML/jackson-module-scala/issues/62
case class JsonSuiteOptionLong(foo: Option[Long])

trait JsonSuiteTrait

case class JsonSuiteImpl1(foo: Int) extends JsonSuiteTrait

case class JsonSuiteImpl2(bar: String) extends JsonSuiteTrait

object JsonSuiteObjectWithClass {

  case class ClassInObject(s: String, v: Int)
}

case class JsonSuiteListDouble(vs: List[Double])

case class JsonSuiteObjectWithDefaults(
  foo: Int = 42,
  bar: String = "abc",
  values: List[String] = Nil
)

case class JsonObjectWithDefaultSupport(v: Double) extends JsonSupport

case class JsonObjectWithSupport(v: Double) extends JsonSupport {

  override def hasCustomEncoding: Boolean = true

  override def encode(gen: JsonGenerator): Unit = {
    gen.writeStartObject()
    gen.writeNumberField("custom", v)
    gen.writeEndObject()
  }
}

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

import java.math.BigInteger
import java.util
import java.util.regex.Pattern

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.core.JsonToken
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.Duration
import org.joda.time.Period
import org.scalatest.FunSuite


/**
 * Test case for the capabilities we need from a json parser. Mostly to document what we are using
 * and ease transition to alternative libraries if we want to switch.
 */
class JsonSuite extends FunSuite {

  import java.lang.{Double => JDouble}

  import com.netflix.atlas.json.Json._

  test("garbage") {
    intercept[JsonParseException] { decode[Boolean]("true dklfjal;k;hfnklanf'") }
    intercept[IllegalArgumentException] { decode[Map[String,AnyRef]]("""{"f":"b"} {"b":"d"}""") }
  }

  test("true") {
    val v = true
    assert(encode(v) === "true")
    assert(decode[Boolean](encode(v)) === v)
  }

  test("false") {
    val v = false
    assert(encode(v) === "false")
    assert(decode[Boolean](encode(v)) === v)
  }

  test("byte") {
    val v = 42.asInstanceOf[Byte]
    assert(encode(v) === "42")
    assert(decode[Byte](encode(v)) === v)
  }

  test("short") {
    val v = 42.asInstanceOf[Short]
    assert(encode(v) === "42")
    assert(decode[Short](encode(v)) === v)
  }

  test("int") {
    val v = 42
    assert(encode(v) === "42")
    assert(decode[Int](encode(v)) === v)
  }

  test("long") {
    val v = 42L
    assert(encode(v) === "42")
    assert(decode[Long](encode(v)) === v)
  }

  test("float") {
    val v = 42.0f
    assert(encode(v) === "42.0")
    assert(decode[Float](encode(v)) === v)
  }

  test("double") {
    val v = 42.0
    assert(encode(v) === "42.0")
    assert(decode[Double](encode(v)) === v)
  }

  // This is non-standard, but we prefer to differentiate from infinity or other special values
  test("double NaN") {
    val v = Double.NaN
    assert(encode(v) === "NaN")
    assert(JDouble.isNaN(decode[Double](encode(v))))
  }

  test("double array with string non numeric values") {
    val expected = Array(
      1.0,
      JDouble.NaN,
      JDouble.NEGATIVE_INFINITY,
      JDouble.POSITIVE_INFINITY,
      JDouble.POSITIVE_INFINITY)
    val vs = """[1.0,"NaN","-Infinity","+Infinity","Infinity"]"""
    assert(java.util.Arrays.equals(decode[Array[Double]](vs), expected))
  }

  test("object double array and string non numeric values") {
    val expected = Array(
      1.0,
      JDouble.NaN,
      JDouble.NEGATIVE_INFINITY,
      JDouble.POSITIVE_INFINITY,
      JDouble.POSITIVE_INFINITY)
    val vs = """{"values":[1.0,"NaN","-Infinity","+Infinity","Infinity"]}"""
    assert(java.util.Arrays.equals(decode[JsonSuiteArrayDouble](vs).values, expected))
  }

  test("double list with string non numeric values") {
    val expected = Array(
      1.0,
      JDouble.NaN,
      JDouble.NEGATIVE_INFINITY,
      JDouble.POSITIVE_INFINITY,
      JDouble.POSITIVE_INFINITY)
    val vs = """[1.0,"NaN","-Infinity","+Infinity","Infinity"]"""
    assert(java.util.Arrays.equals(decode[List[Double]](vs).toArray, expected))
  }

  test("object double list and non numeric values") {
    val expected = Array(
      1.0,
      JDouble.NaN,
      JDouble.NEGATIVE_INFINITY,
      JDouble.POSITIVE_INFINITY,
      JDouble.POSITIVE_INFINITY)
    val vs = """{"vs":[1.0,NaN,-Infinity,+Infinity,Infinity]}"""
    assert(java.util.Arrays.equals(decode[JsonSuiteListDouble](vs).vs.toArray, expected))
  }

  ignore("object double list and string non numeric values") {
    val expected = Array(
      1.0,
      JDouble.NaN,
      JDouble.NEGATIVE_INFINITY,
      JDouble.POSITIVE_INFINITY,
      JDouble.POSITIVE_INFINITY)
    val vs = """{"vs":[1.0,"NaN","-Infinity","+Infinity","Infinity"]}"""
    assert(java.util.Arrays.equals(decode[JsonSuiteListDouble](vs).vs.toArray, expected))
  }

  test("BigInteger") {
    val v = new BigInteger("42")
    assert(encode(v) === "42")
    assert(decode[BigInteger](encode(v)) === v)
  }

  test("String") {
    val v = "42"
    assert(encode(v) === "\"42\"")
    assert(decode[String](encode(v)) === v)
  }

  test("Pattern") {
    val v = Pattern.compile("^42.*")
    assert(encode(v) === "\"^42.*\"")
    assert(decode[Pattern](encode(v)).toString === v.toString)
  }

  test("enum") {
    val v = JsonToken.NOT_AVAILABLE
    assert(encode(v) === "\"NOT_AVAILABLE\"")
    assert(decode[JsonToken](encode(v)) === v)
  }

  test("joda DateTime") {
    val v = new DateTime(2012, 1, 13, 4, 37,52, 0, DateTimeZone.UTC)
    assert(encode(v) === v.getMillis.toString)
    assert(decode[DateTime](encode(v)) === v)
  }

  test("joda Duration") {
    val v = Duration.standardSeconds(42)
    assert(encode(v) === v.getMillis.toString)
    assert(decode[Duration](encode(v)) === v)
  }

  test("joda Period") {
    val v = Period.seconds(42)
    assert(encode(v) === "\"PT42S\"")
    assert(decode[Period](encode(v)) === v)
  }

  test("scala Option[String] -- None") {
    val v = None
    assert(encode(v) === "null")
    assert(decode[Option[String]](encode(v)) === v)
  }

  test("scala Option[String] -- Some") {
    val v = Some("42")
    assert(encode(v) === "\"42\"")
    assert(decode[Option[String]](encode(v)) === v)
  }

  test("scala Option[Int] -- None") {
    val v = None
    assert(encode(v) === "null")
    assert(decode[Option[Int]](encode(v)) === v)
  }

  test("scala Option[Int] -- Some") {
    val v = Some(42)
    assert(encode(v) === "42")
    assert(decode[Option[Int]](encode(v)) === v)
  }

  // Ugly nested generics, just to see if it would work
  test("scala List[Map[String, List[Option[Int]]]]") {
    val v = List(Map("foo" -> List(Some(42), None, Some(43))))
    assert(encode(v) === """[{"foo":[42,null,43]}]""")
    assert(decode[List[Map[String, List[Option[Int]]]]](encode(v)) === v)
  }

  test("scala List[Int]") {
    val v = List(42, 43, 44)
    assert(encode(v) === "[42,43,44]")
    assert(decode[List[Int]](encode(v)) === v)
  }

  test("scala Set[Int]") {
    val v = Set(42, 43, 44)
    assert(encode(v) === "[42,43,44]")
    assert(decode[Set[Int]](encode(v)) === v)
  }

  test("scala Map[String, Int]") {
    val v = Map("foo" -> 42)
    assert(encode(v) === "{\"foo\":42}")
    assert(decode[Map[String, Int]](encode(v)) === v)
  }

  test("scala Tuple2[String, Int]") {
    val v = "foo" -> 42
    assert(encode(v) === "[\"foo\",42]")
    assert(decode[(String, Int)](encode(v)) === v)
  }

  ignore("comments") {
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
    assert(decode[Map[String, Int]](json) === v)
  }

  // It seems like this manages to break the singleton property of the object...
  ignore("case object") {
    val v = JsonSuiteObject
    assert(encode(v) === """{}""")
    val result = decode[JsonSuiteObject.type](encode(v))
    assert(result === v)
  }

  test("case class simple") {
    val v = JsonSuiteSimple(42, "forty-two")
    assert(encode(v) === """{"foo":42,"bar":"forty-two"}""")
    assert(decode[JsonSuiteSimple](encode(v)) === v)
  }

  // We don't want null values for fields of case classes, if a field is optional it should be
  // specified as Option[T].
  ignore("case class simple -- missing field") {
    val v = JsonSuiteSimple(42, "forty-two")
    val json = """{"foo":42}"""
    assert(decode[JsonSuiteSimple](json) === v)
  }

  // We want to ignore unknown fields so we have the option of introducing new fields without
  // breaking old code that may not supported yet. Hopefully used sparingly, but nice to know we
  // have the option.
  test("case class simple -- unknown field") {
    val v = JsonSuiteSimple(42, "forty-two")
    val json = """{"foo":42,"bar":"forty-two","baz":"new field"}"""
    assert(decode[JsonSuiteSimple](json) === v)
  }

  test("case class nested -- Some") {
    val v = JsonSuiteNested(Map("a" -> JsonSuiteSimple(42, "forty-two")), Some("forty-two"))
    assert(encode(v) === """{"simple":{"a":{"foo":42,"bar":"forty-two"}},"bar":"forty-two"}""")
    assert(decode[JsonSuiteNested](encode(v)) === v)
  }

  test("case class nested -- None") {
    // Note, as of 2.7.4 scala Option seems to be treated similar to
    // java 8 Optional. It needs the NON_ABSENT include setting rather
    // than NON_NULL
    val v = JsonSuiteNested(Map("a" -> JsonSuiteSimple(42, "forty-two")), None)
    assert(encode(v) === """{"simple":{"a":{"foo":42,"bar":"forty-two"}}}""")
    assert(decode[JsonSuiteNested](encode(v)) === v)
  }

  test("case class nested -- missing Option") {
    val v = JsonSuiteNested(Map("a" -> JsonSuiteSimple(42, "forty-two")), None)
    val json = """{"simple":{"a":{"foo":42,"bar":"forty-two"}}}"""
    assert(decode[JsonSuiteNested](json) === v)
  }

  ignore("case class Option[Long]") {
    val v = JsonSuiteOptionLong(Some(42L))
    assert(encode(v) === """{"foo":42}""")
    assert(decode[JsonSuiteOptionLong](encode(v)) === v)
    assert(decode[JsonSuiteOptionLong](encode(v)).foo == v.foo)

    // java.lang.ClassCastException: java.lang.Integer cannot be cast to java.lang.Long
    assert(decode[JsonSuiteOptionLong](encode(v)).foo.get == v.foo.get)
  }

  test("case class Array[Double]") {
    val v = JsonSuiteArrayDouble("foo", Array(1.0, 2.0, 3.0, 4.0))
    assert(encode(v) === """{"name":"foo","values":[1.0,2.0,3.0,4.0]}""")
    assert(decode[JsonSuiteArrayDouble](encode(v)).name === v.name)
    assert(util.Arrays.equals(decode[JsonSuiteArrayDouble](encode(v)).values, v.values))
  }

  ignore("trait") {
    val v = List(JsonSuiteImpl1(42), JsonSuiteImpl2("42"))
    val json = """{"simple":{"a":{"foo":42,"bar":"forty-two"}}}"""
    assert(decode[JsonSuiteTrait](json) === v)
  }

  // Should get base64 encoded
  test("array Byte") {
    val v = Array[Byte](1, 2, 3, 4)
    assert(encode(v) === "\"AQIDBA==\"")
    assert(util.Arrays.equals(decode[Array[Byte]](encode(v)), v))
  }

  test("array Short") {
    val v = Array[Short](1, 2, 3, 4)
    assert(encode(v) === """[1,2,3,4]""")
    assert(util.Arrays.equals(decode[Array[Short]](encode(v)), v))
  }

  test("array Int") {
    val v = Array[Int](1, 2, 3, 4)
    assert(encode(v) === """[1,2,3,4]""")
    assert(util.Arrays.equals(decode[Array[Int]](encode(v)), v))
  }

  test("array Long") {
    val v = Array[Long](1L, 2L, 3L, 4L)
    assert(encode(v) === """[1,2,3,4]""")
    assert(util.Arrays.equals(decode[Array[Long]](encode(v)), v))
  }

  test("array Float") {
    val v = Array[Float](1.0f, 2.0f, 3.0f, 4.0f)
    assert(encode(v) === """[1.0,2.0,3.0,4.0]""")
    assert(util.Arrays.equals(decode[Array[Float]](encode(v)), v))
  }

  test("array Double") {
    val v = Array[Double](1.0, 2.0, 3.0, 4.0)
    assert(encode(v) === """[1.0,2.0,3.0,4.0]""")
    assert(util.Arrays.equals(decode[Array[Double]](encode(v)), v))
  }

  test("array case class") {
    val v = Array[JsonSuiteSimple](JsonSuiteSimple(42, "a"), JsonSuiteSimple(43, "b"))
    assert(encode(v) === """[{"foo":42,"bar":"a"},{"foo":43,"bar":"b"}]""")
    assert(decode[Array[JsonSuiteSimple]](encode(v)).toList === v.toList)
  }

  // CLDMTA-2174
  test("case class defined in object") {
    import com.netflix.atlas.json.JsonSuiteObjectWithClass._
    val v = ClassInObject("a", 42)
    assert(encode(v) === """{"s":"a","v":42}""")
    assert(decode[ClassInObject](encode(v)) === v)
  }

  // CLDMTA-2174
  test("list of case class defined in object") {
    import com.netflix.atlas.json.JsonSuiteObjectWithClass._
    val v = List(ClassInObject("a", 42))
    assert(encode(v) === """[{"s":"a","v":42}]""")
    assert(decode[List[ClassInObject]](encode(v)) === v)
  }

  test("smile encode/decode") {
    val v = List(1, 2, 3)
    val b = Json.smileEncode[List[Int]](v)
    assert(Json.smileDecode[List[Int]](b) === v)
  }

  // See ObjWithLambda comments for details
  test("object with lambda") {
    val obj = new ObjWithLambda
    obj.setFoo("abc")
    val json = Json.encode(obj)
    assert(json === """{"foo":"abc"}""")
  }

  test("object with defaults") {
    val obj = Json.decode[JsonSuiteObjectWithDefaults]("{}")
    val json = Json.encode(obj)
    assert(json === """{"foo":42,"bar":"abc","values":[]}""")
  }

  test("object with missing key") {
    val obj = Json.decode[JsonSuiteSimple]("{}")
    val json = Json.encode(obj)
    assert(json === """{"foo":0}""")
  }
}

case object JsonSuiteObject

case class JsonSuiteSimple(foo: Int, bar: String)
case class JsonSuiteNested(simple: Map[String, JsonSuiteSimple], bar: Option[String])
case class JsonSuiteArrayDouble(name: String, values: Array[Double])

// https://github.com/FasterXML/jackson-module-scala/issues/62
case class JsonSuiteOptionLong(foo: Option[Long])

trait JsonSuiteTrait
case class JsonSuiteImpl1(foo: Int) extends JsonSuiteTrait
case class JsonSuiteImpl2(bar: String) extends JsonSuiteTrait

object JsonSuiteObjectWithClass {
  case class ClassInObject(s: String, v: Int)
}

case class JsonSuiteListDouble(vs: List[Double])

case class JsonSuiteObjectWithDefaults(foo: Int = 42, bar: String = "abc", values: List[String] = Nil)

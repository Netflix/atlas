/*
 * Copyright 2014-2024 Netflix, Inc.
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

import java.io.InputStream
import java.io.OutputStream
import java.io.Reader
import java.io.Writer
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.*
import com.fasterxml.jackson.core.json.JsonReadFeature
import com.fasterxml.jackson.core.json.JsonWriteFeature
import com.fasterxml.jackson.databind.*
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.JavaTypeable

object Json {

  final class Decoder[T: JavaTypeable](reader: ObjectReader, factory: JsonFactory) {

    def decode(json: Array[Byte]): T = decode(factory.createParser(json))

    def decode(json: Array[Byte], offset: Int, length: Int): T =
      decode(factory.createParser(json, offset, length))

    def decode(json: String): T = decode(factory.createParser(json))

    def decode(input: Reader): T = decode(factory.createParser(input))

    def decode(input: InputStream): T = decode(factory.createParser(input))

    def decode(node: JsonNode): T = reader.readValue[T](node)

    def decode(parser: JsonParser): T = {
      try {
        val value = reader.readValue[T](parser)
        require(parser.nextToken() == null, "invalid json, additional content after value")
        value
      } finally {
        parser.close()
      }
    }
  }

  private val jsonFactory = JsonFactory
    .builder()
    .asInstanceOf[JsonFactoryBuilder]
    .enable(JsonReadFeature.ALLOW_JAVA_COMMENTS)
    .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS)
    .enable(StreamReadFeature.AUTO_CLOSE_SOURCE)
    .enable(StreamWriteFeature.AUTO_CLOSE_TARGET)
    .enable(StreamReadFeature.USE_FAST_DOUBLE_PARSER)
    .enable(StreamWriteFeature.USE_FAST_DOUBLE_WRITER)
    .enable(JsonWriteFeature.WRITE_NAN_AS_STRINGS)
    .build()

  private val smileFactory = SmileFactory
    .builder()
    .enable(StreamReadFeature.AUTO_CLOSE_SOURCE)
    .enable(StreamWriteFeature.AUTO_CLOSE_TARGET)
    .build()

  private val jsonMapper = newMapper(jsonFactory)

  private val smileMapper = newMapper(smileFactory)

  private def newMapper(factory: JsonFactory): ObjectMapper = {
    val mapper = new ObjectMapper(factory)
    mapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
    mapper.configure(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS, false)
    mapper.configure(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS, false)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.registerModule(DefaultScalaModule)
    mapper.registerModule(new JavaTimeModule)
    mapper.registerModule(new Jdk8Module)
    mapper.registerModule(
      new SimpleModule().setSerializerModifier(new JsonSupportSerializerModifier)
    )
    mapper
  }

  /**
    * Register additional modules with the default mappers used for JSON and Smile.
    *
    * @param module
    *     Jackson databind module to register.
    */
  def registerModule(module: Module): Unit = {
    jsonMapper.registerModule(module)
    smileMapper.registerModule(module)
  }

  /**
    * Can be called to alter the configuration of the default mapper. Note, this will
    * cause changes to apply everything using this object which could break things that
    * expect the default behavior.
    */
  def configure(f: ObjectMapper => Unit): Unit = {
    f(jsonMapper)
    f(smileMapper)
  }

  def newMapper: ObjectMapper = newMapper(jsonFactory)

  def newJsonGenerator(writer: Writer): JsonGenerator = {
    jsonFactory.createGenerator(writer)
  }

  def newJsonGenerator(stream: OutputStream): JsonGenerator = {
    jsonFactory.createGenerator(stream, JsonEncoding.UTF8)
  }

  def newJsonParser(reader: Reader): JsonParser = {
    jsonFactory.createParser(reader)
  }

  def newJsonParser(stream: InputStream): JsonParser = {
    jsonFactory.createParser(stream)
  }

  def newJsonParser(string: String): JsonParser = {
    jsonFactory.createParser(string)
  }

  def newJsonParser(bytes: Array[Byte]): JsonParser = {
    jsonFactory.createParser(bytes)
  }

  def newSmileGenerator(stream: OutputStream): JsonGenerator = {
    smileFactory.createGenerator(stream)
  }

  def newSmileParser(stream: InputStream): JsonParser = {
    smileFactory.createParser(stream)
  }

  def newSmileParser(bytes: Array[Byte]): JsonParser = {
    smileFactory.createParser(bytes, 0, bytes.length)
  }

  def encode[T: JavaTypeable](obj: T): String = {
    jsonMapper.writeValueAsString(obj)
  }

  def encode[T: JavaTypeable](writer: Writer, obj: T): Unit = {
    jsonMapper.writeValue(writer, obj)
  }

  def encode[T: JavaTypeable](stream: OutputStream, obj: T): Unit = {
    jsonMapper.writeValue(stream, obj)
  }

  def encode[T: JavaTypeable](gen: JsonGenerator, obj: T): Unit = {
    jsonMapper.writeValue(gen, obj)
  }

  def decodeResource[T: JavaTypeable](name: String): T = {
    val url = getClass.getClassLoader.getResource(name)
    require(url != null, s"could not find resource: $name")
    val input = url.openStream()
    try decode[T](input)
    finally input.close()
  }

  def decode[T: JavaTypeable](json: Array[Byte]): T = decoder[T].decode(json)

  def decode[T: JavaTypeable](json: Array[Byte], offset: Int, length: Int): T =
    decoder[T].decode(json, offset, length)

  def decode[T: JavaTypeable](json: String): T = decoder[T].decode(json)

  def decode[T: JavaTypeable](reader: Reader): T = decoder[T].decode(reader)

  def decode[T: JavaTypeable](stream: InputStream): T = decoder[T].decode(stream)

  def decode[T: JavaTypeable](node: JsonNode): T = decoder[T].decode(node)

  def decode[T: JavaTypeable](parser: JsonParser): T = {
    val reader = jsonMapper.readerFor(constructType[T](jsonMapper))
    val value = reader.readValue[T](parser)
    require(parser.nextToken() == null, "invalid json, additional content after value")
    value
  }

  def decoder[T: JavaTypeable]: Decoder[T] = {
    val reader = jsonMapper.readerFor(constructType[T](jsonMapper))
    new Decoder[T](reader, jsonFactory)
  }

  def smileEncode[T: JavaTypeable](obj: T): Array[Byte] = {
    smileMapper.writeValueAsBytes(obj)
  }

  def smileEncode[T: JavaTypeable](stream: OutputStream, obj: T): Unit = {
    smileMapper.writeValue(stream, obj)
  }

  def smileDecode[T: JavaTypeable](stream: InputStream): T = smileDecoder[T].decode(stream)

  def smileDecode[T: JavaTypeable](json: Array[Byte]): T = smileDecoder[T].decode(json)

  def smileDecoder[T: JavaTypeable]: Decoder[T] = {
    val reader = smileMapper.readerFor(constructType[T](smileMapper))
    new Decoder[T](reader, smileFactory)
  }

  private def constructType[T: JavaTypeable](mapper: ObjectMapper): JavaType = {
    implicitly[JavaTypeable[T]].asJavaType(mapper.getTypeFactory)
  }
}

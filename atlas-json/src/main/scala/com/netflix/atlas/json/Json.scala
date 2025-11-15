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

import com.fasterxml.jackson.annotation.JsonInclude

import java.io.InputStream
import java.io.OutputStream
import java.io.Reader
import java.io.Writer
import tools.jackson.core.*
import tools.jackson.core.json.JsonFactory
import tools.jackson.core.json.JsonFactoryBuilder
import tools.jackson.core.json.JsonReadFeature
import tools.jackson.core.json.JsonWriteFeature
import tools.jackson.databind.*
import tools.jackson.databind.cfg.DateTimeFeature
import tools.jackson.databind.cfg.MapperBuilder
import tools.jackson.databind.json.JsonMapper
import tools.jackson.databind.module.SimpleModule
import tools.jackson.dataformat.smile.SmileFactory
import tools.jackson.dataformat.smile.SmileMapper
import tools.jackson.dataformat.smile.SmileWriteFeature
import tools.jackson.module.scala.DefaultScalaModule
import tools.jackson.module.scala.JavaTypeable

object Json {

  final class Decoder[T](reader: ObjectReader, factory: TokenStreamFactory) {

    def decode(json: Array[Byte]): T = decode(factory.createParser(ObjectReadContext.empty, json))

    def decode(json: Array[Byte], offset: Int, length: Int): T =
      decode(factory.createParser(ObjectReadContext.empty, json, offset, length))

    def decode(json: String): T = decode(factory.createParser(ObjectReadContext.empty, json))

    def decode(input: Reader): T = decode(factory.createParser(ObjectReadContext.empty, input))

    def decode(input: InputStream): T = decode(factory.createParser(ObjectReadContext.empty, input))

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
    .enable(SmileWriteFeature.LENIENT_UTF_ENCODING)
    .build()

  private val jsonMapper = configure(JsonMapper.builder(jsonFactory)).build()

  private val smileMapper = configure(SmileMapper.builder(smileFactory)).build()

  def newMapperBuilder: JsonMapper.Builder = configure(JsonMapper.builder(jsonFactory))

  private def configure[M <: ObjectMapper, B <: MapperBuilder[M, B]](
    builder: MapperBuilder[M, B]
  ): B = {
    builder
      .changeDefaultPropertyInclusion(_ =>
        JsonInclude.Value.construct(JsonInclude.Include.NON_ABSENT, JsonInclude.Include.NON_ABSENT)
      )
      .enable(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS)
      .disable(DateTimeFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS)
      .disable(DateTimeFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS)
      .enable(DateTimeFeature.WRITE_DURATIONS_AS_TIMESTAMPS)
      .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
      .disable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
      .addModule(DefaultScalaModule)
      .addModule(new SimpleModule().setSerializerModifier(new JsonSupportSerializerModifier))
  }

  def newJsonGenerator(writer: Writer): JsonGenerator = {
    jsonFactory.createGenerator(ObjectWriteContext.empty, writer)
  }

  def newJsonGenerator(stream: OutputStream): JsonGenerator = {
    jsonFactory.createGenerator(ObjectWriteContext.empty, stream, JsonEncoding.UTF8)
  }

  def newJsonParser(reader: Reader): JsonParser = {
    jsonFactory.createParser(ObjectReadContext.empty, reader)
  }

  def newJsonParser(stream: InputStream): JsonParser = {
    jsonFactory.createParser(ObjectReadContext.empty, stream)
  }

  def newJsonParser(string: String): JsonParser = {
    jsonFactory.createParser(ObjectReadContext.empty, string)
  }

  def newJsonParser(bytes: Array[Byte]): JsonParser = {
    jsonFactory.createParser(ObjectReadContext.empty, bytes)
  }

  def newSmileGenerator(stream: OutputStream): JsonGenerator = {
    smileFactory.createGenerator(ObjectWriteContext.empty, stream)
  }

  def newSmileParser(stream: InputStream): JsonParser = {
    smileFactory.createParser(ObjectReadContext.empty, stream)
  }

  def newSmileParser(bytes: Array[Byte]): JsonParser = {
    smileFactory.createParser(ObjectReadContext.empty, bytes, 0, bytes.length)
  }

  def encode(obj: Any): String = {
    jsonMapper.writeValueAsString(obj)
  }

  def encode(writer: Writer, obj: Any): Unit = {
    jsonMapper.writeValue(writer, obj)
  }

  def encode(stream: OutputStream, obj: Any): Unit = {
    jsonMapper.writeValue(stream, obj)
  }

  def encode(gen: JsonGenerator, obj: Any): Unit = {
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

  def smileEncode(obj: Any): Array[Byte] = {
    smileMapper.writeValueAsBytes(obj)
  }

  def smileEncode(stream: OutputStream, obj: Any): Unit = {
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

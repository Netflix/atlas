/*
 * Copyright 2014-2016 Netflix, Inc.
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

import java.util.concurrent.atomic.AtomicReferenceArray

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.BeanDescription
import com.fasterxml.jackson.databind.DeserializationConfig
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.netflix.atlas.json.Reflection.FieldInfo

/**
  * Custom deserializer for case classes. The primary difference is that it honors the
  * default values specified on the primary constructor if a field is not explicitly
  * set.
  */
class CaseClassDeserializer(
    javaType: JavaType,
    config: DeserializationConfig,
    beanDesc: BeanDescription)
  extends StdDeserializer[AnyRef](javaType) {

  private val desc = Reflection.createDescription(javaType)

  private val fieldDesers = new AtomicReferenceArray[JsonDeserializer[_]](desc.params.size)

  private def getFieldDeser(finfo: FieldInfo, ctxt: DeserializationContext): JsonDeserializer[_] = {
    val fieldDeser = fieldDesers.get(finfo.pos)
    if (fieldDeser != null) fieldDeser else {
      // Probably a better way, but findContextualValueDeserializer does not look
      // at the using param of the JsonDeserialize annotation. So we look for it
      // here and use that if present.
      val annoDeser = ctxt.getAnnotationIntrospector
        .findDeserializer(finfo.property.getMember)
        .asInstanceOf[Class[_]]
      if (annoDeser != null) annoDeser.newInstance().asInstanceOf[JsonDeserializer[_]] else {
        // If possible, then get the type info from the bean description as it has more
        // context about generic types. In some cases it is null so fallback to using
        // the type we find for the field in the class.
        val btype = beanDesc.getType.containedType(finfo.pos)
        val ftype = if (btype == null) ctxt.getTypeFactory.constructType(finfo.jtype) else btype
        val deser = ctxt.findContextualValueDeserializer(ftype, finfo.property)
        fieldDesers.set(finfo.pos, deser)
        deser
      }
    }
  }

  override def deserialize(p: JsonParser, ctxt: DeserializationContext): AnyRef = {
    val args = desc.newInstanceArgs

    val t = p.getCurrentToken
    if (t != JsonToken.START_OBJECT) {
      ctxt.reportMappingException(s"found ${t.name()}, but START_OBJECT is required")
    }

    p.nextToken()
    while (p.getCurrentToken == JsonToken.FIELD_NAME) {
      val field = p.getText
      p.nextToken()
      desc.field(field) match {
        case None =>
          p.skipChildren()
        case Some(finfo) =>
          if (p.getCurrentToken != JsonToken.VALUE_NULL) {
            val deser = getFieldDeser(finfo, ctxt)
            desc.setField(args, field, deser.deserialize(p, ctxt))
          }
      }
      p.nextToken()
    }

    desc.newInstance(args)
  }
}

/*
 * Copyright 2014-2021 Netflix, Inc.
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

import java.lang.reflect.ParameterizedType

import com.fasterxml.jackson.core.`type`.TypeReference

/**
  * Helper functions for using reflection to access information about case
  * classes.
  */
private[json] object Reflection {

  type JType = java.lang.reflect.Type

  // Taken from com.fasterxml.jackson.module.scala.deser.DeserializerTest.scala
  def typeReference[T: Manifest]: TypeReference[T] = new TypeReference[T] {

    override def getType: JType = typeFromManifest(manifest[T])
  }

  // Taken from com.fasterxml.jackson.module.scala.deser.DeserializerTest.scala
  def typeFromManifest(m: Manifest[_]): JType = {
    if (m.typeArguments.isEmpty) {
      m.runtimeClass
    } else
      new ParameterizedType {

        def getRawType: Class[_] = m.runtimeClass

        def getActualTypeArguments: Array[JType] = m.typeArguments.map(typeFromManifest).toArray

        def getOwnerType: JType = null
      }
  }
}

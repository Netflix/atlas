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

import java.lang.reflect.ParameterizedType

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.`type`.TypeFactory
import com.fasterxml.jackson.databind.util.ClassUtil

import scala.language.existentials
import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe._

/**
  * Helper functions for using reflection to access information about case
  * classes.
  */
private[json] object Reflection {

  // Taken from com.fasterxml.jackson.module.scala.deser.DeserializerTest.scala
  def typeReference[T: Manifest] = new TypeReference[T] {
    override def getType = typeFromManifest(manifest[T])
  }

  // Taken from com.fasterxml.jackson.module.scala.deser.DeserializerTest.scala
  def typeFromManifest(m: Manifest[_]): java.lang.reflect.Type = {
    if (m.typeArguments.isEmpty) { m.runtimeClass }
    else new ParameterizedType {
      def getRawType = m.runtimeClass

      def getActualTypeArguments = m.typeArguments.map(typeFromManifest).toArray

      def getOwnerType = null
    }
  }

  /**
    * Create a description object for a case class. Use [[isCaseClass()]] to verify
    * before creating the description.
    */
  def createDescription(cls: Class[_]): CaseClassDesc = {
    createDescription(TypeFactory.defaultInstance().constructType(cls))
  }

  /**
    * Create a description object for a case class. Use [[isCaseClass()]] to verify
    * before creating the description.
    */
  def createDescription(jt: JavaType): CaseClassDesc = {
    createDescription(jt, currentMirror.classSymbol(jt.getRawClass).asClass)
  }

  private def createDescription(jt: JavaType, csym: ClassSymbol): CaseClassDesc = {
    val ctor = csym.primaryConstructor.asMethod

    val companion = currentMirror.reflectModule(csym.companion.asModule).instance
    val instanceMirror = currentMirror.reflect(companion)

    // See http://www.scala-lang.org/files/archive/spec/2.11/04-basic-declarations-and-definitions.html#default-arguments
    // for details. This is looking for the default value accessor for the apply on the
    // companion object.
    val ms = csym.companion.asModule
    val params = ctor.paramLists.head.zipWithIndex.map { case (p, i) =>
      val name = p.name.toString
      val dflt = if (!p.asTerm.isParamWithDefault) None else {
        val ts = instanceMirror.symbol.typeSignature
        val name = s"apply$$default$$${i + 1}"
        val dfltArg = ts.member(TermName(name))
        if (dfltArg == NoSymbol) None else {
          Some(instanceMirror.reflectMethod(dfltArg.asMethod).apply())
        }
      }
      Param(name, dflt)
    }

    CaseClassDesc(jt, currentMirror.reflectClass(csym).reflectConstructor(ctor), params)
  }

  /**
    * Check to see if a class is a case class. Currently this will ignore all classes that
    * are in sub-packages of `scala.` such as option and tuples. That check maybe overly
    * broad, but seems to work for existing use-cases.
    */
  def isCaseClass(cls: Class[_]): Boolean = {
    !cls.getName.startsWith("scala.") && currentMirror.classSymbol(cls).asClass.isCaseClass
  }

  /**
    * Parameter for a case class constructor.
    *
    * @param name
    *     Name of the parameter.
    * @param dflt
    *     Default value or `None` if no default is specified.
    */
  case class Param(name: String, dflt: Option[Any])

  /**
    * Description of a case class and its parameters.
    *
    * @param jt
    *     Raw class to be created.
    * @param ctor
    *     Handle to the constructor for creating an instance of the case class.
    * @param params
    *     Parameters for the primary constructor.
    */
  case class CaseClassDesc(jt: JavaType, ctor: MethodMirror, params: List[Param]) {

    // Create a map to allow quick lookup of the field and ensure that we have
    // allowed access to all fields.
    private val fields = {
      val ps = params.zipWithIndex.map { case (p, i) =>
        val field = jt.getRawClass.getDeclaredField(p.name)
        p.name -> FieldInfo(i, field.getType, field.getGenericType)
      }
      ps.toMap
    }

    // Default parameter values for the object. A copy of this will be returned to
    // fill in while parsing the JSON structure.
    private val dfltParams = {
      val ps = new Array[Any](params.size)
      params.zipWithIndex.foreach { case (p, i) =>
        ps(i) = p.dflt.getOrElse { fields(p.name).defaultValue }
      }
      ps
    }

    /** Create a new instance of the case class using the provided arguments. */
    def newInstance(args: Array[Any]): AnyRef = ctor.apply(args: _*).asInstanceOf[AnyRef]

    /**
      * Creates a new parameter list for the case class using default values for all parameters.
      * If there is a default specified in the code, then it will be used. Otherwise, the
      * value will be `null` or `None` if the field is an `Option`.
      */
    def newInstanceArgs: Array[Any] = dfltParams.clone()

    /** Set the value for a particular field on the instance. */
    def setField(args: Array[Any], name: String, value: Any): Unit = {
      fields.get(name).foreach { f => args(f.pos) = value }
    }

    /**
      * Get the metadata for a field based on the name. This is used to deserialize the field
      * values.
      */
    def field(name: String): Option[FieldInfo] = fields.get(name)
  }

  type JType = java.lang.reflect.Type

  case class FieldInfo(pos: Int, cls: Class[_], jtype: JType) {
    def defaultValue: Any = cls match {
      case c if c.isAssignableFrom(classOf[Option[_]]) => None
      case c if c.isPrimitive                          => ClassUtil.defaultValue(cls)
      case _                                           => null
    }
  }
}

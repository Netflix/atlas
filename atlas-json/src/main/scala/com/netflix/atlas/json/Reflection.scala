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

import com.fasterxml.jackson.databind.util.ClassUtil

import scala.language.existentials
import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe._

/**
  * Helper functions for using reflection to access information about case
  * classes.
  */
private[json] object Reflection {

  /**
    * Create a description object for a case class. Use [[isCaseClass()]] to verify
    * before creating the description.
    */
  def createDescription(cls: Class[_]): CaseClassDesc = {
    createDescription(cls, currentMirror.classSymbol(cls).asClass)
  }

  private def createDescription(cls: Class[_], csym: ClassSymbol): CaseClassDesc = {
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

    CaseClassDesc(cls, currentMirror.reflectClass(csym).reflectConstructor(ctor), params)
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
    * @param cls
    *     Raw class to be created.
    * @param ctor
    *     Handle to the constructor for creating an instance of the case class.
    * @param params
    *     Parameters for the primary constructor.
    */
  case class CaseClassDesc(cls: Class[_], ctor: MethodMirror, params: List[Param]) {

    // Create a map to allow quick lookup of the field and ensure that we have
    // allowed access to all fields.
    private val fields = {
      val fs = ClassUtil.getDeclaredFields(cls)
      fs.foreach(f => ClassUtil.checkAndFixAccess(f, true))
      fs.map(f => f.getName -> f).toMap
    }

    // Default parameter values used when constructing the object.
    private val dfltParams = params.map { p =>
      p.dflt.getOrElse {
        val fieldCls = fields(p.name).getType
        if (fieldCls.isPrimitive) ClassUtil.defaultValue(fieldCls)
        else if (fieldCls.isAssignableFrom(classOf[Option[_]])) None
        else null
      }
    }

    /**
      * Creates a new instance of the case class using default values for all parameters.
      * If there is a default specified in the code, then it will be used. Otherwise, the
      * value will be `null` or `None` if the field is an `Option`.
      */
    def newInstance: AnyRef = {
      ctor.apply(dfltParams: _*).asInstanceOf[AnyRef]
    }

    /**
      * Set the value for a particular field on the instance.
      */
    def setField(instance: AnyRef, name: String, value: Any): Unit = {
      fields.get(name).foreach { f => f.set(instance, value) }
    }

    /**
      * Determine the type for a given field. This is used to deserialize the field
      * values.
      */
    def fieldType(name: String): Option[java.lang.reflect.Type] = {
      fields.get(name).map(_.getGenericType)
    }
  }
}

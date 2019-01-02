/*
 * Copyright 2014-2019 Netflix, Inc.
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
package com.netflix.atlas.core.validation

import com.netflix.atlas.core.util.SmallHashMap
import com.typesafe.config.Config

import scala.reflect.runtime.universe
import scala.util.control.NonFatal

/**
  * Base type for validation rules.
  */
trait Rule {

  private val ruleName = getClass.getSimpleName

  /**
    * Validates that the tag map matches the rule.
    */
  def validate(tags: Map[String, String]): ValidationResult = {
    tags match {
      case m: SmallHashMap[String, String] => validate(m)
      case _                               => validate(SmallHashMap(tags))
    }
  }

  /**
    * Validates that the tag map matches the rule.
    */
  def validate(tags: SmallHashMap[String, String]): ValidationResult

  /**
    * Helper for generating the failure response.
    */
  protected def failure(reason: String): ValidationResult = {
    ValidationResult.Fail(ruleName, reason)
  }
}

object Rule {

  def load(ruleConfigs: java.util.List[_ <: Config]): List[Rule] = {
    import scala.collection.JavaConverters._
    load(ruleConfigs.asScala.toList)
  }

  def load(ruleConfigs: List[_ <: Config]): List[Rule] = {
    val configClass = classOf[Config]
    ruleConfigs.map { cfg =>
      val cls = Class.forName(cfg.getString("class"))

      try {
        cls.getConstructor(configClass).newInstance(cfg).asInstanceOf[Rule]
      } catch {
        case NonFatal(th) =>
          val runtimeMirror = universe.runtimeMirror(cls.getClassLoader)
          val moduleSymbol = runtimeMirror.moduleSymbol(cls)

          val targetMethod = moduleSymbol.typeSignature.members
            .collect {
              case x if x.isMethod && x.name.toString == "apply" => x.asMethod
            }
            .find(_.paramLists match {
              case List(List(param)) if param.info.toString == configClass.getName => true
              case _                                                               => false
            })
            .getOrElse {
              val err = new RuntimeException(
                s"""Could not find a constructor for class ${cls.getName} which takes a single parameter
                 |of type ${configClass.getName}, or an apply method with the same signature""".stripMargin
              )
              err.addSuppressed(th)
              throw err
            }

          runtimeMirror
            .reflect(runtimeMirror.reflectModule(moduleSymbol).instance)
            .reflectMethod(targetMethod)(cfg)
            .asInstanceOf[Rule]
      }
    }
  }

  @scala.annotation.tailrec
  def validate(tags: Map[String, String], rules: List[Rule]): ValidationResult = {
    if (rules.isEmpty) ValidationResult.Pass
    else {
      val res = rules.head.validate(tags)
      if (res.isFailure) res else validate(tags, rules.tail)
    }
  }
}

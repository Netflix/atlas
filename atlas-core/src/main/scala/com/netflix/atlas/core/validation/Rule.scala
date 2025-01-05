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
package com.netflix.atlas.core.validation

import com.netflix.atlas.core.util.SortedTagMap
import com.netflix.spectator.api.Id
import com.typesafe.config.Config

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
      case m: SortedTagMap => validate(m)
      case _               => validate(SortedTagMap(tags))
    }
  }

  /**
    * Validates that the tag map matches the rule.
    */
  def validate(tags: SortedTagMap): ValidationResult

  /**
    * Validates that the id matches the rule.
    */
  def validate(id: Id): ValidationResult

  /**
    * Helper for generating the failure response.
    */
  protected def failure(reason: String, tags: Map[String, String]): ValidationResult = {
    ValidationResult.Fail(ruleName, reason, tags)
  }
}

object Rule {

  def load(ruleConfigs: java.util.List[? <: Config], useComposite: Boolean = false): List[Rule] = {
    import scala.jdk.CollectionConverters.*
    load(ruleConfigs.asScala.toList, useComposite)
  }

  def load(ruleConfigs: List[? <: Config], useComposite: Boolean): List[Rule] = {
    val rules = ruleConfigs.map { cfg =>
      newInstance(cfg.getString("class"), cfg)
    }

    if (useComposite) {
      val (tagRules, others) = rules.partition(_.isInstanceOf[TagRule])
      if (tagRules.isEmpty)
        others
      else
        CompositeTagRule(tagRules.map(_.asInstanceOf[TagRule])) :: others
    } else {
      rules
    }
  }

  private def newInstance(className: String, config: Config): Rule = {
    // First look for a constructor directly on the class. If no such constructor
    // is available, then fallback to using an apply method from the companion
    // object.
    try {
      val cls = Class.forName(className)
      cls.getConstructor(classOf[Config]).newInstance(config).asInstanceOf[Rule]
    } catch {
      case _: NoSuchMethodException => newInstanceUsingApply(className, config)
    }
  }

  private def newInstanceUsingApply(className: String, config: Config): Rule = {
    try {
      val cls = Class.forName(s"$className$$")
      val companion = cls.getField("MODULE$").get(null)
      val method = companion.getClass.getMethod("apply", classOf[Config])
      method.invoke(companion, config).asInstanceOf[Rule]
    } catch {
      case _: Throwable =>
        throw new RuntimeException(
          s"""Could not find a constructor for class $className which takes a single
           |parameter of type ${classOf[Config].getName}, or an apply method with
           |the same signature""".stripMargin
        )
    }
  }

  @scala.annotation.tailrec
  def validate(tags: Map[String, String], rules: List[Rule]): ValidationResult = {
    rules match {
      case r :: rs =>
        val result = r.validate(tags)
        if (result.isFailure) result else validate(tags, rs)
      case Nil =>
        ValidationResult.Pass
    }
  }
}

/*
 * Copyright 2014-2026 Netflix, Inc.
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
package com.netflix.atlas.core.stacklang.ast

/**
  * Describes a typed parameter for a stack language word.
  *
  * @param name
  *     Display name for this parameter (e.g. "training", "alpha").
  * @param description
  *     Short description of the parameter's purpose.
  * @param dataType
  *     The expected type for this parameter.
  */
case class Parameter(name: String, description: String, dataType: DataType)

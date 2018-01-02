/*
 * Copyright 2014-2018 Netflix, Inc.
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

import com.fasterxml.jackson.core.Version
import com.fasterxml.jackson.databind.Module
import com.fasterxml.jackson.databind.Module.SetupContext

/**
  * Adds custom serializers and deserializers for our use cases.
  */
private[json] class AtlasModule extends Module {
  override def getModuleName: String = "atlas"

  override def setupModule(context: SetupContext): Unit = {
    context.addDeserializers(new CaseClassDeserializers)
  }

  override def version(): Version = Version.unknownVersion()
}

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
package com.netflix.atlas.core.model

import org.scalatest.FunSuite

class VocabularySuite extends FunSuite {

  for (vocab <- StyleVocabulary :: StyleVocabulary.dependencies; w <- vocab.words) {
    test(s"${vocab.name}: ${w.name} == self") {
      assert(w === w)
    }
  }
}


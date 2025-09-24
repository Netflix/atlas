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
package com.netflix.atlas.core.util;

/**
 * Set of features that are enabled for the API.
 */
public enum Features {
  /** Default feature set that is stable and the user can rely on. */
  STABLE,

  /**
   * Indicates that unstable features should be enabled for testing by early adopters.
   * Features in this set can change at anytime without notice. A feature should not stay
   * in this state for a long time. A few months should be considered an upper bound.
   */
  UNSTABLE
}

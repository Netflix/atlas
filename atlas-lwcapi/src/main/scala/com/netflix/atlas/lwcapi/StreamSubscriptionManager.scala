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
package com.netflix.atlas.lwcapi

import akka.stream.scaladsl.SourceQueueWithComplete

/**
  * Subclass of [[SubscriptionManager]] that uses a source queue for the handler. In some cases
  * with dependency injection erasure can cause problems with generic types. This class
  * is just to avoid those issues for the main use-cases where we need to inject a version
  * that needs a source queue.
  */
class StreamSubscriptionManager extends SubscriptionManager[QueueHandler]

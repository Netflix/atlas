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
package com.netflix.atlas.core.util

object ArrayHelper {

  import java.util.{Arrays => JArrays}

  def fill(size: Int, value: Double): Array[Double] = {
    val array = new Array[Double](size)
    JArrays.fill(array, value)
    array
  }

  def fill(size: Int, value: Float): Array[Float] = {
    val array = new Array[Float](size)
    JArrays.fill(array, value)
    array
  }

  def fill(size: Int, value: Int): Array[Int] = {
    val array = new Array[Int](size)
    JArrays.fill(array, value)
    array
  }

  def newInstance[T](n: Int): Array[T] = {

    // Note, we do a cast here and avoid using RefIntHashMap[T: ClassTag] because
    // the ClassTag seems to add a lot of memory overhead. From jol when using
    // ClassTag:
    //
    // com.netflix.atlas.core.util.RefIntHashMap@78e94dcfd footprint:
    // COUNT       AVG       SUM   DESCRIPTION
    //    55        50      2760   [C
    //     1        64        64   [I
    //     4        20        80   [Ljava.lang.Class;
    //     1        64        64   [Ljava.lang.Long;
    //     3        40       120   [Ljava.lang.Object;
    //     9        35       320   [Ljava.lang.reflect.Field;
    //     2        16        32   [Ljava.lang.reflect.Method;
    //     1        24        24   [Ljava.lang.reflect.TypeVariable;
    //     1        16        16   [Lsun.reflect.generics.tree.ClassTypeSignature;
    //     2        24        48   [Lsun.reflect.generics.tree.FieldTypeSignature;
    //     1        24        24   [Lsun.reflect.generics.tree.FormalTypeParameter;
    //     3        16        48   [Lsun.reflect.generics.tree.TypeArgument;
    //     1        32        32   com.netflix.atlas.core.util.RefIntHashMap
    //    22       525     11560   java.lang.Class
    //     7        56       392   java.lang.Class$ReflectionData
    //     5        24       120   java.lang.Long
    //    55        24      1320   java.lang.String
    //     1        16        16   java.lang.ref.ReferenceQueue$Lock
    //     1        32        32   java.lang.ref.ReferenceQueue$Null
    //     7        40       280   java.lang.ref.SoftReference
    //    52        72      3744   java.lang.reflect.Field
    //     3        24        72   java.util.ArrayList
    //     1        16        16   scala.reflect.ClassTag$$anon$1
    //     2        32        64   sun.reflect.UnsafeObjectFieldAccessorImpl
    //     9        32       288   sun.reflect.UnsafeQualifiedObjectFieldAccessorImpl
    //     1        40        40   sun.reflect.UnsafeQualifiedStaticObjectFieldAccessorImpl
    //     1        24        24   sun.reflect.generics.factory.CoreReflectionFactory
    //     2        32        64   sun.reflect.generics.reflectiveObjects.TypeVariableImpl
    //     1        32        32   sun.reflect.generics.repository.ClassRepository
    //     1        24        24   sun.reflect.generics.scope.ClassScope
    //     1        24        24   sun.reflect.generics.tree.ClassSignature
    //     3        16        48   sun.reflect.generics.tree.ClassTypeSignature
    //     2        24        48   sun.reflect.generics.tree.FormalTypeParameter
    //     3        24        72   sun.reflect.generics.tree.SimpleClassTypeSignature
    //   264               21912   (total)
    new Array[AnyRef](n).asInstanceOf[Array[T]]
  }
}

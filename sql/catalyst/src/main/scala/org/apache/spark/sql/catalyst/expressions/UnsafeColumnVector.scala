/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions

import java.io._
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.ByteBuffer
import java.util.Arrays
import java.util.Collections
import java.util.HashSet
import java.util.Set

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.KryoSerializable
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output

import org.apache.spark.sql.catalyst.InternalRow
//import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DataTypes._

/**
 * An Unsafe implementation of ColumnVector which is backed by raw memory instead of Java objects
 * through Arrow.
 */
abstract class UnsafeColumnVector extends InternalRow {


  //////////////////////////////////////////////////////////////////////////////
  // Static methods
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Field types that can be updated in place in UnsafeColumnVectors
   * (e.g. we support set() for these types)
   */
  var mutableFieldTypes: Seq[org.apache.spark.sql.types.DataType] = Seq(
        NullType,
        BooleanType,
        ByteType,
        ShortType,
        IntegerType,
        LongType,
        FloatType,
        DoubleType,
        DateType,
        TimestampType
      )

  def isFixedLength(dt: org.apache.spark.sql.types.DataType): Boolean = {
    if (dt.isInstanceOf[org.apache.spark.sql.types.DecimalType]) {
      (dt.asInstanceOf[org.apache.spark.sql.types.DecimalType]).precision <= org.apache.spark.sql.types.Decimal.MAX_LONG_DIGITS
    } else {
      mutableFieldTypes.contains(dt)
    }
  }

  def isMutable(dt: org.apache.spark.sql.types.DataType): Boolean = {
    mutableFieldTypes.contains(dt) || dt.isInstanceOf[org.apache.spark.sql.types.DecimalType]
  }

  def addRow(count: Int): Unit

  def getRow(): InternalRow

  def getSizeInBytes(): Int

  def getCount(): Int

  def writeToStream(out: DataOutputStream): Unit
}

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

package org.apache.spark.sql.catalyst.expressions.codegen;

import org.apache.spark.sql.catalyst.expressions.UnsafeColumnVector;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData;
import org.apache.spark.sql.catalyst.expressions.UnsafeMapData;
import org.apache.spark.sql.catalyst.vectorized.arrow.ArrowFormatColumnVector;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.bitset.BitSetMethods;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

import static org.apache.spark.sql.types.DataTypes.*;


/**
 * A helper class to write data into global columnVector buffer using `UnsafeColumnVector` format.
 *
 * This class will call ArrowWriter to write data to a column-based layout to replace current
 * UnsafeRowWriter.
 */
public final class UnsafeColumnVectorWriter extends UnsafeWriter {

  private final ArrowFormatColumnVector columnVector;

  public UnsafeColumnVectorWriter(String[] schema) {
    columnVector = new ArrowFormatColumnVector(schema);
  }

  public UnsafeColumnVectorWriter(String[] schema, int initialBufferSize) {
    columnVector = new ArrowFormatColumnVector(schema);
  }

  /**
   * Updates total size of the UnsafeColumnVector using the size collected by BufferHolder, and returns
   * the UnsafeColumnVector created at a constructor
   */
  public void addRow() {
    columnVector.addRow(1);
  }

  public void addRow(int count) {
    columnVector.addRow(count);
  }

  public UnsafeColumnVector getRow() {
    return columnVector;
  }

  @Override
  public void reset() {
  }

  /**
   * Resets the `startingOffset` according to the current cursor of columnVector buffer, and clear out null
   * bits.  This should be called before we write a new nested struct to the columnVector buffer.
   */
  public void resetRowWriter() {
  }

  /**
   * Clears out null bits.  This should be called before we write a new columnVector to columnVector buffer.
   */
  public void zeroOutNullBytes() {
  }

  public boolean isNullAt(int ordinal) {
    return columnVector.isNullAt(ordinal);
  }

  public void setNullAt(int ordinal) {
    columnVector.setNullAt(ordinal);
  }

  @Override
  public void setNull1Bytes(int ordinal) {
    setNullAt(ordinal);
  }

  @Override
  public void setNull2Bytes(int ordinal) {
    setNullAt(ordinal);
  }

  @Override
  public void setNull4Bytes(int ordinal) {
    setNullAt(ordinal);
  }

  @Override
  public void setNull8Bytes(int ordinal) {
    setNullAt(ordinal);
  }

  public void write(int ordinal, Object value) {
    columnVector.setObject(ordinal, value);
  }

  @Override
  public void write(int ordinal, boolean value) {
    columnVector.setBoolean(ordinal, value);
  }

  @Override
  public void write(int ordinal, byte value) {
    columnVector.setByte(ordinal, value);
  }

  @Override
  public void write(int ordinal, short value) {
    columnVector.setShort(ordinal, value);
  }

  @Override
  public void write(int ordinal, int value) {
    columnVector.setInt(ordinal, value);
  }

  @Override
  public void write(int ordinal, long value) {
    columnVector.setLong(ordinal, value);
  }

  @Override
  public void write(int ordinal, float value) {
    columnVector.setFloat(ordinal, value);
  }

  @Override
  public void write(int ordinal, double value) {
    columnVector.setDouble(ordinal, value);
  }

  @Override
  public void write(int ordinal, Decimal value, int precision, int scale) {
    columnVector.setDecimal(ordinal, value, precision, scale);
  }

  @Override
  public void write(int ordinal, UTF8String value) {
    columnVector.setUTF8String(ordinal, value);
  }

  @Override
  public void write(int ordinal, byte[] value) {
    columnVector.setBinary(ordinal, value);
  }

  @Override
  public void write(int ordinal, CalendarInterval value) {
    columnVector.setInterval(ordinal, value);
  }

  @Override
  public void write(int ordinal, UnsafeRow value) {
    columnVector.setStruct(ordinal, value);
  }

  @Override
  public void write(int ordinal, UnsafeMapData value) {
  }

  @Override
  public void write(UnsafeArrayData value) {
  }
}

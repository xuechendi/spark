package org.apache.spark.sql.catalyst.vectorized.arrow

import java.io._
import java.net._
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

import org.apache.arrow.vector.VectorSchemaRoot

import org.apache.spark._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._;
import org.apache.spark.sql.catalyst.vectorized.arrow.{ArrowFieldReader, ArrowFieldWriter, ArrowUtils}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.bitset.BitSetMethods;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.util.Utils

class ArrowFormatColumnVector(columnTypes: Array[String]) extends UnsafeColumnVector(columnTypes) {
  var name: Int = -1
  val schema: StructType = StructType(columnTypes.map{dataType => name += 1; StructField(s"$name", getType(dataType), true)}.toSeq)
  val timeZoneId: String = SQLConf.get.sessionLocalTimeZone
  val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
  val allocator = ArrowUtils.rootAllocator.newChildAllocator(
    s"ArrowFormatColumnVector", 0, Long.MaxValue)
  val root = VectorSchemaRoot.create(arrowSchema, allocator)
  val arrowColumnVectorsWriter = root.getFieldVectors().asScala.map { vector =>
    vector.allocateNew()
    ArrowFieldWriter.createFieldWriter(vector)
  }
  val arrowColumnVectorsReader = root.getFieldVectors().asScala.map { vector =>
    new ArrowFieldReader(vector)
  }
  // rowId is used for read, write will use a count inside ArrowFieldWriter.
  var rowId: Int = 0

  def getType(name: String): DataType = name match {
    case "BinaryType" => BinaryType
    case "BooleanType" => BooleanType
    case "ByteType" => ByteType
    case "CalendarIntervalType" => CalendarIntervalType
    case "DateType" => DateType
    case "DoubleType" => DoubleType
    case "FloatType" => FloatType
    case "IntegerType" => IntegerType
    case "LongType" => LongType
    case "ShortType" => ShortType
    case "StringType" => StringType
    case _ => throw new UnsupportedOperationException(s"Unsupported data type: $name")
  }

  def addRow(): Unit = {
    rowId += 1
  }
  
  def assertIndexIsValid(ordinal: Int): Unit = {
    assert(ordinal < arrowColumnVectorsWriter.size)
  }

  override def setNullAt(ordinal: Int): Unit = {
    assertIndexIsValid(ordinal)
    arrowColumnVectorsWriter(ordinal).setNull()
  }

  override def setInt(ordinal: Int, value: Int): Unit = {
    assertIndexIsValid(ordinal)
    arrowColumnVectorsWriter(ordinal).write(value, ordinal)
  }

  override def setLong(ordinal: Int, value: Long): Unit = {
    assertIndexIsValid(ordinal)
    arrowColumnVectorsWriter(ordinal).write(value, ordinal)
  }

  override def setDouble(ordinal: Int, value: Double): Unit = {
    assertIndexIsValid(ordinal)
    arrowColumnVectorsWriter(ordinal).write(value, ordinal)
  }

  override def setBoolean(ordinal: Int, value: Boolean): Unit = {
    assertIndexIsValid(ordinal)
    arrowColumnVectorsWriter(ordinal).write(value, ordinal)
  }

  override def setShort(ordinal: Int, value: Short): Unit = {
    assertIndexIsValid(ordinal)
    arrowColumnVectorsWriter(ordinal).write(value, ordinal)
  }

  override def setByte(ordinal: Int, value: Byte): Unit = {
    assertIndexIsValid(ordinal)
    arrowColumnVectorsWriter(ordinal).write(value, ordinal)
  }

  override def setFloat(ordinal: Int, value: Float): Unit = {
    assertIndexIsValid(ordinal)
    arrowColumnVectorsWriter(ordinal).write(value, ordinal)
  }

  def setDecimal(ordinal: Int, value: Decimal, precision: Int, scale: Int): Unit = {
    assertIndexIsValid(ordinal)
    arrowColumnVectorsWriter(ordinal).write(value, ordinal)
  }

  def setUTF8String(ordinal: Int, value: UTF8String): Unit = {
    assertIndexIsValid(ordinal)
    arrowColumnVectorsWriter(ordinal).write(value, ordinal)
  }

  def setBinary(ordinal: Int, value: Array[Byte]): Unit = {
    assertIndexIsValid(ordinal)
    arrowColumnVectorsWriter(ordinal).write(value, ordinal)
  }

  def setStruct(ordinal: Int, value: UnsafeRow): Unit = {
    throw new UnsupportedOperationException(s"Unsupported data type: UnsafeRow")
    //assertIndexIsValid(ordinal)
    //arrowColumnVectorsWriter(ordinal).write(value, ordinal)
  }

  def setArray(ordinal: Int, value: ArrayData): Unit = {
    assertIndexIsValid(ordinal)
    arrowColumnVectorsWriter(ordinal).write(value, ordinal)
  }

  def setInterval(ordinal: Int, value: CalendarInterval): Unit = {
    throw new UnsupportedOperationException(s"Unsupported data type: CalendarInterval")
  }

  override def get(ordinal: Int, dataType: DataType): Object = {
    // TODO
    if (isNullAt(ordinal)) {
      throw new IndexOutOfBoundsException()
    }
    throw new UnsupportedOperationException(s"Unsupported data type: Object")
    //arrowColumnVectorsReader(ordinal).getObject(rowId)
  }

  override def isNullAt(ordinal: Int): Boolean = {
    assertIndexIsValid(ordinal)
    arrowColumnVectorsReader(ordinal).isNullAt(rowId)
  }

  override def getBoolean(ordinal: Int): Boolean = {
    if (isNullAt(ordinal)) {
      throw new IndexOutOfBoundsException()
    }
    arrowColumnVectorsReader(ordinal).getBoolean(rowId)
  }
 
  override def getByte(ordinal: Int): Byte = {
    if (isNullAt(ordinal)) {
      throw new IndexOutOfBoundsException()
    }
    arrowColumnVectorsReader(ordinal).getByte(rowId)
  }

  override def getShort(ordinal: Int): Short = {
    if (isNullAt(ordinal)) {
      throw new IndexOutOfBoundsException()
    }
    arrowColumnVectorsReader(ordinal).getShort(rowId)
  }

  override def getInt(ordinal: Int): Int = {
    if (isNullAt(ordinal)) {
      throw new IndexOutOfBoundsException()
    }
    arrowColumnVectorsReader(ordinal).getInt(rowId)
  }

  override def getLong(ordinal: Int): Long = {
    if (isNullAt(ordinal)) {
      throw new IndexOutOfBoundsException()
    }
    arrowColumnVectorsReader(ordinal).getLong(rowId)
  }

  override def getFloat(ordinal: Int): Float = {
    if (isNullAt(ordinal)) {
      throw new IndexOutOfBoundsException()
    }
    arrowColumnVectorsReader(ordinal).getFloat(rowId)
  }

  override def getDouble(ordinal: Int): Double = {
    if (isNullAt(ordinal)) {
      throw new IndexOutOfBoundsException()
    }
    arrowColumnVectorsReader(ordinal).getDouble(rowId)
  }

  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = {
    if (isNullAt(ordinal)) {
      throw new IndexOutOfBoundsException()
    }
    arrowColumnVectorsReader(ordinal).getDecimal(rowId, precision, scale)
  }

  override def getUTF8String(ordinal: Int): UTF8String = {
    if (isNullAt(ordinal)) {
      throw new IndexOutOfBoundsException()
    }
    arrowColumnVectorsReader(ordinal).getUTF8String(rowId)
  }

  override def getBinary(ordinal: Int): Array[Byte] = {
    if (isNullAt(ordinal)) {
      throw new IndexOutOfBoundsException()
    }
    arrowColumnVectorsReader(ordinal).getBinary(rowId)
  }

  override def copy(): UnsafeRow = {
    //TODO
    throw new UnsupportedOperationException(s"Unsupported data type: copy")
    new UnsafeRow(0)
  }

  override def numFields(): Int = {
    columnTypes.size
  }

  override def update(ordinal: Int, value: Any): Unit = {
    assertIndexIsValid(ordinal)
    arrowColumnVectorsWriter(ordinal).write(value, ordinal)
  }

  override def getInterval(ordinal: Int): CalendarInterval = {
    if (isNullAt(ordinal)) {
      throw new IndexOutOfBoundsException()
    }
    //TODO
    throw new UnsupportedOperationException(s"Unsupported data type: CalendarInterval")
  }

  override def getStruct(ordinal: Int, numFields: Int): UnsafeRow = {
    if (isNullAt(ordinal)) {
      throw new IndexOutOfBoundsException()
    }
    //TODO
    throw new UnsupportedOperationException(s"Unsupported data type: UnsafeRow")
  }

  override def getArray(ordinal: Int): UnsafeArrayData = {
    if (isNullAt(ordinal)) {
      throw new IndexOutOfBoundsException()
    }
    //TODO
    throw new UnsupportedOperationException(s"Unsupported data type: UnsafeArrayData")
  }

  override def getMap(ordinal: Int): UnsafeMapData = {
    if (isNullAt(ordinal)) {
      throw new IndexOutOfBoundsException()
    }
    //TODO
    throw new UnsupportedOperationException(s"Unsupported data type: UnsafeMapData")
  }
}

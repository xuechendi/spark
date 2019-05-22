package org.apache.spark.sql.catalyst.vectorized.arrow

import java.io._
import java.net._
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}

import org.apache.spark._
import org.apache.spark.internal.Logging
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

class ArrowFormatColumnVector extends UnsafeColumnVector with Logging {
  private var root: VectorSchemaRoot = _
  val allocator = ArrowUtils.rootAllocator.newChildAllocator(
    s"ArrowFormatColumnVector", 0, Long.MaxValue)
  private var arrowColumnVectorsWriter: Seq[ArrowFieldWriter] = _
  private var arrowColumnVectorsReader: Seq[ArrowFieldReader] = _
  private var reader: ArrowStreamReader = _

  var rowId: Int = 0
  var numFields_v: Int = 0
  var curRowReader: InternalRow = _

  def this(columnTypes: Array[String]) = {
    this()
    var name: Int = -1
    val schema: StructType = StructType(columnTypes.map{dataType => name += 1; StructField(s"$name", getType(dataType), true)}.toSeq)
    val timeZoneId: String = SQLConf.get.sessionLocalTimeZone
    val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
    root = VectorSchemaRoot.create(arrowSchema, allocator)
    arrowColumnVectorsWriter = root.getFieldVectors().asScala.map { vector =>
      numFields_v += 1
      vector.allocateNew()
      ArrowFieldWriter.createFieldWriter(vector)
    }.toSeq
  }

  def this(ins: DataInputStream) = {
    this()
    reader = new ArrowStreamReader(ins, allocator)
    root = reader.getVectorSchemaRoot()
    initializeReaderVectors()
    reader.loadNextBatch()
    logDebug(s"rowId is $rowId, rowCount is ${root.getRowCount()}, numFields is ${numFields_v}")
  }

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

  def initializeReaderVectors(): Unit = {
    if (arrowColumnVectorsReader != null) return
    arrowColumnVectorsReader = root.getFieldVectors().asScala.map { vector =>
      numFields_v += 1
      new ArrowFieldReader(vector)
    }.toSeq
  }

  def addRow(): Unit = {
    rowId += 1
    root.setRowCount(rowId)
  }
  
  def getRow(): InternalRow = {
    if (arrowColumnVectorsReader == null) {
      initializeReaderVectors()
    }
    val row = new RowReader(numFields_v)
    if (!row.isValid) return null
    row
  }

  def getRowIterator(): Iterator[InternalRow] = {
    new Iterator[InternalRow] {
      var nextRow: InternalRow = getRow()
      override def hasNext: Boolean = {
        nextRow != null
      }

      override def next(): InternalRow = {
        var cur = nextRow
        nextRow = getRow()
        cur
      }
    }
  }

  def getSizeInBytes(): Int = {
    var sizeInBytes: Int = 0
    for (vector <- arrowColumnVectorsWriter) {
      sizeInBytes += vector.getSizeInBytes()
    }
    sizeInBytes
  }

  override def getCount(): Int = {
    root.getRowCount()
  }

  def assertIndexIsValid(ordinal: Int): Unit = {
    assert(ordinal < numFields_v)
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
    if (curRowReader == null) curRowReader = getRow()
    curRowReader.get(ordinal, dataType)
  }

  override def isNullAt(ordinal: Int): Boolean = {
    if (curRowReader == null) curRowReader = getRow()
    if (curRowReader == null) return true
    curRowReader.isNullAt(ordinal)
  }

  override def getBoolean(ordinal: Int): Boolean = {
    if (curRowReader == null) curRowReader = getRow()
    curRowReader.getBoolean(ordinal)
  }

  override def getByte(ordinal: Int): Byte = {
    if (curRowReader == null) curRowReader = getRow()
    curRowReader.getByte(ordinal)
  }

  override def getShort(ordinal: Int): Short = {
    if (curRowReader == null) curRowReader = getRow()
    curRowReader.getShort(ordinal)
  }

  override def getInt(ordinal: Int): Int = {
    if (curRowReader == null) curRowReader = getRow()
    curRowReader.getInt(ordinal)
  }

  override def getLong(ordinal: Int): Long = {
    if (curRowReader == null) curRowReader = getRow()
    curRowReader.getLong(ordinal)
  }

  override def getFloat(ordinal: Int): Float = {
    if (curRowReader == null) curRowReader = getRow()
    curRowReader.getFloat(ordinal)
  }

  override def getDouble(ordinal: Int): Double = {
    if (curRowReader == null) curRowReader = getRow()
    curRowReader.getDouble(ordinal)
  }

  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = {
    if (curRowReader == null) curRowReader = getRow()
    curRowReader.getDecimal(ordinal, precision, scale)
  }

  override def getUTF8String(ordinal: Int): UTF8String = {
    if (curRowReader == null) curRowReader = getRow()
    curRowReader.getUTF8String(ordinal)
  }

  override def getBinary(ordinal: Int): Array[Byte] = {
    if (curRowReader == null) curRowReader = getRow()
    curRowReader.getBinary(ordinal)
  }

  override def getInterval(ordinal: Int): CalendarInterval = {
    //TODO
    throw new UnsupportedOperationException(s"Unsupported data type: CalendarInterval")
  }

  override def getStruct(ordinal: Int, numFields: Int): UnsafeRow = {
    //TODO
    throw new UnsupportedOperationException(s"Unsupported data type: UnsafeRow")
  }

  override def getArray(ordinal: Int): UnsafeArrayData = {
    //TODO
    throw new UnsupportedOperationException(s"Unsupported data type: UnsafeArrayData")
  }

  override def getMap(ordinal: Int): UnsafeMapData = {
    //TODO
    throw new UnsupportedOperationException(s"Unsupported data type: UnsafeMapData")
  }
  override def copy(): UnsafeRow = {
    //TODO
    throw new UnsupportedOperationException(s"Unsupported data type: copy")
    new UnsafeRow(0)
  }

  override def numFields(): Int = {
    numFields_v
  }

  override def update(ordinal: Int, value: Any): Unit = {
    assertIndexIsValid(ordinal)
    arrowColumnVectorsWriter(ordinal).write(value, ordinal)
  }

  def writeToStream(out: DataOutputStream): Unit = {
    Utils.tryWithSafeFinally {
      val writer = new ArrowStreamWriter(root, null, out)
      writer.start()
      writer.writeBatch()
      writer.end()
      logDebug(s"writeToStream wrotes ${writer.bytesWritten()} bytes, contains ${rowId} rows.")
    } {
      root.close()
      allocator.close()
    }
  }

  class RowReader(numFields_v: Int) extends InternalRow with Logging{
    def isValid(): Boolean = {
      arrowColumnVectorsReader(0).isValid()
    }

    def get(ordinal: Int, dataType: DataType): Object = {
      // TODO
      if (isNullAt(ordinal)) {
        throw new IndexOutOfBoundsException()
      }
      throw new UnsupportedOperationException(s"Unsupported data type: Object")
    }

    def isNullAt(ordinal: Int): Boolean = {
      assertIndexIsValid(ordinal)
      try {
        !arrowColumnVectorsReader(ordinal).isValid()
      } catch {
        case e: java.lang.NullPointerException => {
          logInfo(s"Null pointer exception, ordinal: ${ordinal}")
          return true
        }
      }
    }

    def setNullAt(ordinal: Int): Unit = {
      throw new UnsupportedOperationException(s"Unsupported data type: setNullAt")
    }

    def getBoolean(ordinal: Int): Boolean = {
      if (isNullAt(ordinal)) {
        throw new IndexOutOfBoundsException()
      }
      arrowColumnVectorsReader(ordinal).getBoolean()
    }
 
    def getByte(ordinal: Int): Byte = {
      if (isNullAt(ordinal)) {
        throw new IndexOutOfBoundsException()
      }
      arrowColumnVectorsReader(ordinal).getByte()
    }

    def getShort(ordinal: Int): Short = {
      if (isNullAt(ordinal)) {
        throw new IndexOutOfBoundsException()
      }
      arrowColumnVectorsReader(ordinal).getShort()
    }

    def getInt(ordinal: Int): Int = {
      if (isNullAt(ordinal)) {
        throw new IndexOutOfBoundsException()
      }
      arrowColumnVectorsReader(ordinal).getInt()
    }

    def getLong(ordinal: Int): Long = {
      if (isNullAt(ordinal)) {
        throw new IndexOutOfBoundsException()
      }
      arrowColumnVectorsReader(ordinal).getLong()
    }

    def getFloat(ordinal: Int): Float = {
      if (isNullAt(ordinal)) {
        throw new IndexOutOfBoundsException()
      }
      arrowColumnVectorsReader(ordinal).getFloat()
    }

    def getDouble(ordinal: Int): Double = {
      if (isNullAt(ordinal)) {
        throw new IndexOutOfBoundsException()
      }
      arrowColumnVectorsReader(ordinal).getDouble()
    }

    def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = {
      if (isNullAt(ordinal)) {
        throw new IndexOutOfBoundsException()
      }
      arrowColumnVectorsReader(ordinal).getDecimal(precision, scale)
    }

    def getUTF8String(ordinal: Int): UTF8String = {
      if (isNullAt(ordinal)) {
        throw new IndexOutOfBoundsException()
      }
      //logDebug(s"getUTF8String, rowId: ${arrowColumnVectorsReader(ordinal).getRowId()}")
      arrowColumnVectorsReader(ordinal).getUTF8String()
    }

    def getBinary(ordinal: Int): Array[Byte] = {
      if (isNullAt(ordinal)) {
        throw new IndexOutOfBoundsException()
      }
      arrowColumnVectorsReader(ordinal).getBinary()
    }

    def getInterval(ordinal: Int): CalendarInterval = {
      //TODO
      throw new UnsupportedOperationException(s"Unsupported data type: CalendarInterval")
    }
  
    def getStruct(ordinal: Int, numFields: Int): UnsafeRow = {
      //TODO
      throw new UnsupportedOperationException(s"Unsupported data type: UnsafeRow")
    }
  
    def getArray(ordinal: Int): UnsafeArrayData = {
      //TODO
      throw new UnsupportedOperationException(s"Unsupported data type: UnsafeArrayData")
    }
  
    def getMap(ordinal: Int): UnsafeMapData = {
      //TODO
      throw new UnsupportedOperationException(s"Unsupported data type: UnsafeMapData")
    }
    def copy(): UnsafeRow = {
      //TODO
      throw new UnsupportedOperationException(s"Unsupported data type: copy")
      new UnsafeRow(0)
    }
  
    def numFields(): Int = {
      numFields_v
    }
  
    def update(ordinal: Int, value: Any): Unit = {
      assertIndexIsValid(ordinal)
      arrowColumnVectorsWriter(ordinal).write(value, ordinal)
    }

  }
}

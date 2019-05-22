package org.apache.spark.sql.catalyst.vectorized.arrow

import scala.collection.JavaConverters._

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.bitset.BitSetMethods;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types._

object ArrowFieldWriter {
  def createFieldWriter(vector: ValueVector): ArrowFieldWriter = {
    val field = vector.getField()
    (ArrowUtils.fromArrowField(field), vector) match {
      case (BooleanType, vector: BitVector) => new BooleanWriter(vector)
      case (ByteType, vector: TinyIntVector) => new ByteWriter(vector)
      case (ShortType, vector: SmallIntVector) => new ShortWriter(vector)
      case (IntegerType, vector: IntVector) => new IntegerWriter(vector)
      case (LongType, vector: BigIntVector) => new LongWriter(vector)
      case (FloatType, vector: Float4Vector) => new FloatWriter(vector)
      case (DoubleType, vector: Float8Vector) => new DoubleWriter(vector)
      case (DecimalType.Fixed(precision, scale), vector: DecimalVector) =>
        new DecimalWriter(vector, precision, scale)
      case (StringType, vector: VarCharVector) => new StringWriter(vector)
      case (BinaryType, vector: VarBinaryVector) => new BinaryWriter(vector)
      case (DateType, vector: DateDayVector) => new DateWriter(vector)
      case (TimestampType, vector: TimeStampMicroTZVector) => new TimestampWriter(vector)
      case (ArrayType(_, _), vector: ListVector) =>
        val elementVector = createFieldWriter(vector.getDataVector())
        new ArrayWriter(vector, elementVector)
      case (StructType(_), vector: StructVector) =>
        val children = (0 until vector.size()).map { ordinal =>
          createFieldWriter(vector.getChildByOrdinal(ordinal))
        }
        new StructWriter(vector, children.toArray)
      case (dt, _) =>
        throw new UnsupportedOperationException(s"Unsupported data type: ${dt.catalogString}")
    }
  }
}

abstract class ArrowFieldWriter {

  def valueVector: ValueVector

  def name: String = valueVector.getField().getName()
  def dataType: DataType = ArrowUtils.fromArrowField(valueVector.getField())
  def nullable: Boolean = valueVector.getField().isNullable()

  def setNull(): Unit
  def setValue(input: SpecializedGetters, ordinal: Int): Unit
  //def setValueImp(input: Any, ordinal: Int): Unit
  def writeValue(input: Any, ordinal: Int): Unit

  var count: Int = 0

  def write(input: Any, ordinal: Int): Unit = {
    if (input.isInstanceOf[SpecializedGetters]) {
      if (input.asInstanceOf[SpecializedGetters].isNullAt(ordinal)) {
        setNull()
      } else {
        setValue(input.asInstanceOf[SpecializedGetters], ordinal)
      }
    } else {
        writeValue(input, ordinal)
    }
    count += 1
  }

  def finish(): Unit = {
    valueVector.setValueCount(count)
  }

  def reset(): Unit = {
    valueVector.reset()
    count = 0
  }

  def getSizeInBytes(): Int
}

class BooleanWriter(val valueVector: BitVector) extends ArrowFieldWriter {

  def writeValue(input: Any, ordinal: Int): Unit = {
    val value = input.asInstanceOf[Int]
    setValueImp(value, ordinal)
  }

  def setNull(): Unit = {
    valueVector.setNull(count)
  }

  def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    setValueImp(if (input.getBoolean(ordinal)) 1 else 0, ordinal)
  }

  def setValueImp(input: Int, ordinal: Int): Unit = {
    valueVector.setSafe(count, input)
  }

  def getSizeInBytes(): Int = {
    valueVector.getBufferSize()
  }
}

class ByteWriter(val valueVector: TinyIntVector) extends ArrowFieldWriter {

  def writeValue(input: Any, ordinal: Int): Unit = {
    val value = input.asInstanceOf[Byte]
    setValueImp(value, ordinal)
  }

  def setNull(): Unit = {
    valueVector.setNull(count)
  }

  def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    setValueImp(input.getByte(ordinal), ordinal)
  }

  def setValueImp(input: Byte, ordinal: Int): Unit = {
    valueVector.setSafe(count, input)
  }

  def getSizeInBytes(): Int = {
    valueVector.getBufferSize()
  }
}

class ShortWriter(val valueVector: SmallIntVector) extends ArrowFieldWriter {

  def writeValue(input: Any, ordinal: Int): Unit = {
    val value = input.asInstanceOf[Short]
    setValueImp(value, ordinal)
  }

  def setNull(): Unit = {
    valueVector.setNull(count)
  }

  def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    setValueImp(input.getShort(ordinal), ordinal)
  }

  def setValueImp(input: Short, ordinal: Int): Unit = {
    valueVector.setSafe(count, input)
  }

  def getSizeInBytes(): Int = {
    valueVector.getBufferSize()
  }
}

class IntegerWriter(val valueVector: IntVector) extends ArrowFieldWriter {

  def writeValue(input: Any, ordinal: Int): Unit = {
    val value = input.asInstanceOf[Int]
    setValueImp(value, ordinal)
  }

  def setNull(): Unit = {
    valueVector.setNull(count)
  }

  def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    setValueImp(input.getInt(ordinal), ordinal)
  }

  def setValueImp(input: Int, ordinal: Int): Unit = {
    valueVector.setSafe(count, input)
  }

  def getSizeInBytes(): Int = {
    valueVector.getBufferSize()
  }
}

class LongWriter(val valueVector: BigIntVector) extends ArrowFieldWriter {

  def writeValue(input: Any, ordinal: Int): Unit = {
    val value = input.asInstanceOf[Long]
    setValueImp(value, ordinal)
  }

  def setNull(): Unit = {
    valueVector.setNull(count)
  }

  def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    setValueImp(input.getLong(ordinal), ordinal)
  }

  def setValueImp(input: Long, ordinal: Int): Unit = {
    valueVector.setSafe(count, input)
  }

  def getSizeInBytes(): Int = {
    valueVector.getBufferSize()
  }
}

class FloatWriter(val valueVector: Float4Vector) extends ArrowFieldWriter {

  def writeValue(input: Any, ordinal: Int): Unit = {
    val value = input.asInstanceOf[Float]
    setValueImp(value, ordinal)
  }

  def setNull(): Unit = {
    valueVector.setNull(count)
  }

  def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    setValueImp(input.getFloat(ordinal), ordinal)
  }

  def setValueImp(input: Float, ordinal: Int): Unit = {
    valueVector.setSafe(count, input)
  }

  def getSizeInBytes(): Int = {
    valueVector.getBufferSize()
  }
}

class DoubleWriter(val valueVector: Float8Vector) extends ArrowFieldWriter {

  def writeValue(input: Any, ordinal: Int): Unit = {
    val value = input.asInstanceOf[Double]
    setValueImp(value, ordinal)
  }

  def setNull(): Unit = {
    valueVector.setNull(count)
  }

  def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    setValueImp(input.getDouble(ordinal), ordinal)
  }

  def setValueImp(input: Double, ordinal: Int): Unit = {
    valueVector.setSafe(count, input)
  }

  def getSizeInBytes(): Int = {
    valueVector.getBufferSize()
  }
}

class DecimalWriter(
    val valueVector: DecimalVector,
    precision: Int,
    scale: Int) extends ArrowFieldWriter {

  def writeValue(input: Any, ordinal: Int): Unit = {
    val value = input.asInstanceOf[Decimal]
    setValueImp(value, ordinal)
  }

  def setNull(): Unit = {
    valueVector.setNull(count)
  }

  def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    setValueImp(input.getDecimal(ordinal, precision, scale), ordinal)
  }

  def setValueImp(input: Decimal, ordinal: Int): Unit = {
    if (input.changePrecision(precision, scale)) {
      valueVector.setSafe(count, input.toJavaBigDecimal)
    } else {
      setNull()
    }
  }

  def getSizeInBytes(): Int = {
    valueVector.getBufferSize()
  }
}

class StringWriter(val valueVector: VarCharVector) extends ArrowFieldWriter {

  def writeValue(input: Any, ordinal: Int): Unit = {
    val value = input.asInstanceOf[UTF8String]
    setValueImp(value, ordinal)
  }

  def setNull(): Unit = {
    valueVector.setNull(count)
  }

  def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    setValueImp(input.getUTF8String(ordinal), ordinal)
  }

  def setValueImp(input: UTF8String, ordinal: Int): Unit = {
    val utf8ByteBuffer = input.getByteBuffer
    // todo: for off-heap UTF8String, how to pass in to arrow without copy?
    valueVector.setSafe(count, utf8ByteBuffer, utf8ByteBuffer.position(), input.numBytes())
  }

  def getSizeInBytes(): Int = {
    valueVector.getBufferSize()
  }
}

class BinaryWriter( val valueVector: VarBinaryVector) extends ArrowFieldWriter {

  def writeValue(input: Any, ordinal: Int): Unit = {
    val value = input.asInstanceOf[Array[Byte]]
    setValueImp(value, ordinal)
  }

  def setNull(): Unit = {
    valueVector.setNull(count)
  }

  def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    setValueImp(input.getBinary(ordinal), ordinal)
  }

  def setValueImp(input: Array[Byte], ordinal: Int): Unit = {
    valueVector.setSafe(count, input, 0, input.length)
  }

  def getSizeInBytes(): Int = {
    valueVector.getBufferSize()
  }
}

class DateWriter(val valueVector: DateDayVector) extends ArrowFieldWriter {

  def writeValue(input: Any, ordinal: Int): Unit = {
    val value = input.asInstanceOf[Int]
    setValueImp(value, ordinal)
  }

  def setNull(): Unit = {
    valueVector.setNull(count)
  }

  def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    setValueImp(input.getInt(ordinal), ordinal)
  }

  def setValueImp(input: Int, ordinal: Int): Unit = {
    valueVector.setSafe(count, input)
  }

  def getSizeInBytes(): Int = {
    valueVector.getBufferSize()
  }
}

class TimestampWriter(val valueVector: TimeStampMicroTZVector) extends ArrowFieldWriter {

  def writeValue(input: Any, ordinal: Int): Unit = {
    val value = input.asInstanceOf[Long]
    setValueImp(value, ordinal)
  }

  def setNull(): Unit = {
    valueVector.setNull(count)
  }

  def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    setValueImp(input.getLong(ordinal), ordinal)
  }

  def setValueImp(input: Long, ordinal: Int): Unit = {
    valueVector.setSafe(count, input)
  }

  def getSizeInBytes(): Int = {
    valueVector.getBufferSize()
  }
}

class ArrayWriter(
    val valueVector: ListVector,
    val elementWriter: ArrowFieldWriter) extends ArrowFieldWriter {

  def writeValue(input: Any, ordinal: Int): Unit = {
    val value = input.asInstanceOf[ArrayData]
    setValueImp(value, ordinal)
  }

  def setNull(): Unit = {
  }

  def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    setValueImp(input.getArray(ordinal), ordinal)
  }

  def setValueImp(input: ArrayData, ordinal: Int): Unit = {
    var i = 0
    valueVector.startNewValue(count)
    while (i < input.numElements()) {
      elementWriter.write(input, i)
      i += 1
    }
    valueVector.endValue(count, input.numElements())
  }

  override def finish(): Unit = {
    super.finish()
    elementWriter.finish()
  }

  override def reset(): Unit = {
    super.reset()
    elementWriter.reset()
  }

  def getSizeInBytes(): Int = {
    valueVector.getBufferSize()
  }
}

class StructWriter(
    val valueVector: StructVector,
    children: Array[ArrowFieldWriter]) extends ArrowFieldWriter {

  def writeValue(input: Any, ordinal: Int): Unit = {
    val value = input.asInstanceOf[InternalRow]
    setValueImp(value, ordinal)
  }

  def setNull(): Unit = {
    var i = 0
    while (i < children.length) {
      children(i).setNull()
      children(i).count += 1
      i += 1
    }
    valueVector.setNull(count)
  }

  def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    setValueImp(input.getStruct(ordinal, children.length), ordinal)
  }

  def setValueImp(input: InternalRow, ordinal: Int): Unit = {
    var i = 0
    while (i < input.numFields) {
      children(i).write(input, i)
      i += 1
    }
    valueVector.setIndexDefined(count)
  }

  override def finish(): Unit = {
    super.finish()
    children.foreach(_.finish())
  }

  override def reset(): Unit = {
    super.reset()
    children.foreach(_.reset())
  }

  def getSizeInBytes(): Int = {
    valueVector.getBufferSize()
  }
}

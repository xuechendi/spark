package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.hadoop.hive.ql.exec.vector._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.catalyst.vectorized.arrow._
import org.apache.spark.unsafe.types.UTF8String

class ColumnVectorProcessor(input: Any) extends DataType with Logging {

  private[spark] override def asNullable: ColumnVectorProcessor = this
  def defaultSize: Int = 4
  var op: ColumnVectorOperation = _

  def processTo(output: ArrowFieldWriter): Unit = {
    if (op == null) {
      // input copy to output
      op = new CopyOperation()
    }
    op.process(input, output)
  }

  def add(that: Any): ColumnVectorProcessor = {
    op = new AddOperation(that)
    this
  }

  abstract class ColumnVectorOperation {
    def process(input: Any, output: ArrowFieldWriter): Unit
  }

  class AddOperation(data: Any) extends ColumnVectorOperation {
    def process(input: Any, output: ArrowFieldWriter): Unit = {
      //logInfo(s"AddOperation.process($input, $output)")
      var rowId: Int = 0
      input match {
        case _: LongColumnVector => {
          for (row <- input.asInstanceOf[LongColumnVector].vector) {
            output match {
              case _: LongWriter => {
                output.write(row + data.asInstanceOf[Int], rowId)
              }
              case _: IntegerWriter => {
                output.write(row.asInstanceOf[Int] + data.asInstanceOf[Int], rowId)
              }
              case _ => throw new UnsupportedOperationException(s"Unsupported data type")
            }
            rowId += 1
          }
        }
        case _: DoubleColumnVector => {
          for (row <- input.asInstanceOf[DoubleColumnVector].vector) {
            output.write(row + data.asInstanceOf[Double], rowId)
            rowId += 1
          }
        }
        case _ => throw new UnsupportedOperationException(s"Unsupported data type")
      }
    }
  }

  class CopyOperation() extends ColumnVectorOperation {
    def process(input: Any, output: ArrowFieldWriter): Unit = {
      var rowId: Int = 0
      input match {
        case _: LongColumnVector => {
          for (row <- input.asInstanceOf[LongColumnVector].vector) {
            output match {
              case _: LongWriter => {
                output.write(row, rowId)
              }
              case _: IntegerWriter => {
                output.write(row.asInstanceOf[Int], rowId)
              }
              case _ => throw new UnsupportedOperationException(s"Unsupported data type")
            }
            rowId += 1
          }
        }
        case _: DoubleColumnVector => {
          for (row <- input.asInstanceOf[DoubleColumnVector].vector) {
            output.write(row, rowId)
            rowId += 1
          }
        }
        case _: BytesColumnVector => {
          val col = input.asInstanceOf[BytesColumnVector]
          for ( index <- 0 until col.vector.length) {
            output match {
              case _: StringWriter => {
                output.write(
                  UTF8String.fromBytes(col.vector(index), col.start(index), col.length(index)),
                  rowId)
              }
              case _: BinaryWriter => {
                output.write(col.vector(index).slice(col.start(index), col.length(index)), rowId)
              }
              case _ => throw new UnsupportedOperationException(s"Unsupported data type")
            }
            rowId += 1
          }
        }
        case _ => throw new UnsupportedOperationException(s"Unsupported data type")
      }
    }
  }
}
case object ColumnVectorProcessor extends ColumnVectorProcessor(null)

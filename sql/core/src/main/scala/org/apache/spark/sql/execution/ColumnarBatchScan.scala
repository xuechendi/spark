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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.{BoundReference, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}


/**
 * Helper trait for abstracting scan functionality using [[ColumnarBatch]]es.
 */
private[sql] trait ColumnarBatchScan extends CodegenSupport {

  def vectorTypes: Option[Seq[String]] = None

  protected def supportsBatch: Boolean = true

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan time"))

  /**
   * Generate [[ColumnVector]] expressions for our parent to consume as rows.
   * This is called once per [[ColumnarBatch]].
   */
  private def genCodeColumnVectorInRow(
      ctx: CodegenContext,
      columnVar: String,
      ordinal: String,
      dataType: DataType,
      nullable: Boolean): ExprCode = {
    var javaType = CodeGenerator.javaType(dataType)
    val value = CodeGenerator.getValueFromVector(columnVar, dataType, ordinal)
    val isNullVar = if (nullable) {
      JavaCode.isNullVariable(ctx.freshName("isNull"))
    } else {
      FalseLiteral
    }
    val valueVar = ctx.freshName("value")
    val str = s"columnVector[$columnVar, $ordinal, ${dataType.simpleString}]"
    val code = code"${ctx.registerComment(str)}" + (if (nullable) {
      code"""
        boolean $isNullVar = $columnVar.isNullAt($ordinal);
        $javaType $valueVar = $isNullVar ? ${CodeGenerator.defaultValue(dataType)} : ($value);
      """
    } else {
      code"$javaType $valueVar = $value;"
    })
    ExprCode(code, isNullVar, JavaCode.variable(valueVar, dataType))
  }

  /**
   * Generate [[ColumnVector]] expressions for our parent to consume as rows.
   * This is called once per [[ColumnarBatch]].
   */
  private def genCodeColumnVectorInBatch(
      ctx: CodegenContext,
      columnVar: String): ExprCode = {
    var javaType = classOf[ColumnVectorProcessor].getName
    val value = code"$columnVar.createProcessor()"
    val valueVar = ctx.freshName("value")
    val code =
      code"""
        |$javaType $valueVar = $value;
      """.stripMargin
    ExprCode(code, FalseLiteral, JavaCode.variable(valueVar, classOf[ColumnVectorProcessor]))
  }

  /**
   * Produce code to process the input iterator as [[ColumnarBatch]]es.
  private var resultState: String = _
   * This produces an [[UnsafeRow]] for each row in each batch.
   */
  // TODO: return ColumnarBatch.Rows instead
  override protected def doProduce(ctx: CodegenContext): String = {
    // PhysicalRDD always just has one input
    val input = ctx.addMutableState("scala.collection.Iterator", "input",
      v => s"$v = inputs[0];")
    if (supportsBatch) {
      produceBatches(ctx, input)
    } else {
      produceRows(ctx, input)
    }
  }

  private def produceBatches(ctx: CodegenContext, input: String): String = {
    // metrics
    val numOutputRows = metricTerm(ctx, "numOutputRows")
    val scanTimeMetric = metricTerm(ctx, "scanTime")
    val scanTimeTotalNs =
      ctx.addMutableState(CodeGenerator.JAVA_LONG, "scanTime") // init as scanTime = 0

    val columnarBatchClz = classOf[ColumnarBatch].getName
    val batch = ctx.addMutableState(columnarBatchClz, "batch")

    val idx = ctx.addMutableState(CodeGenerator.JAVA_INT, "batchIdx") // init as batchIdx = 0
    val columnVectorClzs = vectorTypes.getOrElse(
      Seq.fill(output.indices.size)(classOf[ColumnVector].getName))
    val (colVars, columnAssigns) = columnVectorClzs.zipWithIndex.map {
      case (columnVectorClz, i) =>
        val name = ctx.addMutableState(columnVectorClz, s"colInstance$i")
        (name, s"$name = ($columnVectorClz) $batch.column($i);")
    }.unzip

    val nextBatch = ctx.freshName("nextBatch")
    val nextBatchFuncName = ctx.addNewFunction(nextBatch,
      s"""
         |private void $nextBatch() throws java.io.IOException {
         |  long getBatchStart = System.nanoTime();
         |  if ($input.hasNext()) {
         |    $batch = ($columnarBatchClz)$input.next();
         |    $numOutputRows.add($batch.numRows());
         |    $idx = 0;
         |    ${columnAssigns.mkString("", "\n", "\n")}
         |  }
         |  $scanTimeTotalNs += System.nanoTime() - getBatchStart;
         |}""".stripMargin)

    ctx.currentVars = null
    val rowidx = ctx.freshName("rowIdx")
    ctx.setBatchRowCount(rowidx)

    ctx.setSupportsBatchProcess()
    val columnsBatchInput = if (ctx.isSupportsBatchProcess()) {
      colVars.map {colVar => genCodeColumnVectorInBatch(ctx, colVar)}
    } else {
      (output zip colVars).map { case (attr, colVar) =>
        genCodeColumnVectorInRow(ctx, colVar, rowidx, attr.dataType, attr.nullable)
      }
    }
    val consume_code = consume(ctx, columnsBatchInput).trim
    val localIdx = ctx.freshName("localIdx")
    val localEnd = ctx.freshName("localEnd")
    val numRows = ctx.freshName("numRows")
    val getRow = ctx.addMutableState(CodeGenerator.JAVA_INT, "getRow")
    val shouldStop = if (parent.needStopCheck) {
      s"if (shouldStop()) { $idx = $rowidx + 1; return; }"
    } else {
      "// shouldStop check is eliminated"
    }
    val processBatch = if(ctx.isSupportsBatchProcess()) {
      s"""
      |int $rowidx = $batch.numRows();
      |${consume_code}
      |$shouldStop
      """
    } else {
      s"""
       |int $numRows = $batch.numRows();
       |int $localEnd = $numRows - $idx;
       |for (int $localIdx = 0; $localIdx < $localEnd; $localIdx++) {
       |  int $rowidx = $idx + $localIdx;
       |  ${consume_code}
       |  $shouldStop
       |}
       |$idx = $numRows;
      """.stripMargin
    }
    s"""
       |if ($batch == null) {
       |  $nextBatchFuncName();
       |}
       |int $getRow = 0;
       |while ($limitNotReachedCond $batch != null) {
       |  $getRow = 1;
       |  $processBatch
       |  $batch = null;
       |  $nextBatchFuncName();
       |}
       |$scanTimeMetric.add($scanTimeTotalNs / (1000 * 1000));
       |$scanTimeTotalNs = 0;
       |if ($getRow == 1) {
       |  append(${ctx.getResultState()});
       |}
     """.stripMargin
  }

  private def produceRows(ctx: CodegenContext, input: String): String = {
    val numOutputRows = metricTerm(ctx, "numOutputRows")
    val row = ctx.freshName("row")

    ctx.INPUT_ROW = row
    ctx.currentVars = null
    s"""
       |while ($limitNotReachedCond $input.hasNext()) {
       |  InternalRow $row = (InternalRow) $input.next();
       |  $numOutputRows.add(1);
       |  ${consume(ctx, null, row).trim}
       |  if (shouldStop()) return;
       |}
     """.stripMargin
  }
}

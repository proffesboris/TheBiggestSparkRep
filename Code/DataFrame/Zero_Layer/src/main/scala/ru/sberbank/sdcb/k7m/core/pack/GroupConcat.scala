package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{ArrayType, DataType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.ArrayBuffer

/**
	* Aggregate function,
	* groups rows by concatenating values with comma separator
	* */
object GroupConcat extends UserDefinedAggregateFunction {

	def inputSchema: StructType = new StructType().add("x", StringType)

	def bufferSchema: StructType = new StructType().add("buff", ArrayType(StringType))

	override def dataType: DataType = StringType

	override def deterministic: Boolean = true

	override def initialize(buffer: MutableAggregationBuffer): Unit = buffer.update(0, ArrayBuffer.empty[String])

	override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
		if (!input.isNullAt(0))
			buffer.update(0, buffer.getSeq[String](0) :+ input.getString(0))
	}

	override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
		buffer1.update(0, buffer1.getSeq[String](0) ++ buffer2.getSeq[String](0))
	}

	override def evaluate(buffer: Row): Any = UTF8String.fromString(
		buffer.getSeq[String](0).mkString(", ")
	)

}

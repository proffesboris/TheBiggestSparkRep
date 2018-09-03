package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class S2T_Stage(spark: SparkSession, schema: String, source: String) {

	import spark.implicits._

	private val input = spark.table(s"$schema.s2t_stage")

	private val inputLowerTrimmed = input.select(input.schema.map{ f => trim(lower(input(f.name))).as(f.name)} : _* )

	private val transformedDataFrame = source match{

		case "ld" | "ld2" =>

			val inputWithCasts = inputLowerTrimmed
				.withColumn("Final_Type", when(
					$"Type_LD" =!= $"Type_OD" && $"Type_LD" === "bigint",
					concat(lit("cast("), $"Field", lit("/ 1000 as timestamp) "), $"Field"))  // cast(field / 1000 as timestamp) field
					.when(
					$"Type_LD" =!= $"Type_OD" && $"Type_LD" =!= "bigint",
					concat(lit("cast("), $"Field", lit(" as "), $"Type_OD", lit(")"), $"Field")) //cast(field_ld as field_od) field_ld
					.otherwise($"Field"))

			inputWithCasts
				.groupBy($"Node", $"Scheme_LD".as("schema"), $"Table")
				.agg(
					first($"Validation_LD").alias("window_function"),
					first($"Where_LD").alias("where"),
					GroupConcat($"Final_Type").alias("columns"),
					GroupConcat($"Field").alias("raw_columns"),
					GroupConcat($"Type_LD").alias("types")
				)


		case "prod" => inputLowerTrimmed
			.groupBy($"Node", $"Scheme_OD".as("schema"), $"Table")
			.agg(
				first($"Validation_OD").alias("window_function"),
				first($"Where_OD").alias("where"),
				GroupConcat($"Field").alias("columns"),
				GroupConcat($"Field").alias("raw_columns"),
				GroupConcat($"Type_OD").alias("types")
			)

	}


	val finalDataFrame: DataFrame = transformedDataFrame.where($"Table" =!= "table")

}

object S2T_Stage{

	def apply(spark: SparkSession, schema: String, source: String): S2T_Stage = new S2T_Stage(spark, schema, source)

}
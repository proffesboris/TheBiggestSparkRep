package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.functions.{lit, trim, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import ru.sberbank.sdcb.k7m.core.pack.Main.spark
import ru.sberbank.sdcb.k7m.core.pack.Utils._

trait Functions {

	import spark.implicits._

	def collectMetaForTable(table: String, tableWithMeta: DataFrame): List[TableMeta] = {
		tableWithMeta.where($"Table" === table.toLowerCase)
			.collect
			.map(r => new TableMeta(r.getAs[String]("Table"),
				r.getAs[String]("schema"),
				r.getAs[String]("columns"),
				r.getAs[String]("where"),
				r.getAs[String]("window_function"),
				r.getAs[String]("types"),
        r.getAs[String]("raw_columns"))).toList
	}

	def getSelectStatement(table: TableMeta): String = {

		val windowFiltered = if (table.Window == null) "" else ", " + table.Window.toString
		val whereFiltered  = if (table.Where == null) "" else table.Where.toString

		val columns_full = if (whereFiltered.contains("ods_opc")) table.columns + ", lower(ods_opc) ods_opc"
									else if (whereFiltered.contains("ctl_action")) table.columns + ", lower(ctl_action) ctl_action"
										 else table.columns

		case class filters(whereFilter: String, windowFilter: String)

		val SelectStatement = filters(whereFiltered, windowFiltered) match {
			case filters("", "") => s"Select $columns_full from ${table.sourceFullName}"
			case filters(_, "")  => s"Select $columns_full from ${table.sourceFullName} where $whereFiltered"
			case filters("", _)  => s"Select a.* from (Select $columns_full $windowFiltered from ${table.sourceFullName}) a where a.rn = 1"
			case filters(_, _) if table.name == "z_territory"  => s"Select a.* from (Select $columns_full $windowFiltered from ${table.sourceFullName} where $whereFiltered) a where rn = 1" // TODO придумать как реализовать это исключение по-нормальному
			case filters(_, _)   => s"Select a.* from (Select $columns_full $windowFiltered from ${table.sourceFullName}) a where rn = 1 and $whereFiltered"
		}

		SelectStatement
	}

	def getDropStatement(table: TableMeta, saveSchema: String): String = s"drop table if exists ${table.saveFullName(saveSchema)}"

	def dropAndSaveTable(table: TableMeta, spark: SparkSession, saveSchema: String, savePath: String): Unit = {

		println("table " + table.saveFullName(saveSchema) + " is built in  " + table.saveFullPath(savePath))
		println(getSelectStatement(table))

		spark.sql(getDropStatement(table, saveSchema))

		var dataframe = spark.sql(getSelectStatement(table))

		if (dataframe.columns.contains("rn")) dataframe = dataframe.drop("rn")
		if (dataframe.columns.contains("ods_opc")) dataframe = dataframe.drop("ods_opc")
		if (dataframe.columns.contains("ctl_action")) dataframe = dataframe.drop("ctl_action")

		replaceEmptyWithNulls(dataframe)
			.replace("[\\n\\r]", " ")
			.write
			.mode("overwrite")
			.option("path", table.saveFullPath(savePath))
			.format("parquet")
			.saveAsTable(s"${table.saveFullName(saveSchema)}")



		// 20.07.2018 Добавили проводки 2016 года, т.к их нет в реплике ОД (архивация ЕКС)
		if (table.name == "z_main_docum"  )
			{
			val z_main_docum_arc_2016 = spark.sql(s"""
				select
						 id,
						 class_id,
						 state_id,
						 c_acc_dt,
						 c_acc_kt,
						 c_date_prov,
						 c_kl_dt_1_1,
						 c_kl_dt_1_2,
						 c_kl_dt_2_1,
						 c_kl_dt_2_inn,
						 c_kl_dt_2_2,
						 c_kl_dt_2_3,
						 c_kl_dt_2_kpp,
						 c_kl_kt_1_1,
						 c_kl_kt_1_2,
						 c_kl_kt_2_1,
						 c_kl_kt_2_inn,
						 c_kl_kt_2_2,
						 c_kl_kt_2_3,
						 c_kl_kt_2_kpp,
						 c_nazn,
						 c_sum,
						 c_valuta,
						 c_vid_doc,
						 c_kod_nazn_pay,
						 c_valuta_po,
						 c_sum_po,
						 c_multicurr,
						 c_sum_nt,
						 c_type_mess,
						 c_code_doc,
						 c_num_dt,
						 c_num_kt,
						 c_filial,
						 c_depart
				from 		$saveSchema.eks_z_main_docum_arc_2016
    		"""  				)

				z_main_docum_arc_2016
					.replace("[\\n\\r]", " ")
					.write
					.mode("append")
					.option("path", table.saveFullPath(savePath))
					.format("parquet")
					.saveAsTable(s"${table.saveFullName(saveSchema)}")
			}

	}

	def tableExists(table: TableMeta, spark: SparkSession): Boolean = {
		if (spark.sql(s"show tables in ${table.sourceSchema} like '${table.name}'").collect().length == 1 ) true else false
	}

	def tableEmpty(table: TableMeta, spark: SparkSession): Boolean = {
		if (spark.sql(s"select * from ${table.sourceFullName} limit 10").collect.length == 0) true else false
	}

	def tableIsInS2T(table: TableMeta, S2T_tableFiltered: DataFrame): Boolean = {
		if (S2T_tableFiltered.select("*").where($"table" === table.name).collect.length == 0) false else true
	}

	def replaceEmptyWithNulls(dataframe: DataFrame): DataFrame = {
		dataframe.select(
			dataframe.schema.map { f =>
				if (f.dataType == StringType) replaceEmptyWithNulls(dataframe(f.name)).as(f.name)
				else dataframe(f.name)
			} : _* )
	}

	def replaceEmptyWithNulls(col: Column): Column = { when (trim(col) === lit(""), lit(null).cast(StringType)).otherwise(trim(col)) }

}

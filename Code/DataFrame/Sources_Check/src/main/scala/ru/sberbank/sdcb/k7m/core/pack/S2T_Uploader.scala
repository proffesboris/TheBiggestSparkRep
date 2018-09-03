package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession
import sys.process._

class S2T_Uploader(spark: SparkSession, schema: String, filePath: String) {

	private val S2TtableSchema = schema

	def createS2Ttable(): Unit = {
		copyFile()
		dropS2TTable()
		createS2TTableDDL()
		replaceEmptyWithNulls()
		loadDataIntoS2Ttable()
	}

	// Copy of file should be used because it is deleted from the directory upon being inserted into hive table
	private def copyFile(): Unit = {
		s"""hdfs dfs -cp -f ${filePath}S2T-Stage.csv ${filePath}S2T-StageLoad.csv""".!
	}

	private def dropS2TTable(): Unit = {
		spark.sql(s"""drop table if exists $S2TtableSchema.S2T_Stage""")
	}

	private def createS2TTableDDL(): Unit = {
		spark.sql(
			s"""create table $S2TtableSchema.S2T_Stage
				 |(Node String,
				 |Scheme_LD String,
				 |Scheme_OD String,
				 |`Table` String,
				 |`Field` String,
				 |Type_LD String,
				 |Type_OD String,
				 |Validation_LD String,
				 |Where_LD String,
				 |Validation_OD String,
				 |Where_OD String,
				 |Comment String)
				 |row format delimited fields terminated by ";"
				 |stored as textfile
				 |tblproperties("skip.header.line.count"="1")""".stripMargin)
	}

	private def replaceEmptyWithNulls(): Unit = {
		spark.sql(s"""alter table $S2TtableSchema.S2T_Stage set serdeproperties ('serialization.null.format' = '')""")
	}

	private def loadDataIntoS2Ttable(): Unit = {
		spark.sql(s"""load data inpath '${filePath}S2T-StageLoad.csv' overwrite into table $S2TtableSchema.S2T_Stage""")
	}

}

object S2T_Uploader {

	def apply(spark: SparkSession, schema: String, filePath: String): S2T_Uploader = new S2T_Uploader(spark, schema, filePath)

}

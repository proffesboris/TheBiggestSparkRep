import org.apache.spark.sql.SparkSession

object Delete_table {
	def main(args: Array[String]): Unit = {

		val params = args.map(_.split("=")).filter(_.length > 0).map(x => (x(0), x(1))).toMap[String, String]

		val DELETE_TIME = params("delete_time")
		val hist_schema = params("schema")

		val spark: SparkSession = SparkSession.builder
			.appName("Create_table")
			.enableHiveSupport()
			.getOrCreate

		spark.sql(s"drop table if exists $hist_schema.smart_src_$DELETE_TIME")

	}
}

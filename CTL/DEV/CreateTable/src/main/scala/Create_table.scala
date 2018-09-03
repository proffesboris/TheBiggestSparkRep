import org.apache.spark.sql.SparkSession

object Create_table {
	def main(args: Array[String]): Unit = {

		val params = args.map(_.split("=")).filter(_.length > 0).map(x => (x(0), x(1))).toMap[String, String]

		val TIMESTAMP = params("timestamp")
		val schema = params("schema")

		val spark: SparkSession = SparkSession.builder
			.appName("Create_table")
			.enableHiveSupport()
			.getOrCreate

		val hist_schema: String = schema + "_hist"

		spark.sql(s"create table $hist_schema.smart_src_$TIMESTAMP as select * from $schema.smart_src")

	}
}


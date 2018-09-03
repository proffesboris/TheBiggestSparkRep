import org.apache.spark.sql.SparkSession

class CreateTable(val spark: SparkSession) {

  val schema = "custom_cb_preapproval"

  def execute(tableNum: String): Unit = {
    val query = s"""create table $schema.table$tableNum as select current_timestamp() as col1"""
    println(query)
    spark.sql(query)
  }
}

object CreateTable {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder()
      .appName("CreateTable")
      .enableHiveSupport()
      .getOrCreate()

    val createTable = new CreateTable(spark)
    createTable.execute(args(0))
  }
}

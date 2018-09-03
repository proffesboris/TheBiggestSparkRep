package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession

object PimUtils {

  def addPath(spark: SparkSession, schema: String, path: String, tableName: String): Unit = {
    spark.sql(s"""alter table $schema.$tableName set SERDEPROPERTIES ('path'='$path$tableName')""")
    spark.catalog.refreshTable(s"""$schema.$tableName""")
  }

}

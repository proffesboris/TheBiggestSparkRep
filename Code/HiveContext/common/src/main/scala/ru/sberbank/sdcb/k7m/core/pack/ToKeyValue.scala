package ru.sberbank.sdcb.k7m.core.pack

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable

object ToKeyValue {

  case class Dt(
                 EXEC_ID: String,
                 OBJ: String,
                 OKR_ID: BigDecimal,
                 OBJ_ID: String,
                 KEY: String,
                 VALUE_C: String,
                 VALUE_N: BigDecimal,
                 VALUE_D: Timestamp
               )

  case class Dict7mkey(
                        obj: Option[String],
                        obj_id: Option[String],
                        code: Option[String],
                        name: Option[String],
                        data_type: Option[String],
                        data_length: Option[String]
               )

  def toKeyValue(
                  dataset: Dataset[_],
                  primaryKeyNames: Seq[String],
                  keyLabel: String,
                  valueMappings: Map[String, String]
                ): DataFrame = {
    val sourceCols = valueMappings.keys.toList
    val targetCols = valueMappings.values.toList.distinct
    val targetToSourceMap = new mutable.HashMap[String, mutable.Set[String]] with mutable.MultiMap[String, String]
    valueMappings.foreach { case (sourceCol, targetCol) =>
      targetToSourceMap.addBinding(targetCol, sourceCol)
    }
    val sourceSchema = dataset.schema.map(field => (field.name.toLowerCase, field.dataType)).toMap
    val targetTypeMap = targetToSourceMap.map { case (targetColName, sourceColNames) =>
      val firstSourceCol = sourceColNames.head
      val otherSourceCols = sourceColNames.tail
      val expectedType = sourceSchema(firstSourceCol.toLowerCase)
      otherSourceCols.foreach { sourceCol =>
        val sourceColType = sourceSchema(sourceCol.toLowerCase)
        if (sourceColType != expectedType)
          throw new Exception(s"Column's $sourceCol type does not match. Expecting $expectedType (inferred from $firstSourceCol), actual $sourceColType.")
      }
      (targetColName, expectedType)
    }.toMap

    dataset
      .withColumn(
        "__values",
        explode(
          array(
            sourceCols.map(sourceColName =>
              struct(
                lit(sourceColName).as(keyLabel) +:
                  targetCols.map { targetColName =>
                    val targetCol = if (valueMappings(sourceColName) == targetColName)
                      dataset(sourceColName)
                    else
                      lit(null).cast(targetTypeMap(targetColName))
                    targetCol.as(targetColName)
                  }: _*
              )): _*
          )))
      .select(
        (primaryKeyNames.map(dataset(_)) :+ col(s"__values.$keyLabel")) ++ targetCols.map(targetCol => col(s"__values.$targetCol")): _*
      )
  }

  def toKeyValue(
                  dataset: Dataset[_],
                  keyColNames: String*
                ): DataFrame = {
    val keyColsSet = keyColNames.map(_.toLowerCase).toSet
    val valueColNames = dataset.schema.map(_.name).filter(name => !keyColsSet.contains(name.toLowerCase))
    toKeyValue(dataset, keyColNames, "key", valueColNames.map(colName => (colName, "value")).toMap)
  }

  def gen(inputSchema: String, inputTable: String, outputSchema: String, outputTable: String)(implicit spark: SparkSession, aux: String, auxPath: String) = {

    import spark.implicits._

    def metadataDS(): Dataset[Dict7mkey] = spark.table(s"$aux.dict_7mkey").as[Dict7mkey]

    val allFields = Set("KEY", "VALUE_C", "VALUE_N", "VALUE_D")

    val columnNameMap = Map(
      "STRING" -> "VALUE_C",
      "NUMBER" -> "VALUE_N",
      "DATE" -> "VALUE_D"
    )

    val formatsMap = Map(
      "VALUE_C" -> "String",
      "VALUE_N" -> "Decimal(38,18)",
      "VALUE_D" -> "Timestamp"
    )

    def expr(myCols: Set[String], allCols: Set[String], obj: String) = {
      col("EXEC_ID").cast("String").as("EXEC_ID") ::
        lit(obj).cast("String").as("OBJ") ::
        lit(null).cast("bigint").as("OKR_ID") ::
        col("OBJ_ID").cast("String").as("OBJ_ID") ::
        allCols.toList.map {
          case "VALUE_N" if myCols.contains("VALUE_N") => col("VALUE_N").cast("decimal(16,4)")
          case "VALUE_N" => lit(null).cast("decimal(16,4)").as("VALUE_N")
          case x if myCols.contains(x) => col(x)
          case x => lit(null).cast(formatsMap(x)).as(x)
        }
    }

    def castFieldsToDestFormat(ds: Dataset[_], colsNameFormat: Map[String, String], objId: String): DataFrame = {
      ds.select(col("EXEC_ID") +: col(objId).as("OBJ_ID") +: colsNameFormat.map(c => col(c._1).cast(formatsMap(c._2))).toSeq: _*)
    }

    def cutStrings(ds: DataFrame, colNameLen: Map[String, Int]): DataFrame = {
      var tmpDs = ds
      colNameLen.foreach { case (name, len) =>
        tmpDs = tmpDs.withColumn(name, col(name).substr(1, len))
      }
      tmpDs
    }

    val metadata = metadataDS().collect()
      .filter(p => p.obj.get.toLowerCase() == inputTable.toLowerCase())

    if (metadata.isEmpty) throw new Exception(s"Table $inputSchema.$inputTable in $aux.dict_7mkey not found")

    val dict = metadata.groupBy(_.obj).head._2

    val tableId = dict.head.obj_id.get

    val columnsInTable = dict.map(d => columnNameMap(d.data_type.get)).toSet + "KEY"
    val valueMappings = dict.map(d => (d.code.get, columnNameMap(d.data_type.get))).toMap
    val colNameLen = dict.filter(x => x.data_type.get == "STRING" && x.data_length.isDefined).map(x => (x.code.get, x.data_length.get.toInt)).toMap

    val fullTableName = s"$inputSchema.$inputTable"
    val df = castFieldsToDestFormat(spark.table(fullTableName), valueMappings, tableId)
    val df2 = cutStrings(df, colNameLen)

    toKeyValue(
      df2,
      Seq("EXEC_ID", "OBJ_ID"),
      "KEY",
      valueMappings
    )
      .select(expr(columnsInTable, allFields, inputTable): _*)
      .where($"VALUE_C".isNotNull or $"VALUE_D".isNotNull or $"VALUE_N".isNotNull)
      .as[Dt]
      .write
      .option("path", s"$auxPath$outputTable")
      .mode(SaveMode.Overwrite)
      .saveAsTable(s"$outputSchema.$outputTable")
  }

}

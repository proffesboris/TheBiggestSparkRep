package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession

object SparkMainClass extends BaseMainClass {

  override def run(params: Map[String, String], config: Config): Unit = {
    val DevSchema = config.aux
    val MartSchema = config.pa

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("fok_base")
      .getOrCreate()

    //заголовки
    val FokDocsHeaderFinalLoan = (new FokDocsHeaderFinalLoanClass(spark, config)).DoFokDocsHeaderFinalLoan()
    val FokDocsHeader = (new FokDocsHeaderClass(spark, config)).DoFokDocsHeader()
    val FokDocsHeaderLoanSbrf = (new FokDocsHeaderLoanSbrfClass(spark, config)).DoFokDocsHeaderLoanSbrf()

    // Долговая нагрузка в других банках
    val FokFinStmtDetail = (new FokFinStmtDetailClass(spark, config)).DoFokFinStmtDetail()
    val FokFinStmtDebt = (new FokFinStmtDebtClass(spark, config)).DoFokFinStmtDebt()

    //РСБУ
    val FokFinStmtRsbu = (new FokFinStmtRsbuClass(spark, config)).DoFokFinStmtRsbu()
    val FokFinStmtRsbuAgr = (new FokFinStmtRsbuAgrClass(spark, config)).DoFokFinStmtRsbuAgr()

    // Долговая нагрузка в СБ РФ
    val FokFinStmtDetailSbrf = (new FokFinStmtLoanSbrfClass(spark, config)).DoFokFinStmtLoanSbrf()
  }
}



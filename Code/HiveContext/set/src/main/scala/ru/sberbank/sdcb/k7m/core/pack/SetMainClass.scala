package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession

object SetMainClass extends BaseMainClass {

  val STAGE_KEY = "stage"
  val possibleStages = List("prep","final")

  override def run(params: Map[String, String], config: Config): Unit = {
    val Stage = obtainStage(params)
    val applicationName = s"SET_$Stage"

    val spark = SparkSession
      .builder
      .enableHiveSupport()
      .appName(applicationName)
      .config("hive.auto.convert.join", "true")
      .getOrCreate

    val paramDate = params.getOrElse("date",LoggerUtils.obtainRunDtWithoutTime(spark,config.aux))

    val processLogger = new ProcessLogger(spark, config, applicationName) // processName- наименование процесса,
    processLogger.logStartProcess()
   if ( Stage == possibleStages(0) ) {
     spark.sql(s"drop table if exists ${config.aux}.set")
     spark.sql(s"drop table if exists ${config.aux}.set_item")
     spark.sql(s"drop table if exists ${config.pa}.set")
     spark.sql(s"drop table if exists ${config.pa}.set_item")

     val TCredPcDf = new TCredPcClass(spark, config)
     val SetCredDf = new SetCredClass(spark, config)
     val SetFactopDf = new SetFactopClass(spark, config)
     val SetCredFiltDf = new SetCredFilteredClass(spark, config)
     val SetStep5Df = new SetStep5Class(spark, config)
     val SetStep6Df = new SetStep6Class(spark, config)
     val SetStep7Df = new SetStep7Class(spark, config)
     val CalcDf = new CalcClass(spark, config)
     val SetZSurDf = new SetZSurClass(spark, config)
     val SetSuretyshipsStatNcDf = new SetSuretyshipsStatNcClass(spark, config)
     val SepLinkCritDualDf = new SepLinkCritDualClass(spark, config)
     val SepBcLinkCritDualDf = new SepBcLinkCritDualClass(spark, config)
     val SepSurety7Mr4BasisClientDf = new SepSurety7Mr4BasisClientClass(spark, config)
     val SepBasisAllSumTrDf = new SepBasisAllSumTrClass(spark, config)
     val SepSurety7Mr4GrBenClassDf = new SepSurety7Mr4GrBenClass(spark, config)
     val SepSurety7Mr4OborotyOrdDf = new SepSurety7Mr4OborotyOrdClass(spark, config)
     val SepSurety7Mr4t2Df = new SepSurety7Mr4t2Class(spark, config)
     val SepSurety7MForNonClientGrBenDf = new SepSurety7MForNonClientGrBenClass(spark, config)
     val SepSurety7MForNonClientSurInitDf = new SepSurety7MForNonClientSurInitClass(spark, config)
     val SetBasisClientinstrRepDf = new SetBasisClientinstrRepClass(spark, config)
     val SetSuretyHeritageDogDf = new SetSuretyHeritageDogClass(spark, config)
     val SetSuretyHeritageDogSurDf = new SetSuretyHeritageDogSurClass(spark, config)
     val SetSuretyHeritageDogSurCntDf = new SetSuretyHeritageDogSurCntClass(spark, config)
     val SetBasisWithHeritanceDf = new SetBasisWithHeritanceClass(spark, config)
     val SetBasisHeritancePlusOverDF = new SetBasisHeritancePlusOverClass(spark, config)
     val SetBasisWithSuretySetOverDF = new SetBasisWithSuretySetClass(spark, config)
     val SetPrepDF = new SetPrepClass(spark, config)
     val SetPrepRiskDF = new SetPrepRiskClass(spark, config)
     val SetSetItemDF = new SetSetItemClass(spark, config)
     val SetDF = new SetClass(spark, config)
     val SetItemDF = new SetItemClass(spark, config)


     TCredPcDf.DoTCredPc()
     SetCredDf.DoSetCred(paramDate)
     SetFactopDf.DoSetFactop()
     SetCredFiltDf.DoSetCredFiltered()
     SetStep5Df.DoSetStep5()
     SetStep6Df.DoSetStep6()
     SetStep7Df.DoSetStep7()
     CalcDf.DoCalc()
     SetZSurDf.DoSetZSur()
     SetSuretyshipsStatNcDf.DoSetSuretyshipsStatNc()
     SepLinkCritDualDf.DoSetsep_link_crit_dual()
     SepBcLinkCritDualDf.DoSetsep_bc_link_crit_dual()
     SepSurety7Mr4BasisClientDf.DoSepSurety7Mr4BasisClient(paramDate)
     SepBasisAllSumTrDf.DoSepBasisAllSumTr()
     SepSurety7Mr4GrBenClassDf.DoSepSurety7Mr4GrBen()
     SepSurety7Mr4OborotyOrdDf.DoSepSurety7Mr4OborotyOrd()
     SepSurety7Mr4t2Df.DoSepSurety7Mr4t2()
     SepSurety7MForNonClientGrBenDf.DoSepSurety7MForNonClientGrBen()
     SepSurety7MForNonClientSurInitDf.DoSepSurety7MForNonClientSurInit()
     SetBasisClientinstrRepDf.DoSetBasisClientinstrRep()
     SetSuretyHeritageDogDf.DoSetSuretyHeritageDog(paramDate)
     SetSuretyHeritageDogSurDf.DoSetSuretyHeritageDogSur()
     SetSuretyHeritageDogSurCntDf.DoSetSuretyHeritageDogSurCnt()
     SetBasisWithHeritanceDf.DoSetBasisWithHeritance()
     SetBasisHeritancePlusOverDF.DoSetBasisHeritancePlusOver()
     SetBasisWithSuretySetOverDF.DoSetBasisWithSuretySet(paramDate)
     SetPrepDF.DoSetPrep()
     SetPrepRiskDF.DoSetPrepRisk()
     SetSetItemDF.DoSetSetItem()
     SetDF.DoSet()
     SetItemDF.DoSetItem()
   }
   else if ( Stage == possibleStages(1) ) {
     val SetFinal = new SetFinalClass(spark, config)
     SetFinal.run()
   }
    processLogger.logEndProcess()
  }

  def obtainStage(params: Map[String, String]): String = {
    if (!params.contains(STAGE_KEY))  possibleStages(0)
    else if (params.contains(STAGE_KEY)  &&   possibleStages.contains(params(STAGE_KEY))     )  params(STAGE_KEY)
    else throw new Exception(s"Wrong stage in parameters. Use stage -> ${possibleStages(0)} or ${possibleStages(1)}")
  }

}

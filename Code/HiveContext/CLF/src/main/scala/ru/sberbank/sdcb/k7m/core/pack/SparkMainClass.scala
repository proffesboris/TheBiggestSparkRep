package ru.sberbank.sdcb.k7m.core.pack
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkMainClass extends BaseMainClass {

//  val requiedFields: String = s"""    """
  //val hashFields =

  //---------------------------------------------------------
  override def run(params: Map[String, String], config: Config): Unit = {

    val Stg0Schema = config.stg

    val DevSchema = config.aux

    val MartSchema = config.pa

    val SchemaAuxP = "t_team_k7m_aux_p"
    //---------------------------------------------------------
    val spark = SparkSession  .builder()
       .config("spark.kryoserializer.buffer.max.mb", "512")
      .enableHiveSupport()
      .appName("CLF")
      .getOrCreate()

    LoggerUtils.createTables(spark, DevSchema) //создание таблиц через if not exists
    val processLogger = new ProcessLogger(spark, config, "CLF") // processName- наименование процесса,
    processLogger.logStartProcess()
    // Заполнение t_team_k7m_aux_d.clf_raw_keys

    val CrmAddr = (new CrmAddrClass(spark, config)).DoCrmAddr()
                   // val IntegrumEio = (new IntegrumEioClass(spark, config)).DoIntegrumEio()//уже не нужна
                   // val CrmRegDoc = (new CrmRegDocClass(spark, config)).DoCrmRegDoc()//уже не нужна
                   // val CrmBen = (new CrmBenClass(spark, config)).DoCrmBen() //уже не нужна
    val ClfRawKeys = (new ClfRawKeysClass(spark, config)).DoClfRawKeys()
    val TmpClfRawKeys = (new TmpClfRawKeysClass(spark, config)).DoTmpClfRawKeys()
  //CLF
//Обогащение из МДМ
    val TmpClfMdmFilter = (new TmpClfMdmFilterClass(spark, config)).DoTmpClfMdmFilter()
    val TmpClfMdmClf = (new TmpClfMdmClfClass(spark, config)).DoTmpClfMdmClf()
    val TmpClfMdmClfStatus = (new TmpClfMdmClfStatusClass(spark, config)).DoTmpClfMdmClfStatus()
    val TmpClfMdmClfName = (new TmpClfMdmClfNameClass(spark, config)).DoTmpClfMdmClfName()
    val TmpClfMdmPers = (new TmpClfMdmPersClass(spark, config)).DoTmpClfMdmPers()
    val TmpClfMdmCntry = (new TmpClfMdmCntryClass(spark, config)).DoTmpClfMdmCntry()
     val TmpClfMdmDul = (new TmpClfMdmDulClass(spark, config)).DoTmpClfMdmDul()
     val TmpClfMdmContLnk1 = (new TmpClfMdmContLnk1Class(spark, config)).DoTmpClfMdmContLnk1()
     val TmpClfMdmContLnk2 = (new TmpClfMdmContLnk2Class(spark, config)).DoTmpClfMdmContLnk2()
     val TmpClfMdmCont = (new TmpClfMdmContClass(spark, config)).DoTmpClfMdmCont()
     val TmpClfMdmAdrLnk = (new TmpClfMdmAdrLnkClass(spark, config)).DoTmpClfMdmAdrLnk()
     val TmpClfMdmAdr = (new TmpClfMdmAdrClass(spark, config)).DoTmpClfMdmAdr()
     val TmpClfMmdmUdbo = (new TmpClfMmdmUdboClass(spark, config)).DoTmpClfMmdmUdbo()
     val TmpClfMdmSystem = (new TmpClfMdmSystemClass(spark, config)).DoTmpClfMdmSystem()
     val CLFMdm = (new CLFMdmClass(spark, config)).DoCLFMdm()
     val TmpClfMdmDedubl = (new TmpClfMdmDedublClass(spark, config)).DoTmpClfMdmDedubl()
     val ClfFpersMdmKi = (new ClfFpersMdmKiClass3(spark, config)).DoClfFpersKiMdm3()
     val ClfFpersMdm = (new ClfFpersMdmClass(spark, config)).DoClfFpersMdm()
  //Обогащение из СРМ
     val TmpClfCrmcbFilter = (new TmpClfCrmcbFilterClass(spark, config)).DoTmpClfCrmcbFilter()
     val TmpClfCrmcbClf = (new TmpClfCrmcbClfClass(spark, config)).DoTmpClfCrmcbClf()
     val TmpClfCrmcbDul = (new TmpClfCrmcbDulClass(spark, config)).DoTmpClfCrmcbDul()
     val TmpClfCrmcbPositions = (new TmpClfCrmcbPositionsClass(spark, config)).DoTmpClfCrmcbPosition()
     val ClfCrmcb = (new ClfCrmcbClass(spark, config)).DoClfCrmcb()
      val ClfCrmcbDedubl = (new ClfCrmcbDedublClass(spark, config)).DoClfCrmcbDedubl()
      val ClfFpersCrmcbKi = (new ClfFpersCrmcbKiClass2(spark, config)).DoClfFpersCrmcbKi2()
      val ClfFpersCrmcb = (new ClfFpersCrmcbClass(spark, config)).DoClfFpersCrmcb()
  //То, что не пришло из СРМ и МДМ
      val ClfBad = (new ClfBadClass(spark, config)).DoClfBad()
      val ClfBadFiltered = (new ClfBadFilteredClass(spark, config)).DoClfBadFiltered()
  //То, что пришло из СРМ и МДМ
      val ClfFpersKi = (new ClfFpersKiClass(spark, config)).DoClfFpersKi()
  //Заполнение CLF_KEYS
    val ClfKeys = (new ClfKeysClass(spark, config)).DoClfKeys()
  //Формирование итоговых витрин
  //CRM
    val ClfCrmRn = (new ClfCrmRnClass(spark, config)).DoClfCrmRn()
    val ClfCrm = (new ClfCrmClass(spark, config)).DoClfCrm()
  //MDM
    val ClfMdmRn = (new ClfMdmRnClass(spark, config)).DoClfMdmRn()
    val ClfMdmR = (new ClfMdmRClass(spark, config)).DoClfMdmR()
  //Итоговый CLF
      val CLF = (new CLFClass(spark, config)).DoCLF()

    processLogger.logEndProcess()

   }
}


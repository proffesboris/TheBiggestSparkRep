package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by sbt-medvedev-ba on 08.02.2018.
  */
object SparkMainClass extends BaseMainClass {
  //---------------------------------------------------------
  val limitKey = "limit"

  val TableNameClCrm = ".tka_client_crm"
  val TableNameClCrmRating = ".tka_client_crm_rating"
  val TableNameClCrmKred = ".tka_client_crm_kred"
  val TableNameClEks = ".tka_client_eks"
  val TableNameClAcc = ".tka_client_acc"
  val TableNameClCred = ".tka_client_credit"
  val TableNameClGuar = ".tka_client_guar"
  val TableNameClGenAgrFrame = ".tka_client_gen_agreem_frame"
  val TableNameIntPrv = ".tka_integr_pravo"
  val TableNameIntEgrul = ".tka_integr_egrul"
  val TableNameIntPredCes = ".tka_integr_predecessor"
  val TableNameIntRosSt = ".tka_integr_rosstat"
  val TableNameIntRosSt2 = ".tka_integr_rosstat2"
  val TableNameIntFinStRos = ".tka_fin_state_rosstat"
  val TableNameStgBasOkv = ".basis_client_okved"
  val TableNameStgBasCl = ".basis_client"


  override def run(params: Map[String, String], config: Config): Unit = {

  //  val TargSchema = config.aux
  //  val SourceSchemaDev = config.stg

    val Stg0Schema = config.stg

    val DevSchema = config.aux

    val MartSchema = config.pa

    //---------------------------------------------------------

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("Basis")
      .getOrCreate()


   LoggerUtils.createTables(spark, DevSchema) //создание таблиц через if not exists

  val limit: Int = params.getOrElse(limitKey,"0").toInt

  val processLogger = new ProcessLogger(spark, config, "Basis") // processName- наименование процесса,
  processLogger.logStartProcess()

  val BasisOrgExt =   new BasisOrgExtClass(spark,config).DoBasisOrgExt()

  val BasisOrgExtX = new BasisOrgExtXClass(spark, config).DoBasisOrgExtX()//(spark)

  val BasisOrgExtFnx = new BasisOrgExtFnxClass(spark, config).DoBasisOrgExtFnx()//(spark)

  val BasisOrgClCrm = new BasisClCrmClass(spark, config).DoBasisClCrm()//(spark)

  val BasisOrgClCrmRating = new BasisClCrmRatingClass(spark, config).DoBasisClCrmRating()//(spark)

  val BasisOrgClCrmKred = new BasisClCrmKredClass(spark, config).DoBasisClCrmKred()//(spark)

  val BasisClientEks = new BasisClEksClass(spark, config).DoBasisClEks()//(spark)

  val BasisClEksAcc = new BasisClEksAccClass(spark, config).DoBasisClEksAcc()//(spark)

  val BasisEksClCred = new BasisEksClCredClass(spark, config).DoBasisEksClCred()//(spark)

  val BasisEksClientGuar = new BasisEksClGuarClass(spark, config).DoBasisEksClGuar()//(spark)//не хватает таблички в нулевом слое eks_z_kind_credits//перестроить

  val BasisEksOrgClGenAgrFrame = new BasisEksClGenAgrFrameClass(spark, config).DoBasisEksClGenAgrFrame()//(spark)//не хватает таблички в нулевом слое eks_z_kind_credits//перестроить

  val BasisClientEksZalog = new BasisEksClZalogClass(spark, config).DoBasisEksClZalog()//(spark) //УБРАТЬ не нужно /Озеров/

  val BasisClientIntPrv = new BasisIntPrvClass(spark, config).DoBasisIntPrv()//(spark)

  val BasisClientIntEgrul = new BasisIntEgrulClass(spark, config).DoBasisIntEgrul()//(spark)

  val BasisClientIntPredCes = new BasisIntPredCesClass(spark, config).DoBasisIntPredCes()//(spark)

  val BasisClientIntRosSt = new BasisIntRosStClass(spark, config).DoBasisIntRosSt()//(spark)

  val BasisClientIntRosSt2 = new BasisIntRosSt2Class(spark, config).DoBasisIntRosSt2()//(spark)

  val BasisClientFinStRos = new BasisIntFinStRosClass(spark, config).DoBasisFinStRos()//(spark)

  val BasisClientOkved = new BasisClientOkvedClass(spark, config).DoBasisClientOkved()//(spark)

  val BasisClient0 = new BasisClient0Class(spark, config).DoBasisClient0()//(spark)

  val BasisClient = new BasisClientClass(spark, config).DoBasisClient(limit)//(spark)

  processLogger.logEndProcess()

  }
}


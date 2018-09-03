lazy val root = (project in file(".")).
  aggregate(acc, amrlirtBase, basisTransactionEks, zeroLayer, sourcesCheck, basis,
    basisTran,
    bkp,
    clKeys,
    CLF, CLU, Score,
    crmOrgMajor,
    exsGen,
    fok_base,
    crmBase, econLinks,
    EksEiogen,
    eksId, common, gszForCrm, integrumBase, km, proxyLgdIn, sbl,
    rlwi, vdLawRobotOut, vdMmzOut, zeroLayerHiveContext, pdekSin,
    pdfokin,
    pdintegrumin,
    pdpravoin,
    PlanLiab,
    proxyRiskSegment,
    rep,
    skroff,
    limitIn,
    etlRunner, boPrep, boGen, UL, Tests, groupInfluenceIn, LK, SET, VDCRMOut, VDOKROut, BBO, boFinal, pim_refresh, rsInt, RL,
    dictLoad).
  settings(inThisBuild(List(
    scalaVersion := "2.11.11",
    organization := "ru.sberbank.sdcb.k7m",
    libraryDependencies ++= sparkDependencies ++ sparkHiveDependencies
  )),
    name := "Root K7M project"
  )

val sparkVersion = "2.2.0"

lazy val sparkDependencies = Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
)

lazy val sparkHiveDependencies = Seq(
    "org.apache.spark" %% "spark-hive" % sparkVersion % Provided
)

lazy val acc = (project in file("HiveContext/acc")).
  settings().dependsOn(common)

lazy val amrlirtBase = (project in file("DataFrame/AMRLIRT_BASE")).
  settings().dependsOn(common)

lazy val basisTransactionEks = (project in file("DataFrame/Basis_Transaction_EKS")).
  settings().dependsOn(common)

lazy val basisTran = (project in file("HiveContext/Basis_tran")).
  settings().dependsOn(common)

lazy val bkp = (project in file("HiveContext/bkp")).
  settings().dependsOn(common)

lazy val zeroLayer = (project in file("DataFrame/Zero_Layer")).
  settings().dependsOn(common)

lazy val sourcesCheck = (project in file("DataFrame/Sources_Check")).
  settings().dependsOn(common, zeroLayer)

lazy val basis = (project in file("HiveContext/Basis")).
  settings().dependsOn(common)

lazy val clKeys = (project in file("HiveContext/cl_keys")).
  settings().dependsOn(common)

lazy val CLF = (project in file("HiveContext/CLF")).
  settings().dependsOn(common)

lazy val CLU = (project in file("HiveContext/CLU")).
  settings().dependsOn(common)

lazy val Score = (project in file("HiveContext/Score")).
  settings().dependsOn(common)

lazy val crmBase = (project in file("HiveContext/CRM_Base")).
  settings().dependsOn(common)

lazy val crmOrgMajor = (project in file("HiveContext/CRM_org_major")).
  settings().dependsOn(common)


lazy val exsGen = (project in file("HiveContext/exsgen")).
  settings().dependsOn(common)

lazy val fok_base = (project in file("HiveContext/fok_base")).
  settings().dependsOn(common)

lazy val econLinks = (project in file("HiveContext/Econ_Links")).
  settings().dependsOn(common)

lazy val EksEiogen = (project in file("HiveContext/EKS_EIOGEN")).
  settings().dependsOn(common)

lazy val eksId = (project in file("HiveContext/EKS_ID")).
  settings().dependsOn(common)

lazy val common = (project in file("HiveContext/common")).
  settings()

lazy val gszForCrm = (project in file("HiveContext/gsz_for_crm")).
  settings().dependsOn(common)

lazy val integrumBase = (project in file("HiveContext/Integrum_base")).
  settings().dependsOn(common)

lazy val km = (project in file("HiveContext/KM")).
  settings().dependsOn(common)

lazy val proxyLgdIn = (project in file("HiveContext/ProxyLGDIn")).
  settings().dependsOn(common)

lazy val rlwi = (project in file("HiveContext/RLWI")).
  settings().dependsOn(common)

lazy val vdLawRobotOut = (project in file("HiveContext/VDLawRobotOut")).
  settings().dependsOn(common)

lazy val vdMmzOut = (project in file("HiveContext/VDMMZOut")).
  settings().dependsOn(common)

lazy val zeroLayerHiveContext = (project in file("HiveContext/zero_layer")).
  settings()

lazy val pdekSin = (project in file("HiveContext/PDEKSin")).
  settings().dependsOn(common)

lazy val pdfokin = (project in file("HiveContext/pdfokin")).
  settings().dependsOn(common)

lazy val pdintegrumin = (project in file("HiveContext/pdintegrumin")).
  settings().dependsOn(common)

lazy val pdpravoin = (project in file("HiveContext/pdpravoin")).
  settings().dependsOn(common)

lazy val PlanLiab = (project in file("HiveContext/Plan_liab")).
  settings().dependsOn(common)

lazy val proxyRiskSegment = (project in file("HiveContext/Proxy_risk_segment")).
  settings().dependsOn(common)

lazy val rep = (project in file("HiveContext/REP")).
  settings().dependsOn(common)

lazy val skroff = (project in file("HiveContext/skroff")).
  settings().dependsOn(common)

lazy val sbl = (project in file("HiveContext/SBL")).
  settings().dependsOn(common)

lazy val etlRunner = (project in file("HiveContext/etlRunner")).
  settings().dependsOn(common)

lazy val UL = (project in file("HiveContext/UL")).
  settings().dependsOn(common)

lazy val Tests = (project in file("HiveContext/Tests")).
  settings().dependsOn(common)

lazy val boGen = (project in file("HiveContext/bo_gen")).
  settings().dependsOn(common)

lazy val boPrep = (project in file("HiveContext/bo_prep")).
  settings().dependsOn(common)

lazy val groupInfluenceIn = (project in file("HiveContext/GroupInfluenceIn")).
  settings().dependsOn(common)

lazy val LK = (project in file("HiveContext/lk")).
  settings().dependsOn(common)

lazy val SET = (project in file("HiveContext/set")).
  settings().dependsOn(common)

lazy val VDCRMOut = (project in file("HiveContext/VDCRMOut")).
  settings().dependsOn(common)

lazy val VDOKROut = (project in file("HiveContext/VDOKROut")).
  settings().dependsOn(common)

lazy val BBO = (project in file("HiveContext/BBO")).
  settings().dependsOn(common)

lazy val limitIn = (project in file("DataFrame/LIMITIN")).
  settings().dependsOn(common)

lazy val boFinal = (project in file("HiveContext/bo_final")).
  settings().dependsOn(common)

lazy val pim_refresh = (project in file("HiveContext/pim_refresh")).
  settings().dependsOn(common)

lazy val rsInt = (project in file("HiveContext/RS_INT")).
  settings().dependsOn(common)

lazy val dictLoad = (project in file("HiveContext/dictload")).
  settings().dependsOn(common)

lazy val RL = (project in file("HiveContext/rl")).
  settings().dependsOn(common)
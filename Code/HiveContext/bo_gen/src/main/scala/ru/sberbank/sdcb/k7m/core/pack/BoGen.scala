package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.{SaveMode, SparkSession}

class BoGen(override val spark: SparkSession, val config: Config) extends EtlJob with EtlLogger {



  override val processName = "BO_GEN"
  var dashboardName = s"${config.pa}.bo"
  val tblBoGen = s"${config.aux}.bo_gen"

  case class Table(schema: String, name: String, select: String) {
    override def toString: String = s"$schema.$name"
  }

  def gen() = {
  /*  spark.sql(s"drop table if exists ${config.pa}.bo_keys")

    spark.sql(s"""Create table ${config.pa}.bo_keys (
                 |exec_id            string,
                 |okr_id             bigint,
                 |bo_id             string,
                 |u7m_id            string,
                 |CUBE_ID           bigint,
                 |clust_id           string
                 |) stored as parquet""".stripMargin)
*/
    dashboardName = s"${config.pa}.bo_keys"

    spark.sql(s"""SELECT
                |'$execId' as exec_id
                |,cast(null as bigint) as okr_id
                |,CAST(ROW_NUMBER() OVER (order by cube_id) as string) as BO_ID
                |,b.u7m_id as u7m_id
                |,cc.cube_id as CUBE_ID
                |,cl.name as clust_id
                |FROM ${config.pa}.CLU b CROSS JOIN ${config.aux}.dict_BO_cube2cluster cc left join ${config.aux}.dict_bo_clusters cl on cc.clust_id=cl.clust_id
                |where b.flag_basis_client='Y' --Только клиенты из базиса""".stripMargin)
      .write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .option("path", s"${config.paPath}bo_keys")
      .saveAsTable(s" ${config.pa}.bo_keys")

    logInserted()

    spark.sql(s"drop table if exists ${config.aux}.bo_empty")

    spark.sql(s"""Create table ${config.aux}.bo_empty (
                 |exec_id           string,
                 |okr_id            bigint,
                 |bo_id             string,
                 |u7m_id            string,
                 |clust_id          string,
                 |key               string,
                 |value_c           string,
                 |value_n           decimal(16,5),
                 |value_d           timestamp
                 |) stored as parquet""".stripMargin)

    spark.table(s"${config.aux}.bo_empty")
      .write
      .mode(SaveMode.Overwrite)
      .option("path", s"${config.auxPath}bo_gen")
      .format("parquet")
      .saveAsTable(tblBoGen)
  }

  def dynSql() = {

    val dynSql1 = spark.sql(raw"""select
                              |feature_id,feature_cd,func_desc,
                              |concat(
                              |',CASE WHEN UPPER(func_desc) like UPPER(\'%RESULT.'
                              |,feature_cd
                              |,'%\') then '
                              |,'\' LEFT JOIN (select bo_id,value_n as '
                              |,feature_cd
                              |,' from $tblBoGen where key=\\\''
                              |,feature_cd
                              |,'_L\\\') '
                              |,feature_cd
                              |,'_L ON '
                              |,feature_cd
                              |,'_L.BO_ID=B.BO_ID \''
                              |,' ELSE \'\' END ') sql_txt
                              |from ${config.aux}.dict_bo_features where type_name='вещественная'
                              |order by feature_id""".stripMargin).select("sql_txt").collect.map(r => r.getString(0)).mkString("\n|")

    val dynSql2 = spark.sql(raw"""select
                              |feature_id,feature_cd,func_desc,
                              |concat(
                              |',CASE WHEN UPPER(func_desc) like UPPER(\'%RESULT.'
                              |,feature_cd
                              |,'%\') then '
                              |,'\' LEFT JOIN (select bo_id,value_n as '
                              |,feature_cd
                              |,' from $tblBoGen where key=\\\''
                              |,feature_cd
                              |,'_U\\\') '
                              |,feature_cd
                              |,'_U ON '
                              |,feature_cd
                              |,'_U.BO_ID=B.BO_ID \''
                              |,' ELSE \'\' END ') sql_txt
                              |from ${config.aux}.dict_bo_features where type_name='вещественная'
                              |order by feature_id""".stripMargin).select("sql_txt").collect.map(r => r.getString(0)).mkString("\n|")

    spark.sql(s"drop table if exists ${config.aux}.bo_gen_TMP_SQL")

    spark.sql(s"CREATE TABLE ${config.aux}.bo_gen_TMP_SQL (feature_id BIGINT,SQL_TXT STRING)")

    dashboardName = s"${config.aux}.bo_gen_TMP_SQL"

    spark.sql(raw"""INSERT INTO ${config.aux}.bo_gen_TMP_SQL
                |SELECT feature_id,CONCAT(
                |'INSERT INTO $tblBoGen select \'$execId\' as exec_id,cast(null as bigint) as okr_id,B.BO_ID as BO_ID,B.u7m_id as u7m_id,b.clust_id as clust_id,CONCAT(F.feature_cd,\'_L\') as KEY,NULL as VALUE_C,cast('
                |,REGEXP_REPLACE(REGEXP_REPLACE(FUNC_DESC,'<X>','C.MIN_VAL'),'RESULT.','')
                |,' as DECIMAL(16,5)) as VALUE_N ,NULL as VALUE_D from ${config.pa}.bo_keys B,${config.aux}.bo_prep EXTvalues ,${config.aux}.dict_bo_cubes C ,${config.aux}.dict_bo_features F '
                |--START SQL 1 result
                |$dynSql1
                |--END  SQL 1 result
                |,' WHERE B.u7m_id=EXTvalues.u7m_id and B.cube_id= c.cube_id and C.FEATURE_ID = F.feature_id and F.feature_cd=\'',
                |feature_cd --SQL создается для каждой feature т.к. у каждой записи может быть своя формула в FUNC_DESC
                |,'\''-- LIMIT 10' --ограничение Временно для тестирования
                |,''
                |) FROM ${config.aux}.dict_bo_features WHERE TYPE_NAME = 'вещественная'""".stripMargin)
    
    spark.sql(raw"""INSERT INTO ${config.aux}.bo_gen_TMP_SQL
                |SELECT 1000+feature_id,CONCAT(
                |'INSERT INTO $tblBoGen select \'$execId\' as exec_id,cast(null as bigint) as okr_id,B.BO_ID as BO_ID,B.u7m_id as u7m_id,b.clust_id as clust_id,CONCAT(F.feature_cd,\'_U\') as KEY,NULL as VALUE_C,cast('
                |,REGEXP_REPLACE(REGEXP_REPLACE(FUNC_DESC,'<X>','C.MAX_VAL'),'RESULT.','')
                |,' as DECIMAL(16,5)) as VALUE_N ,NULL as VALUE_D from ${config.pa}.bo_keys B,${config.aux}.bo_prep EXTvalues ,${config.aux}.dict_bo_cubes C ,${config.aux}.dict_bo_features F '
                |--START SQL 2 RESULT
                |$dynSql2
                |--END  SQL 2 RESULT
                |,' WHERE B.u7m_id=EXTvalues.u7m_id and B.cube_id= c.cube_id and C.FEATURE_ID = F.feature_id and F.feature_cd=\'',
                |feature_cd --SQL создается для каждой feature т.к. у каждой записи может быть своя формула в FUNC_DESC
                |,'\''-- LIMIT 10' --ограничение Временно для тестирования
                |,''
                |) FROM ${config.aux}.dict_bo_features WHERE TYPE_NAME = 'вещественная'""".stripMargin)
    
    spark.sql(raw"""INSERT INTO ${config.aux}.bo_gen_TMP_SQL
                |SELECT 10000+feature_id,CONCAT(
                |'INSERT INTO $tblBoGen select \'$execId\' as exec_id,cast(null as bigint) as okr_id,B.BO_ID as BO_ID,B.u7m_id as u7m_id,b.clust_id as clust_id,F.feature_cd as KEY,c.categorial_val as VALUE_C,NULL as VALUE_N ,NULL as VALUE_D from ${config.pa}.bo_keys B,${config.aux}.bo_prep EXTvalues ,${config.aux}.dict_bo_cubes C ,${config.aux}.dict_bo_features F WHERE B.u7m_id=EXTvalues.u7m_id and B.cube_id= c.cube_id and C.FEATURE_ID = F.feature_id and F.feature_cd=\'',
                |feature_cd --SQL создается для каждой feature т.к. у каждой записи может быть своя формула в FUNC_DESC
                |,'\''-- LIMIT 10' --ограничение Временно для тестирования
                |,''
                |) FROM ${config.aux}.dict_bo_features WHERE TYPE_NAME = 'категориальная'""".stripMargin)

    logInserted()

    spark.table(s"${config.aux}.bo_gen_TMP_SQL").select("sql_txt").collect.map(r => r.getString(0))

  }

  def run() {

    logStart()

    gen()

    dashboardName = tblBoGen

    (dynSql() ++ Seq(
      s"INSERT INTO $tblBoGen select '$execId' as exec_id,cast(null as bigint) as okr_id,B.BO_ID as BO_ID,B.u7m_id as u7m_id,b.clust_id as clust_id,'BO_OfferExpDate' as KEY,null as VALUE_C,null as VALUE_N ,cast(TO_DATE(date_add(current_timestamp(),8)) as timestamp) as VALUE_D FROM ${config.pa}.bo_keys B",
      s"INSERT INTO $tblBoGen select '$execId' as exec_id,cast(null as bigint) as okr_id,B.BO_ID as BO_ID,B.u7m_id as u7m_id,b.clust_id as clust_id,'BO_Q_SRC' as KEY,null as VALUE_C,cast(cc.bo_quality as decimal(16,5)) as VALUE_N ,NULL as VALUE_D FROM ${config.pa}.bo_keys B inner join ${config.aux}.dict_BO_cube2cluster cc on cc.cube_id = B.cube_id"
    )).foreach(spark.sql(_))

    logInserted()

    logEnd()
  }
}
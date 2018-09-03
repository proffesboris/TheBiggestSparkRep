package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class BasisIntPredCesClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val Stg0Schema = config.stg
  val DevSchema = config.aux

  //-----------------------Integrum_pravo-----------------
  val Node1t_team_k7m_aux_d_basis_integr_predecessorIN = s"${Stg0Schema}.int_predecessor"
  val Node2t_team_k7m_aux_d_basis_integr_predecessorIN = s"${DevSchema}.basis_integr_egrul"
  val Node3t_team_k7m_aux_d_basis_integr_predecessorIN = s"${Stg0Schema}.int_ul_organization_egrul"
  val Nodet_team_k7m_aux_d_basis_integr_predecessorOUT = s"${DevSchema}.basis_integr_predecessor"
  val dashboardPath = s"${config.auxPath}basis_integr_predecessor"


  override val dashboardName: String = Nodet_team_k7m_aux_d_basis_integr_predecessorOUT //витрина
  override def processName: String = "Basis"

  def DoBasisIntPredCes()//(spark:org.apache.spark.sql.SparkSession)
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_basis_integr_predecessorOUT).setLevel(Level.WARN)
    logStart()


    val smartSrcHiveTable_t7 = spark.sql(
      s"""
         select     org_id
                   ,ul_inn
                   ,ul_kpp
                   ,ul_ogrn
                   ,ul_reg_first_dt
                   ,ul_reg_ogrn_dt
                   ,ul_ogrn_year
                   ,ul_national_nm
                   ,ul_firm_nm
                   ,ul_full_nm
                   ,ul_status_nm
                   ,ul_activity_stop_method_nm
                   ,p_inn
                   ,p_kpp
                   ,p_ogrn
                   ,p_nm
                   ,p_status_nm
                   ,p_status_dt
                   ,p_active_flg
                   ,p_activity_stop_dt
                   ,p_activity_stop_method_nm
                   ,p_reg_first_dt
                   ,p_reg_ogrn_dt
                   ,p_is_active_in_period
            from
         (select 	t. org_id,								 	--уникальный id в cb_akm_integrum.predecessor
         		t. ul_inn,									--ИНН организации
         		t. ul_kpp,									--КПП организации
         		t. ul_ogrn,									--ОГРН организации
         		t. ul_reg_first_dt, 						--Дата первичной регистрации организации
         		t. ul_reg_ogrn_dt, 							-- &&&new&&& Дата присвоения ОГРН организации
         		case when substr(t. ul_ogrn,2,2)>50
         			then concat('19',substr(t. ul_ogrn,2,2))
         			else concat('20',substr(t. ul_ogrn,2,2))
         		end ul_ogrn_year,							-- &&&new&&& Год регистрации ОГРН организации, полученный из кода ОГРН
         		t. ul_national_nm,							--Наименование на национальном языке
         		t. ul_firm_nm,								--Фирменное наименование
         		t. ul_full_nm,								--Полное наименование
         		t. ul_status_nm,					 		--Статус организации
         		t. ul_activity_stop_method_nm,				--Метод прекращения деятельности
         		p. p_inn,									--ИНН предшественника
         		p. p_kpp,									--КПП предшественника
         		p. p_ogrn,									--ОГРН предшественника
         		p.p_nm,										--Наименование предшественника
         		ui.ul_status_nm as p_status_nm,				--Статус предшественника в ЕГРЮЛ
         		ui.ul_status_dt as p_status_dt,				--Дата установления статуса предшественника в ЕГРЮЛ
         		ui.ul_active_flg as p_active_flg,			--Признак активности записи по предшественнику в ЕГРЮЛ
         		ui.ul_activity_stop_dt as p_activity_stop_dt,--Дата прекращения деятельности предшественника
         		ui.ul_activity_stop_method_nm as p_activity_stop_method_nm,--Способ прекращения деятельности предшественника
         		ui.ul_reg_first_dt as p_reg_first_dt,		--Дата первичной регистрации предшественника
         		ui.ul_reg_ogrn_dt as p_reg_ogrn_dt,			--Дата присвоения ОГРН предшественника
         		ui.is_active_in_period  as p_is_active_in_period,	 --Признак активности внутри периода для предшественника
         		row_number() over (partition by p.egrul_org_id order by p.effectivefrom desc) as rn
            from $Node1t_team_k7m_aux_d_basis_integr_predecessorIN p
            join $Node2t_team_k7m_aux_d_basis_integr_predecessorIN  t on t.org_id=p.egrul_org_id
            left join (select
         				 o.egrul_org_id as org_id,								 	--уникальный id в core_internal_integrum.ul_organization_egrul
         				 o.ul_inn,												 	--ИНН организации
         				 o.ul_kpp,												 	--КПП организации
         				 o.ul_ogrn,												 	--ОГРН организации
         				 o.ul_national_nm,										 	--Наименование на национальном языке
         				 o.ul_firm_nm,										 	 	--Фирменное наименование
         				 o.ul_full_nm,										 	 	--Полное наименование
         				 o.ul_status_nm,										 	--Статус организации
         				 cast(o.ul_status_dt as timestamp) as ul_status_dt,		 	--Дата установления статуса организации
         				 o.ul_active_flg,										 	--Является ли компания или ИП действующей
         				 cast(o.ul_activity_stop_dt as timestamp) as ul_activity_stop_dt, --Дата прекращения деятельности организации
         				 o.ul_activity_stop_method_nm,								--Метод прекращения деятельности
         				 cast(o.ul_reg_first_dt as timestamp) as ul_reg_first_dt, 	--Дата первичной регистрации организации
         				 cast(o.ul_reg_ogrn_dt as timestamp) as  ul_reg_ogrn_dt,	--Дата присвоения ОГРН организации
         				 o.is_active_in_period,										--Флаг актуальности внутри периода
         				 cast(o.effectiveto  as timestamp) as effectiveto			--Дата закрытия версии
         		from $Node3t_team_k7m_aux_d_basis_integr_predecessorIN o) ui on ui.ul_ogrn=p.p_ogrn
            where p.is_active_in_period='Y'
            order by ul_inn) p1
            where p1.rn=1
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_basis_integr_predecessorOUT")

    logInserted()
    logEnd()

  }

}

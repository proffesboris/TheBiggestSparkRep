package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.{SaveMode, SparkSession}

class BBO(override val spark: SparkSession, val config: Config) extends EtlJob with EtlLogger {



  override val processName = "BBO"
  val tblBoGen = s"${config.aux}.bo_gen"
  val tblBoBBO = s"${config.aux}.bo_bbo"

  var dashboardName = tblBoBBO

  def bbo() = {

    val dynSql1 = spark.sql(s"""SELECT
                |CASE TYPE_NAME 
                |WHEN 'вещественная' THEN 
                |    CASE when EKS_FLD is null or TRIM(EKS_FLD)='' or EKS_FLD='NULL' then '' ELSE CONCAT('T_EKS.',feature_cd,'>=T_BO.',feature_cd,'_L AND T_EKS.',feature_cd,'<=T_BO.',feature_cd,'_U AND ') END
                |WHEN 'категориальная' THEN
                |    CASE when EKS_FLD is null or TRIM(EKS_FLD)='' or EKS_FLD='NULL' then '' ELSE CONCAT('T_EKS.',feature_cd,'=T_BO.',feature_cd,' AND ') END
                |END AS SQL_TXT
                | FROM ${config.aux}.dict_bo_features 
                | ORDER BY feature_id""".stripMargin).select("sql_txt").collect.map(r => r.getString(0)).mkString("\n|")
    
    spark.sql(s"DROP TABLE IF EXISTS ${config.aux}.BBO_STG_01_00")

    dashboardName = s"${config.aux}.BBO_STG_01_00"

    spark.sql(s"""CREATE TABLE  ${config.aux}.BBO_STG_01_00 as
                |WITH T_EKS as ( --Собираем витрину ЕКС со всеми фичами, по 10 договоров каждого клиента
                |SELECT * FROM (
                |select 
                |CL.U7M_ID as U7M_ID
                |--,CL.inn as INN --Для отладки
                |,pr_cred_id --Для отладки
                |,c_date_ending as SORT_DATE -- date_close
                |--,DENSE_RANK() OVER (Partition by INN order by c_date_ending ASC) AS RNK
                |,(ROW_NUMBER() OVER 
                |(Partition by C.INN order by ABS(DATEdiff(C.c_date_ending,current_timestamp())) ASC)) as RN --для отсечки 10 договоров по каждому клиенту
                |,ROUND(1.1-0.1*(DENSE_RANK() OVER
                |(Partition by C.INN order by ABS(DATEdiff(C.c_date_ending,current_timestamp())) ASC)
                |),2) as BO_Q_ADD
                |,ROUND(NVL(dur_in_days,0)/30) as DUR 						--1. DUR длительность в месяцах
                |,summa_base/1000 as EXP 							--2. EXP отношение объема обязательства к выручке (в bo уже посчитаны границы обязательства, т.е. в этом поле объем обязательства в тыс. руб.)
                |,CASE 
                |WHEN cred_flag=1 or  nkl_flag=1 then case when dur_in_days=0 then null else (avp_in_days/dur_in_days) END 
                |WHEN vkl_flag=1 or over_flag=1 then 1 ELSE NULL END as AVP 	--3. период доступности / длительность (dur_in_days/avp_in_days)
                |,CASE 
                |WHEN cred_flag=1 or  nkl_flag=1 then case when dur_in_days=0 then null else (avp_in_days/dur_in_days) END 
                |WHEN vkl_flag=1 or over_flag=1 then 1 ELSE NULL END as PRIV 	--4. льготный период / длительность (временно = AVP)
                |,CASE 
                |WHEN cred_flag=1 or  nkl_flag=1 then case when dur_in_days=0 then null else (avp_in_days/dur_in_days) END 
                |WHEN vkl_flag=1 or over_flag=1 then 1 ELSE NULL END as BALLOON --5. Объем баллона / объем обязательства (временно = AVP)
                |,summa_currency as CURRENCY							--6. Валюта обязательства
                |,CASE instrument 
                |WHEN 'ОВЕР' THEN 	'1-M813DK'
                |WHEN 'КД/НКЛ/ВКЛ' THEN 	'1-M813EC'
                |ELSE NULL END as PRODUCT 							--7. Продукт обязательства
                |,CASE
                |WHEN cred_flag=1 then 'Credit Contract'
                |WHEN nkl_flag=1 then 'NKL'	
                |WHEN vkl_flag=1 then 'VKL'	
                |WHEN over_flag=1 then 'Contract Credit Overdraft'
                |ELSE NULL END as CREDIT_MODE 		                --8. Режим обязательства
                |,'01' as TARGET_GR								   --9. Цель предоставления (Временно константа 01)
                |,0 as PD									       --10. Рейтинг контрагента (Временно константа 0, попадает в любой куб по настройке справочника features)
                |,0 as LGD								    	   --11. LGD по обязательству (Временно константа 0, попадает в любой куб по настройке справочника features)
                |,'Credit' as CREDIT_INSTRUMENT                     --12. Инструмент кредитования (по продукту) (Временно константа 0, попадает в любой куб по настройке справочника features)
                |from ${config.aux}.set_cred_active C, ${config.pa}.CLU CL --Соединяем с CLU через ИНН (Отсечка лишних + U7M_ID)
                |where 
                |CL.flag_basis_client='Y' --Только клиенты из базиса
                |and C.inn=CL.inn
                |--and AVP_in_days is not null --временно т.к. много договоров с пустым полем
                |--and C.c_date_ending>date_sub(current_timestamp(),30) --убираем т.к. нужна история
                |--and C.inn is NOT null 
                |--and C.inn in (select inn from ${config.aux}.DEL_ME_0001) --временный список клиентов для тестов
                |) T_EKS WHERE RN<11 --для отсечки 10 договоров по каждому клиенту
                |),
                |T_BO as ( --ШАГ 2 Собираем bo в горизонтальную структуру
                |SELECT BO_ID,U7M_ID 
                |,max(case WHEN key='AVP_L' then value_n else null end) as AVP_L
                |,max(case WHEN key='AVP_U' then value_n else null end) as AVP_U
                |,max(case WHEN key='BALLOON_L' then value_n else null end) as BALLOON_L
                |,max(case WHEN key='BALLOON_U' then value_n else null end) as BALLOON_U
                |,max(case WHEN key='DUR_L' then value_n else null end) as DUR_L
                |,max(case WHEN key='DUR_U' then value_n else null end) as DUR_U
                |,max(case WHEN key='EXP_L' then value_n else null end) as EXP_L
                |,max(case WHEN key='EXP_U' then value_n else null end) as EXP_U
                |,max(case WHEN key='LGD_L' then value_n else null end) as LGD_L
                |,max(case WHEN key='LGD_U' then value_n else null end) as LGD_U
                |,max(case WHEN key='PD_L' then value_n else null end) as PD_L
                |,max(case WHEN key='PD_U' then value_n else null end) as PD_U
                |,max(case WHEN key='PRIV_L' then value_n else null end) as PRIV_L
                |,max(case WHEN key='PRIV_U' then value_n else null end) as PRIV_U
                |,max(case WHEN key='PRODUCT' then value_c else null end) as PRODUCT
                |,max(case WHEN key='CREDIT_MODE' then value_c else null end) as CREDIT_MODE
                |,max(case WHEN key='TARGET_GR' then value_c else null end) as TARGET_GR
                |,max(case WHEN key='CURRENCY' then value_c else null end) as CURRENCY
                |,max(case WHEN key='CREDIT_INSTRUMENT' then value_c else null end) as CREDIT_INSTRUMENT
                |--,max(case WHEN key='BO_Q_SRC' then value_n else null end) as BO_Q_SRC --убрать т.к. будут далее подключаться  bo без договоров
                |FROM $tblBoGen
                |GROUP BY BO_ID,U7M_ID --LIMIT 10
                |) 
                |--SELECT * FROM (
                |SELECT --
                |T_BO.bo_id,
                |--T_BO.u7m_id,--T_EKS.u7m_id,
                |--T_BO.bo_q_src,
                |T_EKS.bo_q_add,
                |T_EKS.dur,T_BO.dur_l,T_BO.dur_u,T_EKS.exp,T_BO.exp_l,T_BO.exp_u,T_EKS.avp,T_BO.avp_l,T_BO.avp_u,T_EKS.priv,T_BO.priv_l,T_BO.priv_u,T_EKS.balloon,T_BO.balloon_l,T_BO.balloon_u,
                |T_BO.currency as BO_CURRENCY,T_EKS.currency,T_BO.product as BO_PRODUCT,T_EKS.product,T_BO.CREDIT_MODE as BO_CREDIT_MODE,T_EKS.CREDIT_MODE,T_BO.target_gr as BO_TARGET_gr,T_EKS.target_gr,
                |T_EKS.pd,T_BO.pd_l,T_BO.pd_u,T_EKS.lgd,T_BO.lgd_l,T_BO.lgd_u,T_BO.credit_instrument as BO_Credit_instrument,T_EKS.credit_instrument
                |--,T_EKS.inn
                |,T_EKS.pr_cred_id
                |,T_EKS.rn as EKS_RN,T_EKS.sort_date
                |--,ROW_NUMBER() OVER (Partition by T_EKS.pr_cred_id order by T_BO.dur_l,T_BO.dur_u,T_BO.exp_l,T_BO.exp_u,T_BO.avp_l,T_BO.avp_u,T_BO.priv_l,T_BO.priv_u,T_BO.balloon_l,T_BO.balloon_u,T_BO.currency,T_BO.product,T_BO.CREDIT_MODE,T_BO.target_GR,T_BO.pd_l,T_BO.pd_u,T_BO.lgd_l,T_BO.lgd_u,t_bo.CREDIT_INSTRUMENT) as RN --Список фич, для того чтобы однозначно сопоставить каждому договору только один куб BO
                |FROM T_BO,T_EKS --Соединяем bo и ЕКС, получаем договора ЕКС и кубы bo, в которые они попадают
                |WHERE
                |--Start DynSQL 1 Result
                |$dynSql1
                |--END DynSQL 1 Result
                |T_EKS.U7M_ID=T_BO.U7M_ID
                |--) T0
                |--WHERE RN=1""".stripMargin)

    logInserted()

    dashboardName = tblBoBBO

    spark.table(s"${config.aux}.bo_empty")
      .write
      .mode(SaveMode.Overwrite)
      .option("path", s"${config.auxPath}bo_bbo")
      .format("parquet")
      .saveAsTable(tblBoBBO)


    spark.sql(s"""INSERT INTO $tblBoBBO
                |select '$execId' as exec_id,null as okr_id,
                |B.BO_ID as BO_ID,B.u7m_id as u7m_id,b.clust_id as clust_id,'BO_QUALITY' as KEY,null as VALUE_C
                |,cast(b.value_n + NVL(b1.BO_Q_ADD,0) as decimal(16,5)) as VALUE_N
                |,NULL as VALUE_D FROM 
                |$tblBoGen B
                |LEFT JOIN 
                |(SELECT BO_ID,SUM(BO_Q_ADD) as BO_Q_ADD FROM ${config.aux}.BBO_STG_01_00 GROUP BY BO_ID) B1
                |ON B.BO_ID=B1.BO_ID
                |WHERE B.key='BO_Q_SRC'""".stripMargin)
  }
  
  def set() = {
    
    spark.sql(s"""INSERT INTO $tblBoBBO
                |select B1.exec_id,B1.okr_id,B1.BO_ID as BO_ID,B1.u7m_id as u7m_id,B1.clust_id as clust_id 
                |--,B1.key as k1,b2.key as k2,b1.value_n as v1,b2.value_c as v2,s.set_id,s.product,PD_MAX_SET
                |,'SET_ID' as KEY,S.set_ID as VALUE_C,NULL as VALUE_N ,NULL as VALUE_D 
                |FROM $tblBoGen B1,$tblBoGen B2, --атрибуты bo - PD_U, PRODUCT
                |(SELECT S0.set_id as set_id
                |,S0.u7m_b_id as U7M_ID
                |,CASE S0.instrument 
                |WHEN 'ОВЕР' THEN 	'1-M813DK'
                |WHEN 'КД/НКЛ/ВКЛ' THEN 	'1-M813EC'
                |ELSE NULL END as PRODUCT 							--7. Продукт обязательства - Аналогично операции BBO
                |,SC.Upper_bound as PD_MAX_SET  -- 19/07/2018 разъяснение Льва по изменению граничных условий
                | FROM ${config.pa}.`set` S0
                | LEFT JOIN ${config.aux}.dict_BO_PD_BY_SCALE SC
                |  on s0.max_borrower_rating=SC.SBER_RAITING
                | ) S
                |where B1.BO_ID=B2.BO_ID AND B1.key='PD_U' AND B2.key='PRODUCT' --атрибуты bo
                |AND s.u7m_id=b1.u7m_id AND s.product=b2.value_c --соединяем с SET по U7M_ID и PRODUCT
                |and (S.PD_MAX_SET is null or b1.value_N<=S.PD_MAX_SET ) --Все остальное не попадет в bo, т.е. SET_ID будет NULL
                |--LIMIT 10""".stripMargin)

    logInserted()
    
  }

  def run() {
    logStart()

    bbo()
    set()

    logEnd()
  }
}
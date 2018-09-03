package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class BasisEksClCredClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val DevSchema = config.aux
  val Stg0Schema = config.stg

  //------------------EKS-----------------------------
  val Node1t_team_k7m_aux_d_basis_client_creditIN = s"${DevSchema}.basis_client_eks"
  val Node2t_team_k7m_aux_d_basis_client_creditIN = s"${Stg0Schema}.eks_z_pr_cred"
  val Node3t_team_k7m_aux_d_basis_client_creditIN = s"${Stg0Schema}.eks_z_product"
  val Node4t_team_k7m_aux_d_basis_client_creditIN = s"${Stg0Schema}.eks_z_com_status_prd"
  val Node5t_team_k7m_aux_d_basis_client_creditIN = s"${Stg0Schema}.eks_z_ac_fin"
  val Node6t_team_k7m_aux_d_basis_client_creditIN = s"${Stg0Schema}.eks_z_kind_credits"
  val Node7t_team_k7m_aux_d_basis_client_creditIN = s"${Stg0Schema}.eks_z_types_cred"
  val Node8t_team_k7m_aux_d_basis_client_creditIN = s"${Stg0Schema}.eks_z_ft_money"
  val Nodet_team_k7m_aux_d_basis_client_creditOUT = s"${DevSchema}.basis_client_credit"
  val dashboardPath = s"${config.auxPath}basis_client_credit"


  override val dashboardName: String = Nodet_team_k7m_aux_d_basis_client_creditOUT //витрина
  override def processName: String = "Basis"

  def DoBasisEksClCred()//(spark:org.apache.spark.sql.SparkSession)
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_basis_client_creditOUT).setLevel(Level.WARN)
    logStart()


    val smartSrcHiveTable_t7 = spark.sql(
      s"""
         select              cc1.cred_id,
                             cc1.c_high_level_cr,
                             cc1.idHighLevelCr,
                             cc1.cred_class_id,
                             cc1.c_account,
                             cc1.c_main_v_id,
                             cc1.c_saldo,
                             cc1.c_saldo_nt,
                             cc1.c_client,
                             cc1.cl_name,
                             cc1.cl_inn,
                             cc1.cl_name_inn,
                             cc1.prod_id,
                             cc1.c_com_status,
                             cc1.c_num_dog,
                             cc1.c_summa_dog,
                             cc1.c_cur_short,
                             cc1.c_summa_out_debt,
                             cc1.c_summa_debt_cred,
                             cc1.c_date_give,
                             cc1.state_id,
                             cc1.c_code,
                             cc1.c_name,
                             cc1.c_max_date_use_up,
                             cc1.c_date_payout_ltd,
                             cc1.c_begin_pay,
                             cc1.c_one_day,
                             cc1.date_begin,
                             cc1.date_begining,
                             cc1.date_close,
                             cc1.date_ending,
                             cc1.kind_code,
                             cc1.kind_name,
                             cc1.type_name,
                             cc1.numdog_type_kind_dategive,
                             cc1.type_cr,
                             cc1.HighLevelCr_NumDog,
                             cc1.HighLevelCr_KindName,
                             cc1.HighLevelCr_TypeName,
                             cc1.HighLevelCr_DateGive,
                             cc1.HighLevelCr_SumDog,
                             cc1.isKL,
                   concat('(', cc1.HighLevelCr_NumDog, ' ', cc1.HighLevelCr_KindName, ')_', cc1.HighLevelCr_TypeName, ')_', cc1.HighLevelCr_DateGive) as HighLevelCr_numdog_type_kind_dategive
                from (select cc.cred_id,
                             cc.c_high_level_cr,
                             cc.idHighLevelCr,
                             cc.cred_class_id,
                             cc.c_account,
                             cc.c_main_v_id,
                             cc.c_saldo,
                             cc.c_saldo_nt,
                             cc.c_client,
                             cc.cl_name,
                             cc.cl_inn,
                             cc.cl_name_inn,
                            cc.prod_id,
                             cc.c_com_status,
                             cc.c_num_dog,
                             cc.c_summa_dog,
                             cc.c_cur_short,
                             cc.c_summa_out_debt,
                             cc.c_summa_debt_cred,
                             cc.c_date_give,
                             cc.state_id,
                             cc.c_code,
                             cc.c_name,
                             cc.c_max_date_use_up,
                             cc.c_date_payout_ltd,
                             cc.c_begin_pay,
                             cc.c_one_day,
                             cc.date_begin,
                             cc.date_begining,
                            cc.date_close,
                             cc.date_ending,
                             cc.kind_code,
                             cc.kind_name,
                             cc.type_name,
                             cc.numdog_type_kind_dategive,
                             cc.type_cr,
                   case when cc.c_high_level_cr is null
                      then cc.c_num_dog
                      else first_value(cc.c_num_dog) over (partition by cc.idHighLevelCr order by cc.c_Date_give, cc.c_high_level_cr)
                   end HighLevelCr_NumDog,                --  Вспомогательный атрибут для валидации. Номер верхнеуровневого договора
                   case when cc.c_high_level_cr is null
                      then cc.kind_name
                      else first_value(cc.kind_name) over (partition by cc.idHighLevelCr order by cc.c_Date_give, cc.c_high_level_cr)
                   end HighLevelCr_KindName,              --  Вспомогательный атрибут для валидации. Вид верхнеуровневого договора
                   case when cc.c_high_level_cr is null
                      then cc.type_name
                      else first_value(cc.type_name) over (partition by cc.idHighLevelCr order by cc.c_Date_give, cc.c_high_level_cr)
                   end HighLevelCr_TypeName,              --  Вспомогательный атрибут для валидации. Тип верхнеуровневого договора
                   case when cc.c_high_level_cr is null
                      then cc.c_date_give
                      else first_value(cc.c_date_give) over (partition by cc.idHighLevelCr order by cc.c_Date_give, cc.c_high_level_cr)
                 end HighLevelCr_DateGive,            --    Вспомогательный атрибут для валидации. Дата выдачи верхнеуровневого договора
                   case when cc.c_high_level_cr is null
                      then cc.c_summa_dog
                      else first_value(cc.c_summa_dog) over (partition by cc.idHighLevelCr order by cc.c_Date_give, cc.c_high_level_cr)
                   end HighLevelCr_SumDog,             --   Вспомогательный атрибут для валидации. Сумма верхнеуровневого договора
                   case
                     when cc.c_high_level_cr is null  and cc.type_cr<>'кредитный договор'
                     then true
                     else false
                  end isKL                      --  Вспомогательный атрибут для валидации. Признак, что запись соответствует открытию ВКЛ или НКЛ
               from  (select p.id as cred_id,            --     уникальный id в core_internal_eks.z_pr_cred
                      p.c_high_level_cr,            --   FK на core_internal_eks.z_pr_cred. Ссылка на кредит верхнего уровня
                       case
                       when p.c_high_level_cr is null
                       then p.id
                       else p.c_high_level_cr
                     end idHighLevelCr,               -- Вспомогательный атрибут для сортировки и удобства валидации. id верхнеуровневого договора
                      p.class_id as cred_class_id,
                      p.c_account,                  --   уникальный id в core_internal_eks.z_ac_fin. Ссылка на ссудный счет
                      f.c_main_v_id,              --     Номер ссудного счета
                      f.c_saldo,                   --    Остаток по ссудному счету в валюте счета
                      f.c_saldo_nt,                  --  Остаток по ссудному счету в рублях
                      p.c_client,                  --   уникальный id в core_internal_eks.z_client. Ссылка на клиента, которому выдан кредит
                      c.c_name as cl_name,           --      Наименование клиента, которому выдан кредит
                      c.c_inn as cl_inn,             --    ИНН клиента, которому выдан кредит
                      concat(c.c_name, '_', c.c_inn)  as cl_name_inn,
                      pr.id as prod_id,             --    уникальный id в core_internal_eks.z_product
                      pr.c_com_status,                --   FK на core_internal_eks.z_com_status_prd. Ссылка на статус кредитного договора
                      pr.c_num_dog,              --      Номер кредитного догвоора
                      p.c_summa_dog,               --     Сумма кредитного догвоора
                      ft.c_cur_short,               --     Валюта, в которой выдан кредит
                      p. c_summa_out_debt,           --      Сумма просроченной задолженности
                      p. c_summa_debt_cred,         --      Сумма ссудной задолженности
                      p.c_date_give,                 --   Дата выдачи
                      s.id as state_id,             --     Fk на internal_eks_ibs.z_com_status_prd.id. id статуса
                      s.c_code,                   --    Код статуса
                      s.c_name,                   --    Статус
                      p.c_max_date_use_up,             --     Предельная дата выборки ссуды
                      p.c_date_payout_ltd,             --     Предельная дата выдач
                      p.c_begin_pay,                   --  Дата начала гашения
                      p.c_one_day,                      --  Однодневный кредит
                      pr.c_date_begin as date_begin,       --    Дата создания договора
                      pr.c_date_begining as date_begining,       --  Дата начала действия договора
                      pr.c_date_close as date_close,          -- Дата закрытия договора
                      pr.c_date_ending as date_ending,         --  Дата окончания действия договора
                      k.c_code as kind_code,             --   Код вида кредита
                      k.c_name as kind_name,             --   Вид кредита
                      tc.c_short_name as type_name,      --     тип кредита
                      concat('(', pr.c_num_dog, ' ', tc.c_short_name, ')_', k.c_name, ')_', p.c_date_give) as numdog_type_kind_dategive, --Вспомогательный атрибут для валидации. Объединение сведений по договору в одну строку
                      case
                    when lower(tc.c_short_name)='cred_line'  and p.c_high_level_cr is null then 'Открытие НКЛ'
                     when lower(tc.c_short_name)='cred_over' then 'Открытие ВКЛ'
                    when lower(tc.c_short_name)='cred_cont' and p.c_high_level_cr is null  then 'кредитный договор'
                     when p.c_high_level_cr is not null then 'Транш'
                     when lower(tc.c_short_name)='guar_cond' then 'гарантия'
                      end type_cr                                                   --  Запись соответствует открытию НКЛ_ВКЛ_кредитному договору_траншу по КЛ
         from $Node1t_team_k7m_aux_d_basis_client_creditIN c                                                                /* @@@NEW_3      оптимизация запроса */
                     join $Node2t_team_k7m_aux_d_basis_client_creditIN p on c.id=p.c_client and p.c_high_level_cr is null                  /* @@@NEW_3 оптимизация запроса, добавила сразу c_high_level_cr is null, так как при формировании базиса есть такой фильтр  */
                     join $Node3t_team_k7m_aux_d_basis_client_creditIN pr on pr.id=p.id                           /* @@@NEW_3 оптимизация запроса */
                     join $Node4t_team_k7m_aux_d_basis_client_creditIN s on s.id=pr.c_com_status and s.c_code in ('TO_CLOSE', 'WORK', 'IN_SUPPORT', 'TO_BUH_CONTROL')     /* @@@NEW_3 оптимизация запроса Отбираем только действующие кредитные договора (исходя из статуса)*/
                     left join $Node5t_team_k7m_aux_d_basis_client_creditIN f on f.id=p.c_account
                     left join $Node6t_team_k7m_aux_d_basis_client_creditIN k on k.id=p.c_kind_credit
                     left join $Node7t_team_k7m_aux_d_basis_client_creditIN tc on k.c_reg_rules=tc.id
                     left join $Node8t_team_k7m_aux_d_basis_client_creditIN ft on ft.id=p.c_ft_credit
                     ) cc) cc1
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_basis_client_creditOUT")

    logInserted()
    logEnd()
  }

}

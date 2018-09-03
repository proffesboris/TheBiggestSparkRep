package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class PrdDtClass (val spark: SparkSession, val config: Config, val date: String) extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_k7m_prd_dtIN = s"${Stg0Schema}.eks_z_product"
  val Node2t_team_k7m_aux_d_k7m_prd_dtIN = s"${Stg0Schema}.eks_z_pr_cred"
  val Node3t_team_k7m_aux_d_k7m_prd_dtIN = s"${Stg0Schema}.eks_z_kred_corp"
  val Node4t_team_k7m_aux_d_k7m_prd_dtIN = s"${Stg0Schema}.eks_z_overdrafts"
  val Node5t_team_k7m_aux_d_k7m_prd_dtIN = s"${Stg0Schema}.eks_z_kind_credits"
  val Node6t_team_k7m_aux_d_k7m_prd_dtIN = s"${Stg0Schema}.eks_z_types_cred"
  val Nodet_team_k7m_aux_d_k7m_prd_dtOUT = s"${DevSchema}.k7m_prd_dt"
  val dashboardPath = s"${config.auxPath}k7m_prd_dt"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_prd_dtOUT //витрина
  override def processName: String = "Plan_liab"

  def DoPrdDt()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_prd_dtOUT).setLevel(Level.WARN)
    logStart()

    val createHiveTableStage1 = spark.sql(
      s"""
       select  t.id
              ,t.collection_id
              ,t.class_id
              ,t.c_num_dog
              ,t.c_date_begin
              ,t.c_date_ending
              ,t.c_date_close
              ,t.c_array_dog_acc
              ,t.c_client_id
              ,cast(t.c_high_level_cr as decimal(38,12)) as c_high_level_cr
              ,t.c_cred_lines
              ,t.c_kind_credit
              ,t.c_reg_rules
              ,t.c_short_name
              ,t.c_ft_limit
              ,t.c_limit_history
               from (
         select p.id
              , p.collection_id
              , p.class_id
              , p.c_num_dog
              , p.c_date_begin
              , p.c_date_ending
              , p.c_date_close
              , p.c_array_dog_acc
              , cast(case when p.class_id <> 'GEN_AGREEM_FRAME' then pc.c_client else null end as bigint) as c_client_id --f.c_client end
              , cast(case when p.class_id <> 'GEN_AGREEM_FRAME' then nvl(pc.c_high_level_cr, pc.id) else pc.id end  as bigint) as C_HIGH_LEVEL_CR
              , cast(case when p.class_id <> 'GEN_AGREEM_FRAME' then null else null end  as bigint) as C_CRED_LINES--f.c_cred_lines end
              , cast(case when p.class_id <> 'GEN_AGREEM_FRAME' then pc.c_kind_credit else null end as bigint) as C_KIND_CREDIT --f.c_kind_credit end
              , kc.c_reg_rules, tc.c_short_name
              , cast(case when p.class_id <> 'GEN_AGREEM_FRAME' then pc.c_ft_limit else null end as bigint) as C_FT_LIMIT --f.c_valuta end
              , cast( case when p.class_id = 'KRED_CORP'        then cc.c_limit_history
                     when p.class_id = 'OVERDRAFTS'       then o.c_limit_history
                     --when p.class_id = 'GEN_AGREEM_FRAME' then f.c_limit_history
                     else null
                 end as bigint) as c_limit_history
                from (select p.id, p.collection_id, p.class_id, p.c_num_dog, p.c_date_begin, p.c_date_ending, p.c_date_close, p.c_array_dog_acc
                                       from $Node1t_team_k7m_aux_d_k7m_prd_dtIN p
                                      where p.class_id in ('KRED_CORP','OVERDRAFTS')--, 'GEN_AGREEM_FRAME')
                     ) p
                join (select pc.id, pc.c_client, pc.c_high_level_cr, pc.c_kind_credit
                                          , case when pc.c_limit_acc_code = 1061988051 then coalesce(pc.c_ft_credit, pc.c_ft_limit) else coalesce(pc.c_ft_limit, pc.c_ft_credit) end c_ft_limit   -- 06042018 lv: применен "Способ учета лимита"
                                       from $Node2t_team_k7m_aux_d_k7m_prd_dtIN pc
                                      where pc.class_id in ('KRED_CORP','OVERDRAFTS')
                     ) pc on pc.id = p.id
           left join (select cc.id, cc.c_limit_history
                                       from $Node3t_team_k7m_aux_d_k7m_prd_dtIN    cc
                     ) cc on cc.id = pc.id
           left join (select o.id, o.c_limit_history
                                       from $Node4t_team_k7m_aux_d_k7m_prd_dtIN   o
                     ) o on o.id = pc.id
           left join (select kc.id, kc.c_reg_rules
                                        from $Node5t_team_k7m_aux_d_k7m_prd_dtIN kc
                      ) kc on kc.id = pc.c_kind_credit
           left join (select tc.id, tc.c_short_name
                                        from $Node6t_team_k7m_aux_d_k7m_prd_dtIN tc
                      ) tc  on tc.id = kc.c_reg_rules
         ) t
          where coalesce(cast(t.c_date_begin as timestamp), cast(current_date as timestamp)) <= cast('$date' as timestamp) --заменить дату 2018-01-01 на параметр Дата расчета К7М
            and coalesce(cast(t.c_date_close as timestamp), cast(current_date as timestamp)) >  cast('$date' as timestamp)  --заменить дату 2018-01-01 на параметр Дата расчета К7М
    """
    )
    createHiveTableStage1
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_prd_dtOUT")

    logInserted()
    logEnd()
  }
}


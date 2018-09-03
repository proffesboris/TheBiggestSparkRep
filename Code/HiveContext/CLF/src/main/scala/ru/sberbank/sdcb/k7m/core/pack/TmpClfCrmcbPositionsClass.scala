package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class TmpClfCrmcbPositionsClass (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_tmp_clf_crmcb_positionsIN = s"${DevSchema}.tmp_clf_crmcb_clf"
  val Node2t_team_k7m_aux_d_tmp_clf_crmcb_positionsIN = s"${Stg0Schema}.crm_s_party_per"
   val Nodet_team_k7m_aux_d_tmp_clf_crmcb_positionsOUT = s"${DevSchema}.tmp_clf_crmcb_positions"
  val dashboardPath = s"${config.auxPath}tmp_clf_crmcb_positions"


  override val dashboardName: String = Nodet_team_k7m_aux_d_tmp_clf_crmcb_positionsOUT //витрина
  override def processName: String = "CLF"

  def DoTmpClfCrmcbPosition()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_tmp_clf_crmcb_positionsOUT).setLevel(Level.WARN)

    val smartSrcHiveTable_t7 = spark.sql(
      s"""	select party_id, cont_id,x_active_flg,x_rel_type,x_job_title,board_seat_flg,main_position_flag
         	from
         		(select
         			  t.party_id     		--Организации контакта - ID организации
         			, t.person_id cont_id  	--Организации контакта - ID контакта
         			, t.x_active_flg     	--Организации контакта - Действующий (Y/N)
         			, t.x_rel_type     		--Организации контакта - Тип отношений
         			, t.x_job_title    		--Организации контакта - Должность
         			, t.board_seat_flg    	--Организации контакта - Топ-менеджмент (Y/N)
         			, case when t.party_id = filter.pr_dept_ou_id then 'Y' else 'N'
         				end main_position_flag	--Организации контакта - Основная организация контакта (Y/N)
         			, row_number() over(
         				partition by t.person_id
         				order by
         					--case when t.x_active_flg = 'Y' then 1 else 2 end,
         					case when t.party_id = filter.pr_dept_ou_id then 1 else 2 end
         					,case when t.board_seat_flg = 'Y' then 1 else 2 end
         				) rn
         		from
         			$Node1t_team_k7m_aux_d_tmp_clf_crmcb_positionsIN filter
         			join $Node2t_team_k7m_aux_d_tmp_clf_crmcb_positionsIN t
         				on filter.cont_id = t.person_id
         		where
         			nvl(trim(t.x_job_title),'')<>''
         			and t.x_active_flg='Y'
         		) tt
         	where rn = 1
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_tmp_clf_crmcb_positionsOUT")

    logInserted()

  }
}



